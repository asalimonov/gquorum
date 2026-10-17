//go:build functional

package metaservice

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type scopedPlanningController struct {
	*statusController
	reads      []string
	beforeRead func()
}

// Exercise the production capture/recapture path against the fixture's real FSM.
func (f *mutationFixture) captureFixturePlan(t *testing.T, desired topology.Document) (metaengine.PlanRead, error) {
	t.Helper()
	return f.g.capturePlan(t.Context(), metaengine.QueryRequest{Kind: "planning", Desired: &desired},
		func(_ context.Context, request metaengine.QueryRequest) (metaengine.PlanRead, error) {
			return f.fixturePlanRead(t, request), nil
		})
}

func (c *scopedPlanningController) ReadNative(ctx context.Context, id string,
	identities ...storage.Identity) (topology.NativeState, error) {
	c.reads = append(c.reads, id)
	if c.beforeRead != nil {
		c.beforeRead()
		c.beforeRead = nil
	}
	if id != c.value.GroupID {
		return topology.NativeState{}, fault.ErrUnavailable
	}
	return c.statusController.ReadNative(ctx, id, identities...)
}

func TestScopedPlanningCollectionIgnoresUnchangedUnavailableGroups(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	base := f.g.machine.Target()
	other := *base.Group("work")
	other.Replicas = slices.Clone(other.Replicas)
	other.ID = "other"
	base.Groups = append(base.Groups, other)
	f.commit(t, base, true)
	base = f.g.machine.Target()
	c := &scopedPlanningController{statusController: &statusController{value: topology.GroupStatus{
		GroupID: "work", Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}}
	f.g.opts.LoadGroups = c
	desired := base.Clone()
	desired.Group("work").Labels = map[string]string{"owner": "jobs"}
	read, err := f.captureFixturePlan(t, desired)
	input := read.Input
	if err != nil || !slices.Equal(c.reads, []string{"work"}) || len(input.Native) != 1 {
		t.Fatal("collection contacted an unchanged group", c.reads, err)
	}
	if input.Groups["other"].Converged == nil || input.Groups[topology.MetaGroupID].Converged == nil ||
		input.Groups["other"].Observation != nil || input.Groups["work"].Observation == nil {
		t.Fatal("scope filtering discarded identity evidence or invented an observation")
	}
	desired.Group("other").Labels = map[string]string{"owner": "jobs"}
	if _, err := f.captureFixturePlan(t, desired); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("affected unavailable group was skipped", err)
	}
}

func TestScopedPlanningCollectionRechecksAffectedSource(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	desired := f.g.machine.Target()
	desired.Group("work").Labels = map[string]string{"owner": "jobs"}
	c := &scopedPlanningController{statusController: &statusController{value: topology.GroupStatus{
		GroupID: "work", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}},
		beforeRead: func() {
			changed := f.g.machine.Target()
			changed.Group("work").Labels = map[string]string{"owner": "newer"}
			f.commit(t, changed, true)
		}}
	f.g.opts.LoadGroups = c
	if _, err := f.captureFixturePlan(t, desired); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("collection retained evidence across an affected-source change", err)
	}
}

func TestScopedPlanningDeletionKeepsProofWithoutNativeRead(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	desired := f.g.machine.Target()
	desired.Groups = slices.DeleteFunc(desired.Groups, func(g topology.Group) bool { return g.ID == "work" })
	read, err := f.captureFixturePlan(t, desired)
	input := read.Input
	row := input.Groups["work"]
	if err != nil || len(input.Native) != 0 || row.Converged == nil ||
		!slices.Equal(row.Converged.Replicas, input.Approved.Group("work").Replicas) || row.ConvergedVersion == 0 {
		t.Fatal("deletion read a quorum or lost proven former hosts", row, err)
	}
}

func TestPlanForSkipsUnconvergedUnchangedScope(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) {
		o.LoadGroups = &planningController{nativeErr: fault.ErrUnavailable}
	}))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	staged, err := g.Stage(ctx, loadDocument(t, g), 1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := g.Approve(ctx, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	work, err := g.state().ReadyBlocks("", 1)
	if err != nil || len(work) != 1 || len(work[0].Operation.Plan.Steps) != 1 {
		t.Fatal("missing founding work", work, err)
	}
	step := work[0].Operation.Plan.Steps[0]
	// An issued founding grant leaves possible native state; unavailable native
	// reads must refuse changes to this group, even before any receipt arrives.
	transition := topology.OperationTransition{OperationID: id, Version: staged.Version,
		Attempt: 1, GroupID: "load-one", StepSeq: 1, State: topology.OperationRunning, At: time.Now().UTC()}
	if _, err := g.propose(ctx, metaengine.TransitionOperationProposal{Transition: transition, ProposalID: newProposalID()}); err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperationID: topology.OperationID(id), Version: staged.Version,
		Attempt: 1, GroupID: "load-one", StepSeq: 1, Kind: topology.StepCreateFoundingGroup,
		ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	if _, err := g.propose(ctx, metaengine.BeginEffectProposal{Action: action, ProposalID: newProposalID()}); err != nil {
		t.Fatal(err)
	}
	desired := staged.Clone()
	other := (topology.GroupProgress{Converged: desired.Group("load-one")}).Clone().Converged
	other.ID = "independent"
	desired.Groups = append(desired.Groups, *other)
	plan, err := g.PlanFor(ctx, desired)
	if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != other.ID ||
		len(plan.Steps) != 1 || plan.Steps[0].Kind != topology.StepCreateFoundingGroup {
		t.Fatal("independent preview depended on unavailable founding", plan, err)
	}
	desired.Group("load-one").Labels = map[string]string{"changed": "true"}
	if _, err := g.PlanFor(ctx, desired); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("affected possible founding did not require native evidence", err)
	}
	view := g.state().Views()
	if view.Latest.Version != staged.Version || view.Target.Version != staged.Version || view.FullyConverged.Version != 1 {
		t.Fatal("preview changed documents or manufactured convergence", view)
	}
}
