//go:build functional

package metaservice

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type scopedPlanningController struct {
	*statusController
	reads      []string
	beforeRead func()
}

// The existing planningInput caller selects AffectedGroups at Tasks 63/75;
// this wrapper exercises that selection without a temporary production API.
func (g *Group) scopedPlanningInput(ctx context.Context, desired topology.Document) (topology.PlanInput, error) {
	input, err := g.state().PlanningInput(ctx, desired, nil)
	if err != nil {
		return topology.PlanInput{}, err
	}
	return g.readPlanningInput(ctx, desired, input, topology.AffectedGroups(input.Approved, desired))
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
	base := f.g.machine.Active()
	other := *base.Group("work")
	other.Replicas = slices.Clone(other.Replicas)
	other.ID = "other"
	base.Groups = append(base.Groups, other)
	f.commit(t, base, true)
	base = f.g.machine.Active()
	c := &scopedPlanningController{statusController: &statusController{value: topology.GroupStatus{
		GroupID: "work", Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}}
	f.g.opts.LoadGroups = c
	desired := base.Clone()
	desired.Group("work").Labels = map[string]string{"owner": "jobs"}
	input, err := f.g.scopedPlanningInput(t.Context(), desired)
	if err != nil || !slices.Equal(c.reads, []string{"work"}) || len(input.Native) != 1 {
		t.Fatal("collection contacted an unchanged group", c.reads, err)
	}
	if input.Groups["other"].Converged == nil || input.Groups[topology.MetaGroupID].Converged == nil ||
		input.Groups["other"].Observation != nil || input.Groups["work"].Observation == nil {
		t.Fatal("scope filtering discarded identity evidence or invented an observation")
	}
	desired.Group("other").Labels = map[string]string{"owner": "jobs"}
	if _, err := f.g.scopedPlanningInput(t.Context(), desired); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("affected unavailable group was skipped", err)
	}
}

func TestScopedPlanningCollectionRechecksApprovedVersion(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	desired := f.g.machine.Active()
	desired.Group("work").Labels = map[string]string{"owner": "jobs"}
	c := &scopedPlanningController{statusController: &statusController{value: topology.GroupStatus{
		GroupID: "work", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}},
		beforeRead: func() { f.commit(t, f.g.machine.Active(), true) }}
	f.g.opts.LoadGroups = c
	if _, err := f.g.scopedPlanningInput(t.Context(), desired); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("collection retained evidence across an approved-version change", err)
	}
}

func TestScopedPlanningDeletionKeepsProofWithoutNativeRead(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	desired := f.g.machine.Active()
	desired.Groups = slices.DeleteFunc(desired.Groups, func(g topology.Group) bool { return g.ID == "work" })
	input, err := f.g.scopedPlanningInput(t.Context(), desired)
	row := input.Groups["work"]
	if err != nil || len(input.Native) != 0 || row.Converged == nil ||
		!slices.Equal(row.Converged.Replicas, input.Approved.Group("work").Replicas) || row.ConvergedVersion == 0 {
		t.Fatal("deletion read a quorum or lost proven former hosts", row, err)
	}
}
