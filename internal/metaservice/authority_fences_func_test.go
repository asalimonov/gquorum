//go:build functional

package metaservice

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// The load resources here are protocol doubles. Only the local _meta participant
// runs consensus; multi-node resource fencing is covered by the process suite.
func TestScopedFencesPersistPartialProgressAfterOperationTerminates(t *testing.T) {
	t.Parallel()
	var available atomic.Bool
	var hosts, groups atomic.Int32
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) {
		o.LoadGroups = &statusController{value: topology.GroupStatus{GroupID: "load-one", Found: true, LeaderID: 1}}
		o.InstallLocalFence = func(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
			proof := topology.FenceEvidence{Key: request.Grant.Key, Resource: request.Resource, Receiver: request.To, ReplicaID: request.ReplicaID}
			if request.Resource.Kind == topology.AuthorityHost {
				hosts.Add(1)
				proof.LocalSequence = 1
			} else {
				groups.Add(1)
				if !available.Load() {
					return load.FenceResult{}, fault.ErrUnavailable
				}
				proof.AppliedIndex, proof.AppliedTerm = 50, 1
			}
			return load.FenceResult{From: request.To, Evidence: proof}, ctx.Err()
		}
	}))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc, err := g.Stage(ctx, loadDocument(t, g), 1)
	if err != nil {
		t.Fatal(err)
	}
	step := topology.Step{Seq: 1, GroupID: "load-one", Kind: topology.StepCreateFoundingGroup}
	plan := guardedFixturePlan(doc.Version, step.GroupID, nil, []uint64{1}, step)
	step = plan.Steps[0]
	approval := (&mutationFixture{g: g}).fixtureApproval(t, doc)
	plan.Blocks = approval.Plan.Blocks
	approval.Plan = plan
	begin, err := g.propose(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	transition := topology.OperationTransition{OperationID: begin.OperationID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, State: topology.OperationRunning, At: time.Now().UTC()}
	if _, err := g.propose(ctx, metaengine.TransitionOperationProposal{Transition: transition, ProposalID: newProposalID()}); err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperationID: begin.OperationID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, Kind: step.Kind, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	if _, err := g.BeginEffect(ctx, action); err != nil {
		t.Fatal(err)
	}
	successor := g.state().Latest()
	successor.Group("load-one").Labels = map[string]string{"test": "supersede"}
	doc, err = g.Stage(ctx, successor, successor.Version)
	if err != nil {
		t.Fatal(err)
	}
	approval = (&mutationFixture{g: g, native: map[string]topology.NativeState{"load-one": fixtureNative(*doc.Group("load-one"))}}).fixtureApproval(t, doc)
	if _, err := g.propose(ctx, approval); err != nil {
		t.Fatal(err)
	}
	coordinator := &operationCoordinator{g: g}
	pending, err := coordinator.ReadyFences(ctx, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, obligation := range pending {
		if obligation.Resource.Kind != topology.AuthorityHost {
			continue
		}
		grant, found := g.state().AuthorityGrant(obligation.Grant.Key)
		if !found {
			t.Fatal("missing exact boundary")
		}
		target, err := coordinator.ResolveFenceTarget(ctx, operator.FenceWork{Grant: grant, Resource: obligation.Resource})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.DispatchFence(ctx, grant, obligation.Resource, target); err != nil {
			t.Fatal(err)
		}
		target.MemberID++
		if err := coordinator.DispatchFence(ctx, grant, obligation.Resource, target); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("foreign fence target accepted", err)
		}
	}
	if err := dispatchFixtureFences(ctx, coordinator); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unavailable group did not retain pending revocation", err)
	}
	status, err := g.ReadAuthority(ctx, begin.OperationID)
	if err != nil || status.RevocationComplete || len(status.Obligations) != 2 {
		t.Fatal(status, err)
	}
	for _, obligation := range status.Obligations {
		if (obligation.Evidence != nil) != (obligation.Resource.Kind == topology.AuthorityHost) {
			t.Fatal("independent host progress was lost", status)
		}
	}
	if hosts.Load() != 1 || groups.Load() != 1 {
		t.Fatal("revocation skipped a resource", hosts.Load(), groups.Load())
	}
	available.Store(true)
	if err := dispatchFixtureFences(ctx, coordinator); err != nil {
		t.Fatal(err)
	}
	if status, err := g.WaitRevoked(ctx, begin.OperationID); err != nil || !status.RevocationComplete {
		t.Fatal(status, err)
	}
	if err := dispatchFixtureFences(ctx, coordinator); err != nil || hosts.Load() != 1 || groups.Load() != 2 {
		t.Fatal("completed resources repeated", hosts.Load(), groups.Load(), err)
	}
}

func dispatchFixtureFences(ctx context.Context, coordinator *operationCoordinator) error {
	pending, err := coordinator.ReadyFences(ctx, 16)
	if err != nil {
		return err
	}
	var failures []error
	for _, work := range pending {
		target, err := coordinator.ResolveFenceTarget(ctx, work)
		if err == nil {
			err = coordinator.DispatchFence(ctx, work.Grant, work.Resource, target)
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
