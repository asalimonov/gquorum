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
func TestReconcileFencesPersistsPartialProgressAfterOperatorTerminates(t *testing.T) {
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
	at := time.Now().UTC()
	begin, err := g.propose(ctx, metaengine.BeginOperatorProposal{Version: doc.Version, Plan: plan, At: at})
	if err != nil {
		t.Fatal(err)
	}
	transition := topology.OperatorTransition{OperatorID: begin.OperatorID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, State: topology.OperatorRunning, At: at.Add(time.Millisecond)}
	if _, err := g.propose(ctx, metaengine.TransitionOperatorProposal{Transition: transition}); err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, Kind: step.Kind, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	if _, err := g.BeginEffect(ctx, action); err != nil {
		t.Fatal(err)
	}
	transition.State, transition.At = topology.OperatorSuperseded, at.Add(2*time.Millisecond)
	if _, err := g.propose(ctx, metaengine.TransitionOperatorProposal{Transition: transition}); err != nil {
		t.Fatal(err)
	}
	coordinator := &operationCoordinator{g: g}
	for _, obligation := range g.state().PendingFences() {
		if obligation.Resource.Kind != topology.AuthorityHost {
			continue
		}
		grant, found := g.state().AuthorityGrant(obligation.Key)
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
	if err := g.ReconcileFences(ctx); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unavailable group did not retain pending revocation", err)
	}
	status, err := g.ReadAuthority(ctx, begin.OperatorID)
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
	if err := g.ReconcileFences(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := g.WaitRevoked(ctx, begin.OperatorID); err != nil || !status.RevocationComplete {
		t.Fatal(status, err)
	}
	if err := g.ReconcileFences(ctx); err != nil || hosts.Load() != 1 || groups.Load() != 2 {
		t.Fatal("completed resources repeated", hosts.Load(), groups.Load(), err)
	}
}
