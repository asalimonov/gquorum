package managed

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (f *fixture) approve(kind topology.StepKind, targets ...storage.Identity) topology.AuthorizedStep {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, Kind: kind, GroupID: "work"}
	if kind != topology.StepCreateFoundingGroup && kind != topology.StepDeleteGroupState {
		a.NodeName, a.ReplicaID = targets[0].NodeName, targets[0].ReplicaID
	}
	f.approval, f.targets = a, targets
	return a
}

func TestMembershipTargetsTheLearnerThroughADifferentLeaderReceiver(t *testing.T) {
	functional(t)
	f := newFixture(t)
	h := f.host(t)
	g, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
	must(t, err)
	await(t, deadline(t), g.Core().IsLeader)
	target := storage.Identity{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}
	a := f.approve(topology.StepAddLearner, target)
	req := f.request(a, f.localID(1))
	req.LocalTarget = storage.Identity{}
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if !result.Verified || !slices.Contains(g.Core().Status().ConfState.Learners, 2) {
		t.Fatal(result, g.Core().Status())
	}
	a = f.approve(topology.StepPromoteVoter, target)
	req.Authorization = a
	if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("promotion bypassed fixed catch-up rules", err)
	}
	if slices.Contains(g.Core().Status().ConfState.Voters, 2) {
		t.Fatal("unready learner became a voter")
	}
}
func (f *fixture) localID(replica uint64) storage.Identity {
	return storage.Identity{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID, GroupID: "work", ReplicaID: replica}
}
func (f *fixture) request(a topology.AuthorizedStep, id storage.Identity) load.ActionRequest {
	return load.ActionRequest{From: f.node, To: load.Peer{Node: f.node, Replica: load.ReplicaRef{GroupID: id.GroupID, ReplicaID: id.ReplicaID}}, Authorization: a, LocalTarget: id}
}

func TestApprovedCreationAndRefusedIdentities(t *testing.T) {
	functional(t)
	f := newFixture(t)
	var factories atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	id := f.localID(1)
	a := f.approve(topology.StepCreateFoundingGroup, id)
	req := f.request(a, id)
	for _, mutate := range []func(*load.ActionRequest){
		func(r *load.ActionRequest) { r.LocalTarget.ReplicaID++ },
		func(r *load.ActionRequest) { r.To.Node.InstanceID = "stale" },
		func(r *load.ActionRequest) { r.Authorization.Attempt++ },
		func(r *load.ActionRequest) { r.Authorization.Version++ },
		func(r *load.ActionRequest) { r.Authorization.StepSeq++ },
		func(r *load.ActionRequest) { r.Authorization.Kind = topology.StepDeleteGroupState },
	} {
		bad := req
		mutate(&bad)
		if reply, err := h.ApplyAction(deadline(t), bad); err == nil || reply.Verified {
			t.Fatal(reply, err)
		}
	}
	if factories.Load() != 0 {
		t.Fatal("refused action reached factory")
	}
	must(t, h.Dispatch(deadline(t), a))
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if !result.Verified || result.From != req.To || result.Authorization != a || result.LocalTarget != id || factories.Load() != 1 {
		t.Fatal(result, factories.Load())
	}
}

func TestStartupRevalidatesApprovalAfterFactoryWait(t *testing.T) {
	functional(t)
	f := newFixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	f.cfg.Factory = func(ctx context.Context, _ load.ReplicaSpec) (load.Application, error) {
		close(entered)
		select {
		case <-proceed:
		case <-ctx.Done():
			return load.Application{}, ctx.Err()
		}
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	h := f.host(t)
	id := f.localID(1)
	a := f.approve(topology.StepCreateFoundingGroup, id)
	done := make(chan error, 1)
	go func() { _, err := h.ApplyAction(deadline(t), f.request(a, id)); done <- err }()
	waitSignal(t, entered)
	f.mu.Lock()
	f.approval.Attempt++
	f.mu.Unlock()
	close(proceed)
	if err := <-done; !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal(err)
	}
	probe, err := f.store.Probe(deadline(t), "work")
	must(t, err)
	if probe.Exists || closed.Load() != 1 {
		t.Fatal("stale approval opened storage or leaked factory ownership", probe, closed.Load())
	}
}

type actionTransport struct {
	discardTransport
	action func(context.Context, load.ActionRequest) (load.ActionResult, error)
}

func (t actionTransport) ApplyAction(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
	return t.action(ctx, req)
}

func TestFoundingFanoutDoesNotWaitForAnEarlierReplica(t *testing.T) {
	functional(t)
	f := newFixture(t)
	ids := []storage.Identity{{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}, {NodeName: "c", ClusterID: "cluster", GroupID: "work", ReplicaID: 3}}
	entered := make(chan uint64, 2)
	proceed := make(chan struct{})
	f.cfg.ResolvePeer = func(ctx context.Context, ref load.ReplicaRef) (load.Peer, error) {
		for _, id := range ids {
			if id.ReplicaID == ref.ReplicaID {
				node := f.node
				node.NodeName, node.MemberID = id.NodeName, id.ReplicaID
				return load.Peer{Node: node, Replica: ref}, ctx.Err()
			}
		}
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	f.cfg.Transport = actionTransport{action: func(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
		entered <- req.To.Replica.ReplicaID
		select {
		case <-proceed:
		case <-ctx.Done():
			return load.ActionResult{}, ctx.Err()
		}
		return load.ActionResult{From: req.To, Authorization: req.Authorization, LocalTarget: req.LocalTarget, Verified: true}, nil
	}}
	h := f.host(t)
	a := f.approve(topology.StepCreateFoundingGroup, ids...)
	done := make(chan error, 1)
	go func() { done <- h.Dispatch(deadline(t), a) }()
	ctx := deadline(t)
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("founding fanout serialized a waiting replica")
		}
	}
	close(proceed)
	must(t, <-done)
}

func TestDispatcherRejectsAnUnverifiedOrMismatchedReply(t *testing.T) {
	functional(t)
	f := newFixture(t)
	id := storage.Identity{ClusterID: "cluster", NodeName: "b", GroupID: "work", ReplicaID: 2}
	var mode atomic.Int32
	f.cfg.Transport = actionTransport{action: func(_ context.Context, req load.ActionRequest) (load.ActionResult, error) {
		result := load.ActionResult{From: req.To, Authorization: req.Authorization, LocalTarget: req.LocalTarget, Verified: true}
		switch mode.Load() {
		case 0:
			result.Verified = false
		case 1:
			result.From.Node.InstanceID = "foreign"
		case 2:
			result.Authorization.Attempt++
		case 3:
			result.LocalTarget.ReplicaID++
		}
		return result, nil
	}}
	h := f.host(t)
	a := f.approve(topology.StepPrepareReplica, id)
	for i := int32(0); i < 4; i++ {
		mode.Store(i)
		if err := h.Dispatch(deadline(t), a); !errors.Is(err, fault.ErrOperatorAuthorization) {
			t.Fatal(err)
		}
	}
	mode.Store(4)
	must(t, h.Dispatch(deadline(t), a))
}

func TestDispatcherRetriesRefusedIdentityOnlyForCurrentApproval(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	remote := storage.Identity{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}
	approval := f.approve(topology.StepPrepareReplica, remote)
	revoke := false
	f.cfg.Transport = actionTransport{action: func(context.Context, load.ActionRequest) (load.ActionResult, error) {
		if revoke {
			f.mu.Lock()
			f.approval.Attempt++
			f.mu.Unlock()
		}
		return load.ActionResult{}, fault.ErrIdentityMismatch
	}}
	h := f.host(t)
	if err := h.Dispatch(t.Context(), approval); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("current approved action did not remain retryable", err)
	}
	revoke = true
	if err := h.Dispatch(t.Context(), approval); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("obsolete approval was retried", err)
	}
}
