//go:build functional

package managed

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestGrantedStartupSurvivesCanceledWaiterAndReturnsExactReceipt(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, grant := fixtureAuthority(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		calls.Add(1)
		close(entered)
		<-release
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	_, err := h.InstallHostFence(deadline(t), grant)
	must(t, err)
	req := f.request(*grant.Action, f.localID(1))
	req.Grant = grant.Clone()
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	waited := make(chan error, 1)
	go func() { _, err := h.ApplyAction(ctx, req); waited <- err }()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("factory did not start")
	}
	cancel()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	var result load.ActionResult
	retryCtx := deadline(t)
	await(retryCtx, t, func() bool {
		result, err = h.ApplyAction(retryCtx, req)
		// A per-effect timeout may expire while the accepted startup remains
		// owned. Retry the exact grant within the overall test deadline.
		if errors.Is(err, context.DeadlineExceeded) && retryCtx.Err() == nil || errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if len(result.Receipts) != 1 || calls.Load() != 1 {
		t.Fatal(result, calls.Load())
	}
	must(t, result.Receipts[0].Validate(grant))
	var again load.ActionResult
	await(deadline(t), t, func() bool {
		again, err = h.ApplyAction(deadline(t), req)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if !reflect.DeepEqual(result, again) || calls.Load() != 1 {
		t.Fatal("retry repeated effect", again, calls.Load())
	}
	result.Receipts[0].Action.NodeName = "mutated"
	cached, err := s.Receipt(t.Context(), grant, actionTarget(req))
	must(t, err)
	if !cached.Found || cached.Receipt.Action.NodeName != grant.Action.NodeName {
		t.Fatal("receipt alias escaped", cached.Receipt)
	}
	fence := grant.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	_, err = h.InstallHostFence(deadline(t), fence)
	must(t, err)
	if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("old cached success authorized a later action", err)
	}
}

func TestGrantedRetirementFinishesWhileNewFenceJoinsCallback(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var callbacks atomic.Int32
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		callbacks.Add(1)
		close(entered)
		<-release
		return retirementProof(req), nil
	}
	f.cfg.ForgetGrantedReplica = func(ctx context.Context, g topology.AuthorityGrant, expected uint64) error {
		if g.Action == nil || g.Action.Kind != topology.StepCleanupReplicaState || g.Action.ReplicaID != expected {
			return fault.ErrOperationAuthorization
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if r, ok := f.records[g.Key.GroupID]; ok && r.ReplicaID != expected {
			return fault.ErrIdentityMismatch
		}
		delete(f.records, g.Key.GroupID)
		return ctx.Err()
	}
	h := f.host(t)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	a := f.approve(topology.StepCleanupReplicaState, f.localID(1))
	grant := initial.Clone()
	grant.Key.Revision++
	grant.Action = &a
	grant.Resources = grant.Resources[1:]
	_, err = h.InstallHostFence(deadline(t), grant)
	must(t, err)
	req := f.request(a, f.localID(1))
	req.Grant = grant
	result := make(chan load.ActionResult, 1)
	errorsCh := make(chan error, 1)
	go func() { r, err := h.ApplyAction(deadline(t), req); result <- r; errorsCh <- err }()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("retirement did not reach callback")
	}
	fence := grant.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	fenced := make(chan error, 1)
	go func() { _, err := h.InstallHostFence(deadline(t), fence); fenced <- err }()
	await(deadline(t), t, func() bool {
		floor, err := s.Current(t.Context(), "work")
		must(t, err)
		return floor.Record.Grant.Key == fence.Key && !floor.Record.Complete
	})
	// Approval changed before callback return. The owned exact old grant may
	// still finish; the new host fence must wait for its durable receipt.
	f.approve(topology.StepDeleteGroupState, f.localID(1))
	once.Do(func() { close(release) })
	must(t, <-errorsCh)
	got := <-result
	if len(got.Receipts) != 1 {
		t.Fatal(got)
	}
	must(t, got.Receipts[0].Validate(grant))
	must(t, <-fenced)
	if callbacks.Load() != 1 {
		t.Fatal("retirement repeated", callbacks.Load())
	}
	if probe, err := f.store.Probe(t.Context(), "work"); err != nil || probe.Exists {
		t.Fatal(probe, err)
	}
	if cached, err := s.Receipt(t.Context(), grant, actionTarget(req)); err != nil || !cached.Found || !reflect.DeepEqual(cached.Receipt, got.Receipts[0]) {
		t.Fatal(cached.Receipt, cached.Found, err)
	}
}

func TestGrantedMembershipUsesInstalledGroupAuthority(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	handle, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	await(deadline(t), t, handle.Core().IsLeader)
	must(t, handle.Core().ReadBarrier(deadline(t)))
	a := f.approve(topology.StepAddLearner, storage.Identity{ClusterID: f.node.ClusterID, NodeName: "b", GroupID: "work", ReplicaID: 2})
	grant := initial.Clone()
	grant.Key.Revision++
	grant.Action = &a
	grant.Resources = grant.Resources[:1]
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	req := f.request(a, f.localID(1))
	req.LocalTarget, req.Grant = storage.Identity{}, grant
	if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("uninstalled grant changed native membership", err)
	}
	if slices.Contains(handle.Core().Status().ConfState.Learners, 2) {
		t.Fatal("membership changed before grant installation")
	}
	fr := load.FenceRequest{From: f.node, To: f.node, ReplicaID: 1, Resource: grant.Resources[0], Grant: grant}
	proof, err := h.InstallFence(deadline(t), fr)
	must(t, err)
	must(t, proof.Evidence.Validate(grant))
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if len(result.Receipts) != 1 || result.Receipts[0].AppliedIndex == 0 || !slices.Contains(handle.Core().Status().ConfState.Learners, 2) {
		t.Fatal(result, handle.Core().Status())
	}
	must(t, result.Receipts[0].Validate(grant))
	var again load.ActionResult
	await(deadline(t), t, func() bool {
		again, err = h.ApplyAction(deadline(t), req)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if !reflect.DeepEqual(result, again) {
		t.Fatal("membership retry changed consumption proof", again)
	}
	bad := fr
	bad.To.InstanceID = "old"
	if _, err := h.InstallFence(deadline(t), bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale endpoint installed fence", err)
	}
	fr.Grant.Key.Revision++
	fr.Grant.Action, fr.Grant.Purpose = nil, topology.AuthorityFence
	await(deadline(t), t, func() bool {
		_, err = h.InstallFence(deadline(t), fr)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("old action crossed group fence", err)
	}
}

func TestGrantedRetirementRejectsIncompleteCallbackProofAndKeepsEvidence(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		result := retirementProof(req)
		result.Grant = topology.AuthorityGrant{}
		return result, nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	a := f.approve(topology.StepCleanupReplicaState, f.localID(1))
	grant := initial.Clone()
	grant.Key.Revision++
	grant.Action, grant.Resources = &a, grant.Resources[1:]
	_, err = h.InstallHostFence(deadline(t), grant)
	must(t, err)
	req := f.request(a, f.localID(1))
	req.Grant = grant
	if result, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrOperationAuthorization) || len(result.Receipts) != 0 {
		t.Fatal("incomplete callback proof counted as success", result, err)
	}
	if found, err := s.Receipt(t.Context(), grant, actionTarget(req)); err != nil || found.Found {
		t.Fatal("refused cleanup produced a receipt", found.Found, err)
	}
	fence := grant.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	_, err = h.InstallHostFence(deadline(t), fence)
	must(t, err)
	if probe, err := f.store.Probe(t.Context(), "work"); err != nil || !probe.Exists || probe.Identity != req.LocalTarget {
		t.Fatal("fencing discarded unverified cleanup evidence", probe, err)
	}
	f.mu.Lock()
	_, recorded := f.records["work"]
	f.mu.Unlock()
	if !recorded {
		t.Fatal("unverified cleanup erased the local boot ledger")
	}
}
