//go:build functional

package managed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type interruptedQuarantine struct {
	storage.Store
	calls atomic.Int32
}

func (s *interruptedQuarantine) Quarantine(ctx context.Context, group string, id uint64) (string, error) {
	path, err := s.Store.Quarantine(ctx, group, id)
	if err == nil && s.calls.Add(1) == 1 {
		return path, errors.New("reply lost after quarantine move")
	}
	return path, err
}

func TestRetirementRetryAfterMoveStillVerifiesApplicationBeforeForgettingLedger(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	f.cfg.Store = &interruptedQuarantine{Store: f.store}
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		return lifecycle.RetentionDecision{Collect: true}, nil
	}
	var callbacks atomic.Int32
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		callbacks.Add(1)
		return retirementProof(req), nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	id := f.localID(1)
	f.setScope("work", 2, false, false)
	req := installFixtureHostAction(t, f, h, initial, topology.StepCleanupReplicaState, id, 11)
	if _, err := h.ApplyAction(deadline(t), req); err == nil {
		t.Fatal("injected post-move failure disappeared")
	}
	if found, err := f.cfg.LookupRecord(deadline(t), "work"); err != nil || !found.Found {
		t.Fatal("uncertain move lost ledger", found.Found, err)
	}
	probe, err := f.store.Probe(deadline(t), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("quarantine injection did not move actual state")
	}
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("retention collected unforgotten evidence", err)
	}
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if len(result.Receipts) != 1 || callbacks.Load() != 2 {
		t.Fatal("absent Raft directory skipped application verification", result, callbacks.Load())
	}
	if found, err := f.cfg.LookupRecord(deadline(t), "work"); err != nil || found.Found {
		t.Fatal("verified retirement retained ledger", found.Found, err)
	}
	must(t, h.CollectRetired(deadline(t)))
	retired, err := f.store.Retired(deadline(t))
	must(t, err)
	if len(retired) != 0 {
		t.Fatal("authorized retention did not collect", retired)
	}
}

func TestMismatchedRetirementProofPreservesRaftAndLedger(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	var mode atomic.Int32
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		result := retirementProof(req)
		if mode.Load() == 0 {
			result.Authorization.Attempt++
		} else {
			result.Grant.Key.Revision++
		}
		return result, nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	id := f.localID(1)
	f.setScope("work", 2, false, false)
	req := installFixtureHostAction(t, f, h, initial, topology.StepCleanupReplicaState, id, 11)
	for i := int32(0); i < 2; i++ {
		mode.Store(i)
		if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal(err)
		}
		probe, err := f.store.Probe(deadline(t), "work")
		must(t, err)
		if !probe.Exists || probe.Identity != id {
			t.Fatal("unverified retirement mutated Raft state", probe)
		}
		if record, err := f.cfg.LookupRecord(deadline(t), "work"); err != nil || !record.Found || record.Record.ReplicaID != 1 {
			t.Fatal(record.Record, record.Found, err)
		}
	}
}

func TestRetirementCannotStopOrEraseAnApprovedSuccessor(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	var retired atomic.Uint64
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		retired.Store(req.Identity.ReplicaID)
		return retirementProof(req), nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	old := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err = h.StartReplica(deadline(t), old, StartOperator)
	must(t, err)
	must(t, h.StopReplica(deadline(t), old, StopExplicit))
	_, err = f.store.Quarantine(deadline(t), "work", 1)
	must(t, err)
	f.setScope("work", 2, false, false)
	installFixtureHostAction(t, f, h, initial, topology.StepPrepareReplica, f.localID(2), 11)
	successor, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2}, StartOperator)
	must(t, err)
	id := f.localID(1)
	req := installFixtureHostAction(t, f, h, initial, topology.StepCleanupReplicaState, id, 12)
	_, err = h.ApplyAction(deadline(t), req)
	must(t, err)
	if !successor.Core().Running() || retired.Load() != 1 {
		t.Fatal("cleanup reached successor", retired.Load())
	}
	probe, err := f.store.Probe(deadline(t), "work")
	must(t, err)
	if probe.Identity != f.localID(2) {
		t.Fatal(probe)
	}
	if record, err := f.cfg.LookupRecord(deadline(t), "work"); err != nil || !record.Found || record.Record.ReplicaID != 2 {
		t.Fatal(record.Record, record.Found, err)
	}
}

func TestShutdownWaitsForRetirementCallbackAndKeepsBorrowedStorageOpen(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.cfg.RetireApplication = func(ctx context.Context, _ load.RetirementRequest) (load.RetirementResult, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return load.RetirementResult{}, ctx.Err()
	}
	h := f.host(t)
	id := f.localID(1)
	req := installFixtureHostAction(t, f, h, initial, topology.StepDeleteGroupState, id, 11)
	done := make(chan error, 1)
	go func() {
		_, err := h.ApplyAction(deadline(t), req)
		done <- err
	}()
	waitSignal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := h.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	waitSignal(t, canceled)
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, h.Stop(deadline(t)))
	if _, err := f.store.Probe(deadline(t), "work"); err != nil {
		t.Fatal("host closed borrowed storage", err)
	}
}

func installFixtureHostAction(t *testing.T, f *fixture, h *Host, initial topology.AuthorityGrant, kind topology.StepKind, id storage.Identity, revision uint64) load.ActionRequest {
	t.Helper()
	a := f.approve(kind, id)
	grant := initial.Clone()
	grant.Key.Revision = revision
	grant.Action = &a
	grant.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: id.GroupID, NodeName: id.NodeName}}
	grant.Targets = []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: f.node.MemberID, ReplicaID: id.ReplicaID}}
	proof, err := h.InstallHostFence(deadline(t), grant)
	must(t, err)
	must(t, proof.Validate(grant))
	req := f.request(a, id)
	req.Grant = grant
	return req
}
