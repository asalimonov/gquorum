//go:build functional

package managed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type reseatCall struct {
	group   string
	version uint64
}

// restartedReplica describes replica 1 of "work" after a process restart: it is
// established in Active and recorded in the ledger, but its store is empty.
func restartedReplica(t *testing.T, f *fixture, class topology.StorageClass, role topology.ReplicaRole) {
	t.Helper()
	f.setScope("work", 1, false, true)
	f.mu.Lock()
	scope := f.scopes["work"]
	scope.Group.Storage = class
	scope.Replica.Role = role
	scope.Group.Replicas[0].Role = role
	f.scopes["work"] = scope
	f.records["work"] = metastore.ReplicaRecord{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID, ReplicaID: 1, Origin: "fresh"}
	f.mu.Unlock()
	active := f.cfg.Active
	f.cfg.Active = func(ctx context.Context) (topology.Document, error) {
		doc, err := active(ctx)
		doc.Version = 7
		return doc, err
	}
}

func startParked(t *testing.T, h *Host) {
	t.Helper()
	_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
	var parked *ParkedError
	if !errors.As(err, &parked) || parked.Reason != ParkStoreMissing {
		t.Fatal("restarted replica did not park store_missing", err)
	}
}

func (h *Host) reseatRunning(f *fixture) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ownReseats[f.localID(1)]
}

func TestParkedMemoryVoterReseatsItsOwnReplica(t *testing.T) {
	functional(t)
	f := newFixture(t)
	withMemory(t, f)
	restartedReplica(t, f, topology.StorageMemory, topology.ReplicaVoter)
	calls := make(chan reseatCall, 16)
	f.cfg.ReseatOwn = func(ctx context.Context, group string, version uint64) error {
		calls <- reseatCall{group, version}
		// The committed repair approves a fresh identity; once it converges the
		// parked identity leaves both Target and Active.
		f.setScope("work", 2, false, false)
		return ctx.Err()
	}
	h := f.host(t)
	startParked(t, h)
	select {
	case call := <-calls:
		if call != (reseatCall{"work", 7}) {
			t.Fatal("reseat not pinned to the observed Active version", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked memory voter was not reseated")
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.reseatRunning(f) || len(h.Parked()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("reseat loop did not finish after the identity left topology", h.Parked())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(calls) != 0 {
		t.Fatal("successor was reseated again")
	}
}

func TestOnlyMemoryVotersAreReseated(t *testing.T) {
	functional(t)
	for name, c := range map[string]struct {
		class   topology.StorageClass
		role    topology.ReplicaRole
		claimed bool // the identity is marked so its refusal is logged once
	}{
		"persistent voter": {topology.StorageDefault, topology.ReplicaVoter, false},
		"memory learner":   {topology.StorageMemory, topology.ReplicaLearner, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			withMemory(t, f)
			restartedReplica(t, f, c.class, c.role)
			var calls atomic.Int32
			f.cfg.ReseatOwn = func(context.Context, string, uint64) error { calls.Add(1); return nil }
			h := f.host(t)
			startParked(t, h)
			startParked(t, h)
			time.Sleep(100 * time.Millisecond)
			if calls.Load() != 0 || h.reseatRunning(f) != c.claimed {
				t.Fatal("unexpected reseat", calls.Load(), h.reseatRunning(f))
			}
		})
	}
}

func TestDemotedMemoryVoterStopsReseating(t *testing.T) {
	functional(t)
	f := newFixture(t)
	withMemory(t, f)
	restartedReplica(t, f, topology.StorageMemory, topology.ReplicaVoter)
	var calls atomic.Int32
	f.cfg.ReseatOwn = func(context.Context, string, uint64) error {
		if calls.Add(1) == 1 {
			// The voter configuration demotes the unavailable voter before any reseat commits.
			f.mu.Lock()
			scope := f.scopes["work"]
			scope.Replica.Role, scope.Group.Replicas[0].Role = topology.ReplicaLearner, topology.ReplicaLearner
			f.scopes["work"] = scope
			f.mu.Unlock()
		}
		return fault.ErrOperatorAuthorization
	}
	h := f.host(t)
	startParked(t, h)
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() == 0 || h.reseatRunning(f) {
		if time.Now().After(deadline) {
			t.Fatal("reseat loop kept running for a learner", calls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A later preflight reports the learner once and starts no loop.
	startParked(t, h)
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 || !h.reseatRunning(f) || len(h.Parked()) != 1 {
		t.Fatal("demoted replica was reseated or lost its park", calls.Load(), h.Parked())
	}
}

func TestStopJoinsTheOwnReseatLoop(t *testing.T) {
	functional(t)
	f := newFixture(t)
	withMemory(t, f)
	restartedReplica(t, f, topology.StorageMemory, topology.ReplicaVoter)
	entered := make(chan struct{})
	var once atomic.Bool
	var canceled atomic.Bool
	f.cfg.ReseatOwn = func(ctx context.Context, _ string, _ uint64) error {
		if once.CompareAndSwap(false, true) {
			close(entered)
		}
		<-ctx.Done()
		canceled.Store(true)
		return fault.ErrUnavailable
	}
	h := f.host(t)
	startParked(t, h)
	waitSignal(t, entered)
	must(t, h.Stop(deadline(t)))
	if !canceled.Load() || h.reseatRunning(f) {
		t.Fatal("Stop returned before the reseat loop joined")
	}
}
