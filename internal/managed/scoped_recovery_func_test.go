//go:build functional

package managed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestStartupRecaptureIgnoresIndependentVersionAndRejectsAssignmentChange(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"unrelated_version", "assignment", "founding_seed", "storage", "creation_receipt"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var closed atomic.Int32
			f.cfg.Factory = func(ctx context.Context, spec load.ReplicaSpec) (load.Application, error) {
				if spec.ApprovedVersion != 1 {
					t.Error("factory context changed", spec.ApprovedVersion)
				}
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return load.Application{}, ctx.Err()
				}
				return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
			}
			h := f.host(t)
			t.Cleanup(unblock)
			result := make(chan error, 1)
			ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
			go func() { _, err := h.StartReplica(deadline(t), ref, StartRecovery); result <- err }()
			waitSignal(t, entered)
			f.mu.Lock()
			scope := f.scopes["work"]
			scope.Version = 9
			// Break slice aliases to the owned startup capture.
			scope.Group = (topology.Document{Groups: []topology.Group{scope.Group}}).Clone().Groups[0]
			switch change {
			case "assignment":
				scope.Group.Replicas[0].Role, scope.Replica.Role = topology.ReplicaLearner, topology.ReplicaLearner
			case "founding_seed":
				scope.Founding = false
			case "storage":
				scope.Group.Storage = topology.StorageMemory
			case "creation_receipt":
				scope.Started, scope.Established = true, true
			}
			f.scopes["work"] = scope
			f.mu.Unlock()
			unblock()
			err := <-result
			if change == "unrelated_version" {
				must(t, err)
				handle, found := h.Lookup("work")
				if !found || !handle.Core().Running() || closed.Load() != 0 {
					t.Fatal("independent approval interrupted recovery", found, closed.Load())
				}
			} else {
				want := fault.ErrVersionConflict
				if change == "creation_receipt" {
					want = fault.ErrReplicaNotReady
				}
				if !errors.Is(err, want) || closed.Load() != 1 {
					t.Fatal("changed assignment/seed published or lost application", err, closed.Load())
				}
				if _, found := h.Lookup("work"); found {
					t.Fatal("invalid startup handle published")
				}
			}
		})
	}
}

type recoveryRoundKey struct{}

func TestRecoveryRotatesPastBlockedAuthority(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	validate := f.cfg.ValidateGrant
	f.cfg.ValidateGrant = func(ctx context.Context, grant topology.AuthorityGrant) error {
		if grant.Key.GroupID == "other" {
			<-ctx.Done()
			return ctx.Err()
		}
		return validate(ctx, grant)
	}
	ready := make(chan string, 4)
	f.cfg.Factory = func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
		ready <- spec.Identity.GroupID
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	// Keep maintenance idle; enable just the explicit recovery requests below.
	var rounds atomic.Int32
	f.cfg.Assignments = func(ctx context.Context) ([]load.ReplicaRef, error) {
		if ctx.Value(recoveryRoundKey{}) == nil {
			return nil, nil
		}
		refs := []load.ReplicaRef{{GroupID: "other", ReplicaID: 1}, {GroupID: "work", ReplicaID: 1}}
		if rounds.Add(1) > 1 {
			refs = append([]load.ReplicaRef{{GroupID: "first", ReplicaID: 1}}, refs...)
		}
		return refs, nil
	}
	h := f.host(t)
	requests := context.WithValue(t.Context(), recoveryRoundKey{}, true)
	for range 2 {
		ctx, cancel := context.WithTimeout(requests, time.Second)
		_ = h.RecoverAssignments(ctx)
		cancel()
	}
	select {
	case id := <-ready:
		if id != "work" {
			t.Fatal("blocked group started", id)
		}
	case <-deadline(t).Done():
		t.Fatal("first blocked authority starved healthy sibling across recovery rounds")
	}
}

func TestMemoryReseatUsesOnlyItsScopeAndHonorsConcurrentHold(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"blocked_sibling", "own_scope", "held"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			withMemory(t, f)
			restartedReplica(t, f, topology.StorageMemory, topology.ReplicaVoter)
			progress := f.cfg.Progress
			entered, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var reads, calls atomic.Int32
			f.cfg.Progress = func(ctx context.Context) (topology.ProgressPage, error) {
				page, err := progress(ctx)
				for i := range page.Groups {
					row := &page.Groups[i]
					if row.GroupID == "other" || mode == "own_scope" && row.GroupID == "work" {
						row.OperationID = topology.MakeOperationID(5, 1)
						row.Blocked = &topology.BlockedReason{Code: "cleanup_failed"}
					}
				}
				switch reads.Add(1) {
				case 1:
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return page, ctx.Err()
					}
				case 2:
					close(second)
				}
				return page, err
			}
			submitted := make(chan storage.Identity, 4)
			f.cfg.ReseatOwn = func(_ context.Context, id storage.Identity) error {
				calls.Add(1)
				submitted <- id
				f.setScope("work", 2, false, false)
				return nil
			}
			h := f.host(t)
			t.Cleanup(unblock)
			startParked(t, h)
			waitSignal(t, entered)
			if mode == "held" {
				must(t, h.StopReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StopExplicit))
			}
			unblock()
			switch mode {
			case "blocked_sibling":
				select {
				case id := <-submitted:
					if id != f.localID(1) {
						t.Fatal("reseat lost exact predecessor", id)
					}
				case <-deadline(t).Done():
					t.Fatal("unrelated debt blocked own reseat")
				}
			case "own_scope":
				waitSignal(t, second)
				if calls.Load() != 0 {
					t.Fatal("busy own scope was reseated")
				}
			case "held":
				joined := make(chan struct{})
				go func() { h.reseatWG.Wait(); close(joined) }()
				waitSignal(t, joined)
				if calls.Load() != 0 || len(h.Held()) != 1 {
					t.Fatal("concurrent local hold was bypassed", calls.Load(), h.Held())
				}
			}
		})
	}
}
