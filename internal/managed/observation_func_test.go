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
	"go.etcd.io/raft/v3/raftpb"
)

type observationTransport struct {
	discardTransport
	read func(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
}

func TestObservationKeepsHealthyMajorityAndOwnsUnreturnedReader(t *testing.T) {
	t.Parallel()
	for _, cooperative := range []bool{true, false} {
		name := "ignores_cancellation"
		if cooperative {
			name = "respects_cancellation"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.EffectTimeout = 100 * time.Millisecond
			f.cfg.Active = func(context.Context) (topology.Document, error) {
				return topology.Document{Groups: []topology.Group{{ID: "work"}}}, nil
			}
			f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
				return []storage.Identity{
					{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2},
					{NodeName: "c", ClusterID: "cluster", GroupID: "work", ReplicaID: 3},
					{NodeName: "d", ClusterID: "cluster", GroupID: "work", ReplicaID: 4},
				}, nil
			}
			f.cfg.ResolvePeer = func(_ context.Context, ref load.ReplicaRef) (load.Peer, error) {
				name := map[uint64]string{2: "b", 3: "c", 4: "d"}[ref.ReplicaID]
				return load.Peer{Replica: ref, Node: topology.Identity{ClusterID: "cluster", NodeName: name, MemberID: ref.ReplicaID, InstanceID: "process-" + name}}, nil
			}
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var slowCalls atomic.Int32
			f.cfg.Transport = observationTransport{read: func(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
				if q.To.Replica.ReplicaID == 3 {
					slowCalls.Add(1)
					if cooperative {
						<-ctx.Done()
						return load.ReplicaStatus{}, ctx.Err()
					}
					<-release
					// A late contradictory success must never mutate a returned view.
					return load.ReplicaStatus{Found: true}, nil
				}
				return load.ReplicaStatus{Peer: q.To, Found: true, Status: topology.GroupStatus{
					GroupID: "work", HostNode: q.To.Node.NodeName, Found: true, LeaderID: 2, Term: 1,
					ConfState: raftpb.ConfState{Voters: []uint64{2, 3, 4}},
				}}, nil
			}}
			h := f.host(t)
			t.Cleanup(unblock)
			for range 3 {
				type outcome struct {
					status topology.GroupStatus
					err    error
				}
				result := make(chan outcome, 1)
				go func() {
					status, err := h.ObserveGroup(t.Context(), "work")
					result <- outcome{status, err}
				}()
				select {
				case got := <-result:
					if got.err != nil || !got.status.HealthKnown || got.status.LeaderID != 2 || !got.status.LiveVoters[2] || !got.status.LiveVoters[4] || got.status.LiveVoters[3] {
						t.Fatal("slow peer hid healthy native majority", got.status, got.err)
					}
				case <-time.After(400 * time.Millisecond):
					unblock()
					<-result
					t.Fatal("observation waited for a noncooperative reader")
				}
			}
			if !cooperative {
				if got := slowCalls.Load(); got != 1 {
					t.Fatalf("overlapping calls to stuck source: %d", got)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
				err := h.Quiesce(ctx)
				cancel()
				if !errors.Is(err, fault.ErrShutdownTimeout) {
					t.Fatal("lost ownership of unreturned reader", err)
				}
			}
			unblock()
			must(t, h.Stop(deadline(t)))
		})
	}
}

func TestObservationCancellationInvalidatesPartialResultAndRetainsOwnership(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{"caller", "owner"} {
		t.Run(cause, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
				return []storage.Identity{{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}}, nil
			}
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			f.cfg.Transport = observationTransport{read: func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
				close(started)
				<-release
				return load.ReplicaStatus{Peer: q.To, Found: true, Status: topology.GroupStatus{GroupID: "work", HostNode: "b", Found: true, LeaderID: 2, Term: 1}}, nil
			}}
			h := f.host(t)
			t.Cleanup(unblock)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := h.ObserveGroup(ctx, "work")
				result <- err
			}()
			waitSignal(t, started)
			if cause == "caller" {
				cancel()
			} else {
				h.Fence()
			}
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation became a usable observation", err)
				}
			case <-time.After(time.Second):
				unblock()
				<-result
				t.Fatal("cancellation waited for a stuck callback")
			}
			bounded, finish := context.WithTimeout(t.Context(), 20*time.Millisecond)
			err := h.Quiesce(bounded)
			finish()
			if !errors.Is(err, fault.ErrShutdownTimeout) {
				t.Fatal("canceled observation lost callback ownership", err)
			}
			unblock()
			must(t, h.Stop(deadline(t)))
		})
	}
}

func TestConcurrentNativeObservationWaitsInsteadOfHidingHealthyPeer(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
		return []storage.Identity{{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}}, nil
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var calls atomic.Int32
	f.cfg.Transport = observationTransport{read: func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return load.ReplicaStatus{Peer: q.To, Found: true, Status: topology.GroupStatus{GroupID: "work", HostNode: "b", Found: true, LeaderID: 2, Term: 1}}, nil
	}}
	h := f.host(t)
	t.Cleanup(unblock)
	first := make(chan error, 1)
	go func() {
		status, err := h.ObserveGroup(t.Context(), "work")
		if err == nil && (!status.HealthKnown || !status.LiveVoters[2]) {
			err = errors.New("first observation lost healthy peer")
		}
		first <- err
	}()
	waitSignal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err := h.ObserveGroup(ctx, "work")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatal("concurrent observation treated an in-flight read as missing health", err, calls.Load())
	}
	unblock()
	must(t, <-first)
	for range 10 {
		status, err := h.ObserveGroup(t.Context(), "work")
		if err != nil || !status.HealthKnown || !status.LiveVoters[2] {
			t.Fatal("completed read left a false missing-peer observation", status, err)
		}
	}
	h.mu.Lock()
	remaining := len(h.observationGroups)
	h.mu.Unlock()
	if remaining != 0 {
		t.Fatal("observation gates retained completed groups", remaining)
	}
}

func (t observationTransport) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return t.read(ctx, q)
}

func TestObservationRefusalDoesNotAttestAReplicaButMalformedSuccessFails(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stale_process", "unreadable", "wrong_process", "wrong_group", "false_found", "valid"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.Active = func(context.Context) (topology.Document, error) {
				return topology.Document{Groups: []topology.Group{{ID: "work"}}}, nil
			}
			f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
				return []storage.Identity{{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}}, nil
			}
			f.cfg.Transport = observationTransport{read: func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
				if mode == "stale_process" {
					return load.ReplicaStatus{}, fault.ErrIdentityMismatch
				}
				if mode == "unreadable" {
					return load.ReplicaStatus{}, fault.ErrUnreadable
				}
				result := load.ReplicaStatus{Peer: q.To, Found: true, Status: topology.GroupStatus{GroupID: "work", HostNode: "b", Found: true, LeaderID: 2, Term: 1}}
				if mode == "wrong_process" {
					result.Peer.Node.InstanceID = "other"
				}
				if mode == "wrong_group" {
					result.Status.GroupID = "other"
				}
				if mode == "false_found" {
					result.Found = false
				}
				return result, nil
			}}
			h := f.host(t)
			status, err := h.ObserveGroup(t.Context(), "work")
			switch mode {
			case "stale_process", "unreadable":
				if !errors.Is(err, fault.ErrUnavailable) || status.LiveVoters[2] {
					t.Fatal("refusal attested a peer or became permanent failure", status, err)
				}
			case "valid":
				if err != nil || !status.HealthKnown || !status.LiveVoters[2] || status.LeaderID != 2 {
					t.Fatal(status, err)
				}
			default:
				if !errors.Is(err, fault.ErrIdentityMismatch) {
					t.Fatal("malformed success accepted", status, err)
				}
			}
		})
	}
}
