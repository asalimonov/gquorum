package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"encoding/json"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type pressureStore struct {
	storage.Store
	mu   sync.Mutex
	gate chan struct{}
}

func (s *pressureStore) SaveReady(ctx context.Context, id string, write storage.ReadyWrite) error {
	s.mu.Lock()
	gate := s.gate
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.SaveReady(ctx, id, write)
}

func TestProposalPressureBoundsCancellationAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop=%t", stop), func(t *testing.T) {
			const limit = 32
			s := &pressureStore{Store: pressureOpenStore(t)}
			m := newMachine()
			owner, err := load.NewStandalone(load.StandaloneOptions{Identity: storage.Identity{GroupID: "load", NodeName: "1", ClusterID: "cluster", ReplicaID: 1}, Mode: load.Founding, Peers: []raft.Peer{{ID: 1}}, Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50}, MaxPendingProposals: limit}, Store: s, StateMachine: m, Transport: pressureTransport{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			pressureMust(t, err)
			g := owner.Group()
			t.Cleanup(func() { pressureMust(t, owner.Stop(pressureContext(t))) })
			pressureMust(t, owner.Start(t.Context()))
			pressureEventually(t, g.IsLeader)
			s.mu.Lock()
			s.gate = make(chan struct{})
			gate := s.gate
			s.mu.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			var wg sync.WaitGroup
			errs := make([]error, limit)
			// Join callers before cleanup stops the engine/store.
			t.Cleanup(func() { cancel(); wg.Wait() })
			for i := range limit {
				wg.Go(func() { _, errs[i] = g.Propose(ctx, pressureCommand(fmt.Sprint(i))) })
			}
			pressureEventually(t, func() bool {
				p := g.ProposalStats()
				return p.Limit == limit && p.InFlight == limit && p.AwaitingCommit == limit && p.Queued == 0
			})
			started := time.Now()
			for range 4000 {
				_, err := g.Propose(t.Context(), pressureCommand("shed"))
				if !errors.Is(err, fault.ErrOverloaded) {
					t.Fatalf("pressure result: %v", err)
				}
			}
			if time.Since(started) > time.Second {
				t.Fatal("bounded rejection blocked behind the writer")
			}
			if len(m.values()) != 0 {
				t.Fatal("acknowledged undurable state")
			}
			cancel()
			wg.Wait()
			for _, err := range errs {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled waiter result: %v", err)
				}
			}
			pressureEventually(t, func() bool { p := g.ProposalStats(); return p.InFlight == 0 && p.AwaitingCommit == 0 && p.Queued == 0 })
			if g.WALStall() == 0 {
				t.Fatal("cleanup was not observed during a blocked writer")
			}
			if stop {
				started := time.Now()
				pressureMust(t, owner.Stop(pressureContext(t)))
				if time.Since(started) > time.Second || g.ProposalStats().AwaitingCommit != 0 {
					t.Fatal("blocked writer or waiter retained after shutdown")
				}
				return
			}
			close(gate)
			pressureEventually(t, func() bool { return len(m.values()) == limit && g.WALStall() == 0 })
			_, err = g.Propose(pressureContext(t), pressureCommand("after"))
			pressureMust(t, err)
			if len(m.values()) != limit+1 || g.Error() != nil {
				t.Fatal("lost uncertain commits or failed to resume")
			}
		})
	}
}

type pressureTransport struct{}

func (pressureTransport) Send(ctx context.Context, _ []raftpb.Message) error { return ctx.Err() }
func pressureCommand(id string) load.Proposal {
	data, _ := json.Marshal(putCommand{RequestID: id, Key: id, Value: id})
	return load.Proposal{RequestID: id, Data: data}
}
func pressureOpenStore(t *testing.T) *walstore.Store {
	t.Helper()
	prewrite := false
	s, err := walstore.Open(walstore.Options{RootDir: t.TempDir(), NodeName: "1", ClusterID: "cluster", SegmentBytes: 1 << 20, Prewrite: &prewrite})
	pressureMust(t, err)
	t.Cleanup(func() { pressureMust(t, s.Close()) })
	return s
}
func pressureMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func pressureContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func pressureEventually(t *testing.T, fn func() bool) {
	t.Helper()
	ctx := pressureContext(t)
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal("condition did not converge", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
