//go:build functional

package load_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Public-package integration checks that the adapter preserves both recovery
// and caller-owned deduplication, including committed entries after a snapshot.
func TestStandaloneRecoveryAndMembershipOwnership(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	prewrite := false
	s, err := walstore.Open(walstore.Options{RootDir: t.TempDir(), NodeName: "a", ClusterID: "cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite})
	require(t, err)
	t.Cleanup(func() { require(t, s.Close()) })
	m := &deduplicatingMachine{}
	peers := []raft.Peer{{ID: 1}}
	opts := load.StandaloneOptions{Identity: storage.Identity{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 1},
		Mode: load.Founding, Peers: peers, Store: s, StateMachine: m, Transport: transport{},
		Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	owner, err := load.NewStandalone(opts)
	require(t, err)
	defer func() { require(t, owner.Stop(context.Background())) }()
	g := owner.Group()
	peers[0].ID = 99
	if g.Running() || g.Identity() != opts.Identity || !slices.Equal(g.Status().ConfState.Voters, []uint64{1}) {
		t.Fatal("constructor started workers or retained caller peer slice", g.Status())
	}
	// The fixed coordinator refuses removal of the final voter before any raw
	// configuration proposal can enter the engine, even without a health source.
	err = owner.ChangeMembership(ctx, raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 1}}}, nil)
	if !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
	require(t, owner.Start(ctx))
	require(t, g.WaitReady(ctx))
	waitLeader(t, ctx, g)
	propose := func(group *load.Group, id, data string) {
		t.Helper()
		result, err := group.Propose(ctx, load.Proposal{RequestID: id, Data: []byte(data)})
		require(t, err)
		if result.Code != "ok" || string(result.Data) != data {
			t.Fatal(result)
		}
	}
	propose(g, "one", "first")
	require(t, g.Snapshot(ctx))
	propose(g, "two", "tail")
	require(t, owner.Stop(ctx))
	if _, err := s.Load(ctx, "work"); err != nil {
		t.Fatal("Stop closed borrowed storage:", err)
	}
	opts.StateMachine = &deduplicatingMachine{}
	opts.Mode, opts.Peers = load.Joining, nil
	next, err := load.NewStandalone(opts)
	require(t, err)
	defer func() { require(t, next.Stop(context.Background())) }()
	require(t, next.Start(ctx))
	require(t, next.Group().WaitReady(ctx))
	waitLeader(t, ctx, next.Group())
	propose(next.Group(), "one", "first")
	propose(next.Group(), "two", "tail")
	result, err := next.Group().Propose(ctx, load.Proposal{RequestID: "two", Data: []byte("different")})
	require(t, err)
	if result.Code != "conflict" || opts.StateMachine.(*deduplicatingMachine).count() != 2 {
		t.Fatal("snapshot/tail recovery lost request identities", result)
	}
	if _, err := g.Propose(ctx, load.Proposal{RequestID: "old", Data: []byte("old")}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("old handle redirected into the replacement:", err)
	}
}

type transport struct{}

func (transport) Send(ctx context.Context, _ []raftpb.Message) error { return ctx.Err() }

type deduplicatingMachine struct {
	mu       sync.Mutex
	requests map[string]string
}

func (m *deduplicatingMachine) Apply(_ context.Context, cmd load.Command) (load.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, found := m.requests[cmd.RequestID]; found && old != string(cmd.Data) {
		return load.Result{Code: "conflict"}, nil
	}
	m.requests[cmd.RequestID] = string(cmd.Data)
	return load.Result{Code: "ok", Data: cmd.Data}, nil
}

func (m *deduplicatingMachine) Snapshot(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(m.requests)
}

func (m *deduplicatingMachine) Restore(_ context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = make(map[string]string)
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &m.requests)
}

func (m *deduplicatingMachine) count() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.requests) }

func waitLeader(t *testing.T, ctx context.Context, g *load.Group) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !g.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

func require(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
