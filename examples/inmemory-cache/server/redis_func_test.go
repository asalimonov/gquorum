//go:build functional

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/cache"
	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/memstore"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type singleReplica struct{}

func (singleReplica) Send(context.Context, []raftpb.Message) error {
	return errors.New("single-replica fixture has no peers")
}

// startReplica opens one local replica of the cache group with a fresh
// in-memory machine over a retained store, and serves it on a Redis listener.
// Multi-node replication is covered by the Compose run, not in-process.
func startReplica(ctx context.Context, t *testing.T, store storage.Store, mode load.StartMode) (string, http.Handler, func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	id := storage.Identity{NodeName: "node1", ClusterID: "test", GroupID: groupID, ReplicaID: 1}
	opts := load.StandaloneOptions{Identity: id, InstanceID: "process-" + string(mode), Mode: mode, Store: store, StateMachine: cache.NewMachine(),
		Transport: singleReplica{}, Logger: logger, Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond}, SnapshotThreshold: 100}}
	if mode == load.Founding {
		opts.Peers = []raft.Peer{{ID: 1}}
	}
	owner, err := load.NewStandalone(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for !owner.Group().IsLeader() {
		if err := wait(ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	server := newRedisServer(func() (*load.Group, error) { return owner.Group(), nil }, logger)
	go func() { _ = server.Serve(serveCtx, listener) }()
	return listener.Addr().String(), healthHandler(func() (*load.Group, error) { return owner.Group(), nil }), func() {
		cancel()
		if err := server.Wait(context.Background()); err != nil {
			t.Error(err)
		}
		if err := owner.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}
}

func TestRedisSubsetOverReplicaAndRecoveryFromLog(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	root := filepath.Join(t.TempDir(), "raft")
	openStore := func() *walstore.Store {
		t.Helper()
		s, err := walstore.Open(walstore.Options{RootDir: root, NodeName: "node1", ClusterID: "test", DisableFSync: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	store := openStore()
	addr, health, stop := startReplica(ctx, t, store, load.Founding)
	probe := func(want int) {
		t.Helper()
		recorder := httptest.NewRecorder()
		health.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
		if recorder.Code != want {
			t.Fatalf("health status=%d want=%d", recorder.Code, want)
		}
	}
	probe(http.StatusOK)
	c, err := resp.Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	do := func(args ...string) resp.Reply {
		t.Helper()
		reply, err := c.Do(5*time.Second, args...)
		if err != nil {
			t.Fatal(args, err)
		}
		return reply
	}
	if r := do("PING"); r.Str != "PONG" {
		t.Fatal(r)
	}
	before := do("INFO").Str
	probe(http.StatusOK)
	if after := do("INFO").Str; after != before {
		t.Fatalf("readiness probe changed Raft state: before=%q after=%q", before, after)
	}
	if r := do("SET", "a", "1"); r.Str != "OK" {
		t.Fatal(r)
	}
	if r := do("GET", "a"); r.Str != "1" {
		t.Fatal(r)
	}
	if r := do("GET", "missing"); !r.Nil {
		t.Fatal(r)
	}
	if r := do("EXISTS", "a", "missing"); r.Int != 1 {
		t.Fatal(r)
	}
	if r := do("DEL", "a", "missing"); r.Int != 1 {
		t.Fatal(r)
	}
	if r := do("GET"); r.Err() == nil {
		t.Fatal("wrong arity accepted")
	}
	if r := do("INCR", "a"); r.Err() == nil {
		t.Fatal("unsupported command accepted")
	}
	// Grow beyond the former 512-key capacity; every value must survive.
	const keyCount = 1024
	for i := range keyCount {
		if r := do("SET", fmt.Sprintf("key:%04d", i), fmt.Sprint(i)); r.Str != "OK" {
			t.Fatal(r)
		}
	}
	probe(http.StatusOK) // The readiness probe must leave cache keys intact.
	if r := do("DBSIZE"); r.Int != keyCount {
		t.Fatal("size", r)
	}
	if r := do("GET", "key:0000"); r.Str != "0" {
		t.Fatal("oldest key lost", r)
	}
	_ = c.Close()
	stop()
	probe(http.StatusServiceUnavailable)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore()

	// A new owner starts from an empty machine; the library restores the
	// snapshot and replays the committed tail from the store before serving.
	addr, health, stop = startReplica(ctx, t, store, load.Joining)
	probe(http.StatusOK)
	defer stop()
	c, err = resp.Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if r := do("DBSIZE"); r.Int != keyCount {
		t.Fatal("restored size", r)
	}
	if r := do("GET", fmt.Sprintf("key:%04d", keyCount-1)); r.Str != fmt.Sprint(keyCount-1) {
		t.Fatal("newest key not restored", r)
	}
	if r := do("GET", "key:0000"); r.Str != "0" {
		t.Fatal("oldest key not restored", r)
	}
}

func TestHealthRefusesReplayReadyReplicaWithoutQuorum(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, err := memstore.New(memstore.Options{NodeName: "node1", ClusterID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner, err := load.NewStandalone(load.StandaloneOptions{
		Identity:   storage.Identity{NodeName: "node1", ClusterID: "test", GroupID: groupID, ReplicaID: 1},
		InstanceID: "no-quorum", Mode: load.Founding, Store: store,
		StateMachine: cache.NewMachine(), Transport: singleReplica{},
		// Only this participant runs; the two other declared voters are absent.
		Peers:  []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := owner.Group().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	handler := healthHandler(func() (*load.Group, error) { return owner.Group(), nil })
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("node without a mutation quorum reported healthy: %d", recorder.Code)
	}
}
