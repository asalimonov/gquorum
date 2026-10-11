package load_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
)

func TestBaselinePhysicalWALSnapshotAndCommittedTailRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("functional retained Raft engine")
	}
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	root := t.TempDir()
	require(t, os.CopyFS(filepath.Join(root, "work"), os.DirFS(filepath.Join("testdata", "api-baseline", "work"))))
	identityPath := filepath.Join(root, "work", walstore.IdentityFile)
	before, err := os.ReadFile(identityPath)
	require(t, err)
	prewrite := false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := walstore.Open(walstore.Options{RootDir: root, NodeName: "n1", ClusterID: "compat-cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite, Logger: logger})
	require(t, err)
	defer store.Close()
	id := storage.Identity{NodeName: "n1", ClusterID: "compat-cluster", GroupID: "work", ReplicaID: 7}
	probe, err := store.Probe(ctx, "work")
	require(t, err)
	if !probe.Exists || !probe.HasLog || probe.Identity != id {
		t.Fatal("baseline WAL pin", probe)
	}
	m := &deduplicatingMachine{}
	owner, err := load.NewStandalone(load.StandaloneOptions{Identity: id, InstanceID: "new-process", Mode: load.Joining, Store: store, StateMachine: m, Transport: transport{}, Logger: logger, Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond}}})
	require(t, err)
	defer owner.Stop(context.Background())
	recovery, err := store.Load(ctx, "work")
	require(t, err)
	if recovery.Snapshot.Metadata.Index != 3 || recovery.HardState.Commit != 4 || !bytes.Equal(recovery.Snapshot.Data, []byte(`{"one":"first"}`)) {
		t.Fatal("fixture must include full snapshot and a committed tail", recovery)
	}
	require(t, owner.Start(ctx))
	require(t, owner.Group().WaitReady(ctx))
	if m.count() != 2 || owner.Group().Identity() != id || !slices.Equal(owner.Group().Status().ConfState.Voters, []uint64{7}) {
		t.Fatal("recovery changed identities or lost snapshot/tail", owner.Group().Status(), m.count())
	}
	waitLeader(t, ctx, owner.Group())
	for request, data := range map[string]string{"one": "first", "two": "tail"} {
		result, err := owner.Group().Propose(ctx, load.Proposal{RequestID: request, Data: []byte(data)})
		require(t, err)
		if result.Code != "ok" || string(result.Data) != data || m.count() != 2 {
			t.Fatal("dedup lost across baseline recovery", result)
		}
	}
	result, err := owner.Group().Propose(ctx, load.Proposal{RequestID: "two", Data: []byte("different")})
	require(t, err)
	if result.Code != "conflict" || m.count() != 2 {
		t.Fatal("recovered request identity overwritten", result)
	}
	require(t, owner.Stop(ctx))
	require(t, store.Close())
	after, err := os.ReadFile(identityPath)
	require(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("recovery rewrote durable identity pin")
	}
}
