//go:build functional

package load_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
)

func TestBaselinePhysicalWALSnapshotIsRejected(t *testing.T) {
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
	if err == nil {
		owner.Stop(context.Background())
		t.Fatal("legacy raw application snapshot accepted")
	}
	if !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	if m.count() != 0 {
		t.Fatal("rejected snapshot reached application machine")
	}
	require(t, store.Close())
	after, err := os.ReadFile(identityPath)
	require(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("recovery rewrote durable identity pin")
	}
}
