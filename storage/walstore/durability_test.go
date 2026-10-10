package walstore

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// Read through a separate WAL reader before closing or probing the live store:
// both those operations flush its encoder and can conceal a crash window.
func TestCommitOnlyReadyIsDurableBeforeAppliedMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7}}))
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 3}}))
	check(t, s.SaveApplied(ctx, "group", 3, raftpb.ConfState{Voters: []uint64{7}}))
	_, hs, entries, err := readLog(filepath.Join(root, "group", WALDir), walpb.Snapshot{})
	check(t, err)
	if hs.Commit != 3 || len(entries) != 3 {
		t.Fatalf("acknowledged apply outran durable WAL: hs=%+v entries=%d", hs, len(entries))
	}
}

func TestSnapshotCommitIsDurableBeforePublishingBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7}}))
	check(t, s.SaveSnapshot(ctx, "group", storagetest.Snapshot(3, 1)))
	_, hs, _, err := readLog(filepath.Join(root, "group", WALDir), walpb.Snapshot{Index: 3, Term: 1})
	check(t, err)
	if hs.Commit < 3 {
		t.Fatalf("published snapshot outran durable commit: %+v", hs)
	}
}
