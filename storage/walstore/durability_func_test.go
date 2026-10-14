//go:build functional

package walstore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// Skipping fsync must still flush the userspace WAL encoder before acknowledging
// writes, including commit-only Ready batches, and must work after reopening.
func TestWALSyncModesFlushAndRecover(t *testing.T) {
	t.Parallel()
	for _, disableFSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_fsync=%t", disableFSync), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			root := t.TempDir()
			id := storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}
			prewrite := false
			open := func() *Store {
				s, err := Open(Options{RootDir: root, NodeName: id.NodeName, ClusterID: id.ClusterID, SegmentBytes: testSegmentBytes, Prewrite: &prewrite, DisableFSync: disableFSync})
				check(t, err)
				t.Cleanup(func() { check(t, s.Close()) })
				check(t, s.BindReplica(ctx, id))
				check(t, s.Ensure(ctx, id.GroupID))
				if s.groups[id.GroupID].state.NoSync != disableFSync {
					t.Fatal("applied-state sync mode does not match WAL option")
				}
				return s
			}
			s := open()
			check(t, s.SaveReady(ctx, id.GroupID, storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7}}))
			check(t, s.SaveReady(ctx, id.GroupID, storage.ReadyWrite{HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 3}}))
			check(t, s.SaveApplied(ctx, id.GroupID, 3, raftpb.ConfState{Voters: []uint64{7}}))
			replay, err := readLog(filepath.Join(root, id.GroupID, WALDir), walpb.Snapshot{})
			hs, entries := replay.hardState, replay.entries
			check(t, err)
			if hs.Commit != 3 || len(entries) != 3 {
				t.Fatal("acknowledged WAL remains buffered", hs, entries)
			}
			check(t, s.SaveSnapshot(ctx, id.GroupID, storagetest.Snapshot(3, 1)))
			check(t, s.Close())
			s = open()
			check(t, s.SaveReady(ctx, id.GroupID, storage.ReadyWrite{Entries: storagetest.Entries(4, 4, 1), HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 4}}))
			replay, err = readLog(filepath.Join(root, id.GroupID, WALDir), walpb.Snapshot{Index: 3, Term: 1})
			hs, entries = replay.hardState, replay.entries
			check(t, err)
			if hs.Commit != 4 || len(entries) != 1 {
				t.Fatal("reopened WAL did not flush the tail", hs, entries)
			}
			check(t, s.Close())
			s = open()
			recovered, err := s.Load(ctx, id.GroupID)
			check(t, err)
			if recovered.Snapshot.Metadata.Index != 3 || recovered.HardState.Commit != 4 || len(recovered.Entries) != 1 || recovered.AppliedHint != 3 {
				t.Fatal("snapshot and tail were not recovered", recovered)
			}
		})
	}
}

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
	replay, err := readLog(filepath.Join(root, "group", WALDir), walpb.Snapshot{})
	hs, entries := replay.hardState, replay.entries
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
	replay, err := readLog(filepath.Join(root, "group", WALDir), walpb.Snapshot{Index: 3, Term: 1})
	hs := replay.hardState
	check(t, err)
	if hs.Commit < 3 {
		t.Fatalf("published snapshot outran durable commit: %+v", hs)
	}
}

func TestReadyRetryDurableWhenUpstreamHardStateAlreadyAdvanced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7}}))
	// Model the uncertain acknowledgement boundary: etcd has advanced its own
	// HardState while the adapter still retains its previous accepted state.
	// The next Save may skip flushing even though MustSync against that adapter
	// state says the term changed. No public close/probe may conceal the result.
	s.mu.Lock()
	g := s.groups["group"]
	s.mu.Unlock()
	check(t, g.w.Save(raftpb.HardState{Term: 2}, nil))
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{HardState: raftpb.HardState{Term: 2, Commit: 3}}))
	check(t, s.SaveApplied(ctx, "group", 3, raftpb.ConfState{Voters: []uint64{7}}))
	replay, err := readLog(filepath.Join(root, "group", WALDir), walpb.Snapshot{})
	hs := replay.hardState
	check(t, err)
	if hs.Term != 2 || hs.Commit != 3 {
		t.Fatalf("retried Ready was acknowledged before durable WAL: %+v", hs)
	}
}
