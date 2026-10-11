//go:build functional

package metastore

import (
	"errors"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
	"os"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "meta.bolt"))
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ent(index, term uint64, data string) raftpb.Entry {
	return raftpb.Entry{Index: index, Term: term, Type: raftpb.EntryNormal, Data: []byte(data)}
}

func (s *Store) setAppliedIndexOnly(idx uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketApplied).Put(keyApplied, itob(idx))
	})
}

func TestStoreIsFresh(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "meta.bolt")

	if fresh, err := StoreIsFresh(dir); err != nil || !fresh {
		t.Fatalf("StoreIsFresh(empty dir) = (%v, %v), want (true, nil)", fresh, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("StoreIsFresh created the meta store (stat err = %v); the probe must be side-effect-free", err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if fresh, err := StoreIsFresh(dir); err != nil || !fresh {
		t.Fatalf("StoreIsFresh(empty store) = (%v, %v), want (true, nil)", fresh, err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := s.EnsureIdentity("node1", "cid-1", 1); err != nil {
		t.Fatalf("EnsureIdentity: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if fresh, err := StoreIsFresh(dir); err != nil || fresh {
		t.Fatalf("StoreIsFresh(identity-pinned) = (%v, %v), want (false, nil)", fresh, err)
	}

	dir2 := filepath.Join(t.TempDir(), "meta.bolt")
	s2, err := Open(dir2)
	if err != nil {
		t.Fatalf("openBoltStore dir2: %v", err)
	}
	if err := s2.SaveReady(nil, raftpb.HardState{Term: 3, Vote: 1, Commit: 0}); err != nil {
		t.Fatalf("SaveReady: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close dir2: %v", err)
	}
	if fresh, err := StoreIsFresh(dir2); err != nil || fresh {
		t.Fatalf("StoreIsFresh(hardstate-only) = (%v, %v), want (false, nil)", fresh, err)
	}
}

func TestBoltStore_Roundtrip(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "meta.bolt")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}

	entries := []raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "c")}
	hs := raftpb.HardState{Term: 2, Vote: 7, Commit: 3}
	if err := s.SaveReady(entries, hs); err != nil {
		t.Fatalf("SaveReady: %v", err)
	}

	snap := raftpb.Snapshot{
		Data: []byte("sm-state"),
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{10, 20, 30}},
		},
	}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.SetApplied(3, raftpb.ConfState{Voters: []uint64{10, 20, 30}}); err != nil {
		t.Fatalf("SetApplied: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	gotHS, gotEntries, gotSnap, err := s2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if gotHS.Term != 2 || gotHS.Vote != 7 || gotHS.Commit != 3 {
		t.Fatalf("hardstate roundtrip: got %+v", gotHS)
	}
	if len(gotEntries) != 0 {
		t.Fatalf("expected entries <= snapshot index filtered out, got %d", len(gotEntries))
	}
	if gotSnap.Metadata.Index != 3 || string(gotSnap.Data) != "sm-state" {
		t.Fatalf("snapshot roundtrip: got %+v", gotSnap.Metadata)
	}
	if len(gotSnap.Metadata.ConfState.Voters) != 3 {
		t.Fatalf("confstate not preserved in snapshot: %+v", gotSnap.Metadata.ConfState)
	}
	if applied, err := s2.AppliedIndex(); err != nil || applied != 3 {
		t.Fatalf("AppliedIndex: got %d err %v", applied, err)
	}
}

func TestBoltStore_LoadAllWithoutSnapshot(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	entries := []raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}
	if err := s.SaveReady(entries, raftpb.HardState{Term: 1, Commit: 3}); err != nil {
		t.Fatalf("SaveReady: %v", err)
	}
	_, got, snap, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if snap.Metadata.Index != 0 {
		t.Fatalf("expected empty snapshot, got index %d", snap.Metadata.Index)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry %d out of order: index %d", i, e.Index)
		}
	}
}

func TestBoltStore_EmptyHardStateNotClobbered(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.SaveReady([]raftpb.Entry{ent(1, 1, "a")}, raftpb.HardState{Term: 5, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveReady 1: %v", err)
	}
	if err := s.SaveReady([]raftpb.Entry{ent(2, 1, "b")}, raftpb.HardState{}); err != nil {
		t.Fatalf("SaveReady 2: %v", err)
	}
	hs, _, _, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if hs.Term != 5 || hs.Vote != 2 || hs.Commit != 1 {
		t.Fatalf("empty hardstate clobbered prior state: %+v", hs)
	}
}

func TestBoltStore_LeaderOverwriteTruncatesTail(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.SaveReady([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}, raftpb.HardState{Term: 1, Commit: 1}); err != nil {
		t.Fatalf("SaveReady 1: %v", err)
	}
	if err := s.SaveReady([]raftpb.Entry{ent(2, 2, "b2")}, raftpb.HardState{Term: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveReady 2: %v", err)
	}
	_, got, _, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries after truncating tail, got %d: %+v", len(got), got)
	}
	if got[1].Index != 2 || got[1].Term != 2 || string(got[1].Data) != "b2" {
		t.Fatalf("overwrite did not replace index 2: %+v", got[1])
	}
}

func TestBoltStore_Compaction(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	var entries []raftpb.Entry
	for i := uint64(1); i <= 10; i++ {
		entries = append(entries, ent(i, 1, "x"))
	}
	if err := s.SaveReady(entries, raftpb.HardState{Term: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveReady: %v", err)
	}
	snap := raftpb.Snapshot{Data: []byte("s"), Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1}}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.CompactEntries(5); err != nil {
		t.Fatalf("CompactEntries: %v", err)
	}
	_, got, _, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected entries 6..10 after compaction, got %d", len(got))
	}
	if got[0].Index != 6 || got[4].Index != 10 {
		t.Fatalf("compaction boundary wrong: first %d last %d", got[0].Index, got[4].Index)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketEntries).Get(itob(5)); v != nil {
			t.Fatalf("index 5 should have been compacted")
		}
		if v := tx.Bucket(bucketEntries).Get(itob(6)); v == nil {
			t.Fatalf("index 6 should survive compaction")
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestBoltStore_StaleSnapshotIgnored(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	newer := raftpb.Snapshot{Data: []byte("new"), Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 2}}
	older := raftpb.Snapshot{Data: []byte("old"), Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1}}
	if err := s.SaveSnapshot(newer); err != nil {
		t.Fatalf("SaveSnapshot newer: %v", err)
	}
	if err := s.SaveSnapshot(older); err != nil {
		t.Fatalf("SaveSnapshot older: %v", err)
	}
	got, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got.Metadata.Index != 10 || string(got.Data) != "new" {
		t.Fatalf("stale snapshot overwrote newer one: %+v", got.Metadata)
	}
}

func TestBoltStore_AppliedMonotone(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.SetApplied(7, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatalf("SetApplied 7: %v", err)
	}
	if err := s.SetApplied(3, raftpb.ConfState{Voters: []uint64{1}}); err != nil { // out-of-order/retry — must be ignored
		t.Fatalf("SetApplied 3: %v", err)
	}
	got, err := s.AppliedIndex()
	if err != nil {
		t.Fatalf("AppliedIndex: %v", err)
	}
	if got != 7 {
		t.Fatalf("applied index rewound: got %d, want 7", got)
	}
}

func TestBoltStore_SetAppliedPersistsConfState(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)

	if _, found, err := s.AppliedConfState(); err != nil || found {
		t.Fatalf("AppliedConfState on a fresh store = (found=%v, %v), want (false, nil)", found, err)
	}

	newCS := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := s.SetApplied(10, newCS); err != nil {
		t.Fatalf("SetApplied 10: %v", err)
	}
	got, found, err := s.AppliedConfState()
	if err != nil || !found {
		t.Fatalf("AppliedConfState = (found=%v, %v), want (true, nil)", found, err)
	}
	if len(got.Voters) != 3 {
		t.Fatalf("ConfState not durable with the applied index: got %+v, want voters {1,2,3}", got)
	}

	if err := s.SetApplied(5, raftpb.ConfState{Voters: []uint64{9}}); err != nil {
		t.Fatalf("SetApplied 5: %v", err)
	}
	got, _, err = s.AppliedConfState()
	if err != nil {
		t.Fatalf("AppliedConfState: %v", err)
	}
	if len(got.Voters) != 3 {
		t.Fatalf("stale SetApplied rewound the ConfState: got %+v, want voters {1,2,3}", got)
	}
}

func TestBoltStore_ReopenAfterPartialWrite(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "meta.bolt")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}

	if err := s.SaveReady([]raftpb.Entry{ent(1, 1, "committed")}, raftpb.HardState{Term: 1, Commit: 1}); err != nil {
		t.Fatalf("SaveReady durable: %v", err)
	}

	wantErr := errors.New("boom")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketEntries).Put(itob(2), []byte("torn")); err != nil {
			return err
		}
		if err := tx.Bucket(bucketHardState).Put(keyHardState, []byte("garbage")); err != nil {
			return err
		}
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected rollback error, got %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	hs, got, _, err := s2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != 1 || got[0].Index != 1 || string(got[0].Data) != "committed" {
		t.Fatalf("torn write leaked into durable state: %+v", got)
	}
	if hs.Term != 1 || hs.Commit != 1 {
		t.Fatalf("torn hardstate leaked: %+v", hs)
	}
}

func TestBoltStore_IdentityPinning(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "meta.bolt")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}

	if err := s.EnsureIdentity("node1", "cluster-A", 11); err != nil {
		t.Fatalf("EnsureIdentity first: %v", err)
	}
	if err := s.EnsureIdentity("node1", "cluster-A", 11); err != nil {
		t.Fatalf("EnsureIdentity repeat: %v", err)
	}

	err = s.EnsureIdentity("node2", "cluster-A", 11)
	var mm *IdentityMismatchError
	if !errors.As(err, &mm) {
		t.Fatalf("expected *IdentityMismatchError, got %v", err)
	}
	if mm.Stored.NodeName != "node1" || mm.Resolved.NodeName != "node2" {
		t.Fatalf("mismatch fields wrong: stored %+v resolved %+v", mm.Stored, mm.Resolved)
	}

	if err := s.EnsureIdentity("node1", "cluster-B", 11); !errors.As(err, &mm) {
		t.Fatalf("expected mismatch on cluster ID, got %v", err)
	}
	if err := s.EnsureIdentity("node1", "cluster-A", 12); !errors.As(err, &mm) {
		t.Fatalf("expected mismatch on member ID, got %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if err := s2.EnsureIdentity("node1", "cluster-A", 11); err != nil {
		t.Fatalf("identity pin lost across reopen: %v", err)
	}
	if err := s2.EnsureIdentity("node2", "cluster-A", 11); !errors.As(err, &mm) {
		t.Fatalf("mismatch not enforced after reopen: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close before identity probe: %v", err)
	}
	identity, found, err := LoadIdentity(dir)
	if err != nil || !found || identity != (Identity{NodeName: "node1", ClusterID: "cluster-A", MemberID: 11}) {
		t.Fatalf("LoadIdentity = (%+v, %t, %v)", identity, found, err)
	}
}

func TestInstallSnapshot_AtomicAndCommitClamped(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)

	if err := s.SaveReady([]raftpb.Entry{ent(8, 2, "a"), ent(9, 2, "b"), ent(10, 2, "c")},
		raftpb.HardState{Term: 2, Vote: 1, Commit: 4}); err != nil {
		t.Fatalf("seed SaveReady: %v", err)
	}

	snap := raftpb.Snapshot{
		Data: []byte("sm-bytes"),
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      3,
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
		},
	}
	if err := s.InstallSnapshot(snap, raftpb.HardState{Term: 3, Vote: 1, Commit: 4}); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	hs, entries, gotSnap, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if gotSnap.Metadata.Index != 10 {
		t.Fatalf("snapshot index: got %d, want 10", gotSnap.Metadata.Index)
	}
	if hs.Commit < gotSnap.Metadata.Index {
		t.Fatalf("hardstate commit %d < snapshot index %d — would panic loadState on boot",
			hs.Commit, gotSnap.Metadata.Index)
	}
	if hs.Term != 3 { // provided HardState's term/vote preserved, only Commit clamped
		t.Fatalf("hardstate term: got %d, want 3", hs.Term)
	}
	if len(entries) != 0 {
		t.Fatalf("log not compacted to snapshot index: %d entries remain", len(entries))
	}
	if st, err := s.LoadState(); err != nil || string(st) != "sm-bytes" {
		t.Fatalf("SM state not persisted atomically: %q err=%v", st, err)
	}
	if applied, err := s.AppliedIndex(); err != nil || applied != 10 {
		t.Fatalf("applied index: got %d err=%v, want 10", applied, err)
	}
}

func TestInstallSnapshot_IgnoresStale(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	newer := raftpb.Snapshot{
		Data:     []byte("v2"),
		Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 4, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
	}
	if err := s.InstallSnapshot(newer, raftpb.HardState{Term: 4, Commit: 20}); err != nil {
		t.Fatalf("InstallSnapshot newer: %v", err)
	}
	stale := raftpb.Snapshot{
		Data:     []byte("v1"),
		Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
	}
	if err := s.InstallSnapshot(stale, raftpb.HardState{Term: 2, Commit: 10}); err != nil {
		t.Fatalf("InstallSnapshot stale: %v", err)
	}
	got, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got.Metadata.Index != 20 {
		t.Fatalf("stale install rewound snapshot to %d, want 20", got.Metadata.Index)
	}
}

func TestPeekLocalLineage(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "meta.bolt")
	if v, m, err := PeekLocalLineage(dir); err != nil || v != 0 || m != 0 {
		t.Fatalf("PeekLocalLineage(absent) = (%d, %d, %v), want (0, 0, nil)", v, m, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("PeekLocalLineage created the meta store; the probe must be side-effect-free")
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}
	if err := s.EnsureIdentity("node1", "cid-1", 7); err != nil {
		t.Fatalf("EnsureIdentity: %v", err)
	}
	if err := s.SaveState([]byte(`{"latest":{"version":9},"target":{"version":9},"active":{"version":8}}`)); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	v, m, err := PeekLocalLineage(dir)
	if err != nil {
		t.Fatalf("PeekLocalLineage: %v", err)
	}
	if m != 7 {
		t.Fatalf("member ID = %d, want 7", m)
	}
	if v != 9 {
		t.Fatalf("doc version = %d, want 9 (highest cursor)", v)
	}
}
