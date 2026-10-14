//go:build functional

package metastore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

func TestOpenUsesExactCallerPathAndSchema(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "chosen", "topology.data")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal(info, err)
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil || parent.Mode().Perm() != 0o700 {
		t.Fatal(parent, err)
	}
	if s.path != path {
		t.Fatal("caller path rewritten")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		for _, name := range allBuckets {
			if tx.Bucket(name) == nil {
				t.Errorf("missing bucket %s", name)
			}
		}
		if tx.Bucket([]byte("events")) != nil {
			t.Error("event bucket inside meta")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketFormat).Put(keySchemaVersion, []byte("99")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("unsupported schema opened", err)
	}
	if _, err := Open(""); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("empty path opened", err)
	}
}

func TestOldMetadataFormatIsRejectedWithoutRewrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFormat).Put(keySchemaVersion, []byte("2"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path); !errors.Is(err, fault.ErrUnsupported) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("old format was not rejected: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("rejected format was changed: %v", err)
	}
}

func TestSaveApplicationAtomicDurableState(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	hs := raftpb.HardState{Term: 1, Vote: 1, Commit: 2}
	if err := s.SaveReady([]raftpb.Entry{ent(1, 1, "first"), ent(2, 1, "second")}, hs); err != nil {
		t.Fatal(err)
	}
	cs := raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if err := s.SaveApplication(2, cs, []byte(`{"latest":{"version":2}}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveApplication(3, cs, []byte("invalid advance")); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.AppliedIndex()
	if err != nil || a != 2 {
		t.Fatal(a, err)
	}
	actual, err := s.AppliedConfState()
	if err != nil || !actual.Found || !reflect.DeepEqual(cs, actual.ConfState) {
		t.Fatal(actual.ConfState, actual.Found, err)
	}
	state, err := s.LoadState()
	if err != nil || string(state) != `{"latest":{"version":2}}` {
		t.Fatal(string(state), err)
	}
}

func TestSnapshotCannotRewindMaterializedState(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.SaveReady([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, raftpb.HardState{Term: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	cs := raftpb.ConfState{Voters: []uint64{1, 2}}
	if err := s.SaveApplication(2, cs, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	old := raftpb.Snapshot{Data: []byte("older"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	if err := s.InstallSnapshot(old, raftpb.HardState{Term: 1, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	state, err := s.LoadState()
	if err != nil || string(state) != "newer" {
		t.Fatal(string(state), err)
	}
}

func TestHardStateAndCommittedTailNeverRegress(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	entries := []raftpb.Entry{ent(1, 2, "a"), ent(2, 2, "b")}
	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 2}
	if err := s.SaveReady(entries, hs); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []raftpb.HardState{{Term: 1, Commit: 2}, {Term: 2, Vote: 2, Commit: 2}, {Term: 2, Vote: 1, Commit: 1}, {Term: 2, Vote: 0, Commit: 2}} {
		if err := s.SaveReady(nil, bad); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid hard state accepted", bad, err)
		}
	}
	if err := s.SaveReady([]raftpb.Entry{ent(2, 3, "replacement")}, raftpb.HardState{Term: 3, Commit: 2}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("committed entry replaced", err)
	}
	actualHS, err := s.LoadAll()
	if err != nil || actualHS.HardState != hs || !reflect.DeepEqual(actualHS.Entries, entries) {
		t.Fatal("failed transaction mutated state", actualHS.HardState, actualHS.Entries, err)
	}
	if err := s.CompactEntries(2); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("compacted without covering snapshot", err)
	}
}

func TestReplicaLedgerExactIdentityAndReplacementProtection(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.EnsureIdentity("a", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	record := ReplicaRecord{NodeName: "a", ClusterID: "cluster", ReplicaID: 7, Origin: "fresh", CreatedAt: time.Unix(1, 0).UTC()}
	if err := s.RecordReplica("load", record); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordReplica("load", record); err != nil {
		t.Fatal("repeat not idempotent", err)
	}
	bad := record
	bad.ReplicaID = 8
	if err := s.RecordReplica("load", bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if err := s.ForgetReplica("load", 8); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	actual, err := s.LookupReplica("load")
	if err != nil || !actual.Found || actual.Record != record {
		t.Fatal(actual.Record, actual.Found, err)
	}
	if err := s.ForgetReplica("load", 7); err != nil {
		t.Fatal(err)
	}
	if found, err := s.LookupReplica("load"); err != nil || found.Found {
		t.Fatal(found.Found, err)
	}
	if err := s.RecordReplica("_meta", record); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	bad = record
	bad.NodeName = "other"
	if err := s.RecordReplica("load", bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
}
