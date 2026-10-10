package metastore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

func TestMemberSeenPersistsAppliedMembershipAcrossRemovalAndRestart(t *testing.T) {
	t.Parallel()
	for _, cs := range []raftpb.ConfState{{Voters: []uint64{2}}, {Learners: []uint64{2}}, {VotersOutgoing: []uint64{2}}, {LearnersNext: []uint64{2}}} {
		path := filepath.Join(t.TempDir(), "meta.bolt")
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.EnsureIdentity("n2", "cluster", 2); err != nil {
			t.Fatal(err)
		}
		if seen, err := s.MemberSeen(); err != nil || seen {
			t.Fatal(seen, err)
		}
		if err = s.SaveReady([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, raftpb.HardState{Term: 1, Commit: 2}); err != nil {
			t.Fatal(err)
		}
		if err = s.SaveApplication(1, cs, []byte("member")); err != nil {
			t.Fatal(err)
		}
		if err = s.SaveApplication(2, raftpb.ConfState{Voters: []uint64{1}}, []byte("removed")); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if seen, err := s.MemberSeen(); err != nil || !seen {
			t.Fatal("lost membership history", seen, err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemberSeenSnapshotAndApplicationRollback(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.EnsureIdentity("n2", "cluster", 2); err != nil {
		t.Fatal(err)
	}
	cs := raftpb.ConfState{Voters: []uint64{1, 2}}
	if err := s.SaveApplication(1, cs, []byte("uncommitted")); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || seen {
		t.Fatal("uncommitted membership recorded", seen, err)
	}
	if err := s.SaveReady(nil, raftpb.HardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	snap := raftpb.Snapshot{Data: []byte("snapshot"), Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1, ConfState: cs}}
	if err := s.InstallSnapshot(snap, raftpb.HardState{Term: 1, Commit: 4}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || seen {
		t.Fatal("rolled back snapshot recorded membership", seen, err)
	}
	if err := s.InstallSnapshot(snap, raftpb.HardState{Term: 2, Commit: 4}); err != nil {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || !seen {
		t.Fatal("installed membership not recorded", seen, err)
	}
	if err := s.InstallSnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}, raftpb.HardState{Term: 2, Commit: 5}); err != nil {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || !seen {
		t.Fatal("snapshot erased membership history", seen, err)
	}
}

func TestMemberSeenDoesNotClassifyPreMembershipJoiner(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.EnsureIdentity("n2", "cluster", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.SetApplied(1, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || seen {
		t.Fatal(seen, err)
	}
	if err := s.SetApplied(2, raftpb.ConfState{Learners: []uint64{2}}); err != nil {
		t.Fatal(err)
	}
	if seen, err := s.MemberSeen(); err != nil || !seen {
		t.Fatal(seen, err)
	}
}

func TestMemberSeenRefusesCorruptMarker(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	for _, data := range [][]byte{{}, {0}, {1, 1}} {
		if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketIdentity).Put(keyMemberSeen, data) }); err != nil {
			t.Fatal(err)
		}
		if _, err := s.MemberSeen(); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal(err)
		}
	}
}
