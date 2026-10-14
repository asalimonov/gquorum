// Package storagetest provides reusable durability contract checks for Store
// adapters.
package storagetest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Opener creates independently owned storage for a test and logical node name.
type Opener func(*testing.T, string) storage.Store

//revive:disable:cognitive-complexity Keep the conformance cases together for test adapters.
//revive:disable:cyclomatic The explicit test cases do not add production branches.

// Run checks round trips, snapshot tails, monotonicity, overwrite, isolation,
// idempotence, non-mutating probes and exact identity binding. Each opener must
// use an independent root for the supplied test and node.
func Run(t *testing.T, open Opener) {
	t.Helper()
	ctx := context.Background()
	prepare := func(t *testing.T, node string) storage.Store {
		t.Helper()
		s := open(t, node)
		must(t, s.BindReplica(ctx, storage.Identity{NodeName: node, ClusterID: "test-cluster", GroupID: "group",
			ReplicaID: 7}))
		return s
	}
	t.Run("StoreWideEvidence", func(t *testing.T) {
		s := open(t, "a")
		retained, err := s.HasState(ctx)
		must(t, err)
		if retained {
			t.Fatal("fresh store reports retained state")
		}
		must(t, s.BindReplica(ctx, storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group",
			ReplicaID: 7}))
		must(t, s.Ensure(ctx, "group"))
		retained, err = s.HasState(ctx)
		must(t, err)
		if !retained {
			t.Fatal("durable identity omitted from inventory")
		}
		_, err = s.Quarantine(ctx, "group", 7)
		must(t, err)
		retained, err = s.HasState(ctx)
		must(t, err)
		if !retained {
			t.Fatal("quarantined evidence mistaken for fresh storage")
		}
	})
	t.Run("SaveLoadRoundTrip", func(t *testing.T) {
		s := prepare(t, "a")
		write := storage.ReadyWrite{
			Entries: Entries(1, 3, 2), HardState: raftpb.HardState{Term: 2, Vote: 7, Commit: 3}}
		must(t, s.SaveReady(ctx, "group", write))
		r := loadRecovery(t, s)
		if r.HardState != write.HardState || r.AppliedHint != 0 ||
			!reflect.DeepEqual(r.Entries, write.Entries) || !raft.IsEmptySnap(r.Snapshot) {
			t.Fatalf("round trip: %+v", r)
		}
		fresh, err := s.Load(ctx, "unused")
		must(t, err)
		if len(fresh.Entries) != 0 || !raft.IsEmptyHardState(fresh.HardState) || !raft.IsEmptySnap(fresh.Snapshot) {
			t.Fatalf("group isolation: %+v", fresh)
		}
	})
	t.Run("TailAfterSnapshot", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 5, 1), HardState: raftpb.HardState{Term: 1, Commit: 5}}))
		ss := Snapshot(3, 1)
		must(t, s.SaveSnapshot(ctx, "group", ss))
		r := loadRecovery(t, s)
		if !reflect.DeepEqual(r.Snapshot, ss) || !reflect.DeepEqual(r.Entries, Entries(4, 5, 1)) {
			t.Fatalf("snapshot and full tail: %+v", r)
		}
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(5, 1)))
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(2, 1)))
		r = loadRecovery(t, s)
		if r.Snapshot.Metadata.Index != 5 || len(r.Entries) != 0 {
			t.Fatalf("stale snapshot won: %+v", r)
		}
	})
	t.Run("HardStateMonotonicity", func(t *testing.T) {
		s := prepare(t, "a")
		hs := raftpb.HardState{Term: 5, Vote: 7, Commit: 10}
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 12, 5), HardState: hs}))
		for _, stale := range []raftpb.HardState{{Term: 3, Vote: 9, Commit: 4},
			{Term: 5, Vote: 9, Commit: 10}, {Term: 5, Vote: 7, Commit: 9}} {
			staleWrite := storage.ReadyWrite{HardState: stale}
			if err := s.SaveReady(ctx, "group", staleWrite); !errors.Is(err, fault.ErrInvalid) {
				t.Fatalf("unsafe state accepted: %+v: %v", stale, err)
			}
		}
		if r := loadRecovery(t, s); r.HardState != hs {
			t.Fatalf("failed write changed hard state: %+v", r.HardState)
		}
		hs = raftpb.HardState{Term: 7, Vote: 8, Commit: 12}
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{HardState: hs}))
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{}))
		if r := loadRecovery(t, s); r.HardState != hs {
			t.Fatalf("empty write changed hard state: %+v", r.HardState)
		}
	})
	t.Run("AppliedNeverRegresses", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 10, 1), HardState: raftpb.HardState{Term: 1, Commit: 10}}))
		cs := Snapshot(1, 1).Metadata.ConfState
		for _, n := range []uint64{7, 4, 7, 0} {
			must(t, s.SaveApplied(ctx, "group", n, cs))
		}
		if r := loadRecovery(t, s); r.AppliedHint != 7 || len(r.Entries) != 10 ||
			!reflect.DeepEqual(r.AppliedConfState, cs) {
			t.Fatalf("applied is advisory and paired with membership: %+v", r)
		}
		if err := s.SaveApplied(ctx, "group", 11, cs); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("applied beyond commit: %v", err)
		}
	})
	t.Run("CompactionFloor", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 10, 1), HardState: raftpb.HardState{Term: 1, Commit: 10}}))
		if err := s.Compact(ctx, "group", 5); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("compaction without full snapshot: %v", err)
		}
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(5, 1)))
		must(t, s.Compact(ctx, "group", 5))
		if r := loadRecovery(t, s); !reflect.DeepEqual(r.Entries, Entries(6, 10, 1)) {
			t.Fatalf("tail after compaction: %+v", r.Entries)
		}
	})
	t.Run("OwnTailOverwrite", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 6, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}))
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(4, 6, 3), HardState: raftpb.HardState{Term: 3, Commit: 3}}))
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		short := Entries(5, 5, 4)
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: short, HardState: raftpb.HardState{Term: 4,
			Commit: 3}}))
		want := append(Entries(4, 4, 3), short...)
		if r := loadRecovery(t, s); !reflect.DeepEqual(r.Entries, want) {
			t.Fatalf("shortened tail retained old suffix: %+v", r.Entries)
		}
	})
	t.Run("IdempotentResave", func(t *testing.T) {
		s := prepare(t, "a")
		write := storage.ReadyWrite{
			Entries: Entries(1, 3, 4), HardState: raftpb.HardState{Term: 4, Vote: 7, Commit: 3}}
		for range 3 {
			must(t, s.SaveReady(ctx, "group", write))
		}
		if r := loadRecovery(t, s); !reflect.DeepEqual(r.Entries, write.Entries) {
			t.Fatalf("duplicate entries: %+v", r.Entries)
		}
		for range 3 {
			must(t, s.SaveSnapshot(ctx, "group", Snapshot(3, 4)))
		}
		if r := loadRecovery(t, s); r.Snapshot.Metadata.Index != 3 || len(r.Entries) != 0 {
			t.Fatalf("duplicate snapshots: %+v", r)
		}
	})
	t.Run("TwoNodeIsolation", func(t *testing.T) {
		a, b := prepare(t, "a"), prepare(t, "b")
		must(t, a.SaveReady(ctx, "group",
			storage.ReadyWrite{
				Entries: Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 3}}))
		must(t, b.SaveReady(ctx, "group",
			storage.ReadyWrite{
				Entries: Entries(1, 2, 9), HardState: raftpb.HardState{Term: 9, Vote: 8, Commit: 2}}))
		must(t, a.SaveApplied(ctx, "group", 3, raftpb.ConfState{Voters: []uint64{7}}))
		must(t, b.SaveApplied(ctx, "group", 2, raftpb.ConfState{Voters: []uint64{8}}))
		before := loadRecovery(t, b)
		must(t, a.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		must(t, a.Compact(ctx, "group", 3))
		must(t, a.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		if after := loadRecovery(t, b); !reflect.DeepEqual(after, before) {
			t.Fatalf("peer altered by compaction: %+v", after)
		}
		_, err := a.Quarantine(ctx, "group", 7)
		must(t, err)
		if after := loadRecovery(t, b); !reflect.DeepEqual(after, before) {
			t.Fatalf("peer altered by quarantine: %+v", after)
		}
	})
	t.Run("ProbeEnsureDrop", func(t *testing.T) {
		s := prepare(t, "a")
		p, err := s.Probe(ctx, "group")
		must(t, err)
		if p.Exists || p.HasLog {
			t.Fatalf("probe created storage: %+v", p)
		}
		for range 2 {
			must(t, s.Ensure(ctx, "group"))
		}
		p, err = s.Probe(ctx, "group")
		must(t, err)
		if !p.Exists || p.HasLog || p.Identity.ReplicaID != 7 {
			t.Fatalf("fresh pinned store: %+v", p)
		}
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{
			Entries: Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}))
		p, err = s.Probe(ctx, "group")
		must(t, err)
		if !p.HasLog {
			t.Fatal("durable log not reported")
		}
		_, err = s.Quarantine(ctx, "group", 7)
		must(t, err)
		p, err = s.Probe(ctx, "group")
		must(t, err)
		if p.Exists || p.HasLog {
			t.Fatalf("quarantined store still live: %+v", p)
		}
		if r := loadRecovery(t, s); len(r.Entries) != 0 || !raft.IsEmptyHardState(r.HardState) {
			t.Fatalf("quarantined state loaded: %+v", r)
		}
	})
	t.Run("BindReplicaIdentity", func(t *testing.T) {
		s := prepare(t, "a")
		id := storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}
		must(t, s.BindReplica(ctx, id))
		must(t, s.Ensure(ctx, "group"))
		id.ReplicaID = 8
		if err := s.BindReplica(ctx, id); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatalf("incompatible binding: %v", err)
		}
		if p, err := s.Probe(ctx, "group"); err != nil || p.Identity.ReplicaID != 7 {
			t.Fatalf("bound identity: %+v, %v", p, err)
		}
	})
}

//revive:enable:cognitive-complexity
//revive:enable:cyclomatic

// Entries constructs an inclusive sequence of Raft entries at the supplied
// term.
func Entries(from, to, term uint64) []raftpb.Entry {
	var result []raftpb.Entry
	for n := from; n <= to; n++ {
		result = append(result, raftpb.Entry{Index: n, Term: term,
			Data: []byte{byte(n & 0xff), byte(term & 0xff)}})
	}
	return result
}

const (
	snapshotVoterA  = 7
	snapshotVoterB  = 9
	snapshotVoterC  = 11
	snapshotLearner = 13
)

// Snapshot constructs a complete test snapshot with fixed voter membership.
func Snapshot(index, term uint64) raftpb.Snapshot {
	return raftpb.Snapshot{
		Data: []byte(`{"commands":["one","two"],"values":{"key":"value"}}`),
		Metadata: raftpb.SnapshotMetadata{
			Index: index,
			Term:  term,
			ConfState: raftpb.ConfState{Voters: []uint64{snapshotVoterA, snapshotVoterB, snapshotVoterC},
				Learners: []uint64{snapshotLearner}},
		},
	}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
}
func loadRecovery(t *testing.T, s storage.Store) storage.Recovery {
	t.Helper()
	r, err := s.Load(context.Background(), "group")
	must(t, err)
	return r
}
