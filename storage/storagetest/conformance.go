// Package storagetest provides reusable durability contract checks for Store adapters.
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

type Opener func(*testing.T, string) storage.Store

// Run checks round trips, snapshot tails, monotonicity, overwrite, isolation,
// idempotence, non-mutating probes and exact identity binding. Each opener must
// use an independent root for the supplied test and node.
func Run(t *testing.T, open Opener) {
	t.Helper()
	ctx := context.Background()
	prepare := func(t *testing.T, node string) storage.Store {
		t.Helper()
		s := open(t, node)
		must(t, s.BindReplica(ctx, storage.Identity{NodeName: node, ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}))
		return s
	}
	t.Run("StoreWideEvidence", func(t *testing.T) {
		s := open(t, "a")
		retained, err := s.HasState(ctx)
		must(t, err)
		if retained {
			t.Fatal("fresh store reports retained state")
		}
		must(t, s.BindReplica(ctx, storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}))
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
		write := storage.ReadyWrite{Entries: Entries(1, 3, 2), HardState: raftpb.HardState{Term: 2, Vote: 7, Commit: 3}}
		must(t, s.SaveReady(ctx, "group", write))
		r := recover(t, s)
		if !reflect.DeepEqual(r.Entries, write.Entries) || r.HardState != write.HardState || !raft.IsEmptySnap(r.Snapshot) || r.AppliedHint != 0 {
			t.Fatalf("round trip: %+v", r)
		}
		fresh, err := s.Load(ctx, "unused")
		must(t, err)
		if !raft.IsEmptyHardState(fresh.HardState) || len(fresh.Entries) != 0 || !raft.IsEmptySnap(fresh.Snapshot) {
			t.Fatalf("group isolation: %+v", fresh)
		}
	})
	t.Run("TailAfterSnapshot", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 5, 1), HardState: raftpb.HardState{Term: 1, Commit: 5}}))
		ss := Snapshot(3, 1)
		must(t, s.SaveSnapshot(ctx, "group", ss))
		r := recover(t, s)
		if !reflect.DeepEqual(r.Snapshot, ss) || !reflect.DeepEqual(r.Entries, Entries(4, 5, 1)) {
			t.Fatalf("snapshot and full tail: %+v", r)
		}
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(5, 1)))
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(2, 1)))
		r = recover(t, s)
		if r.Snapshot.Metadata.Index != 5 || len(r.Entries) != 0 {
			t.Fatalf("stale snapshot won: %+v", r)
		}
	})
	t.Run("HardStateMonotonicity", func(t *testing.T) {
		s := prepare(t, "a")
		hs := raftpb.HardState{Term: 5, Vote: 7, Commit: 10}
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 12, 5), HardState: hs}))
		for _, stale := range []raftpb.HardState{{Term: 3, Vote: 9, Commit: 4}, {Term: 5, Vote: 9, Commit: 10}, {Term: 5, Vote: 7, Commit: 9}} {
			if err := s.SaveReady(ctx, "group", storage.ReadyWrite{HardState: stale}); !errors.Is(err, fault.ErrInvalid) {
				t.Fatalf("unsafe state accepted: %+v: %v", stale, err)
			}
		}
		if r := recover(t, s); r.HardState != hs {
			t.Fatalf("failed write changed hard state: %+v", r.HardState)
		}
		hs = raftpb.HardState{Term: 7, Vote: 8, Commit: 12}
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{HardState: hs}))
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{}))
		if r := recover(t, s); r.HardState != hs {
			t.Fatalf("empty write changed hard state: %+v", r.HardState)
		}
	})
	t.Run("AppliedNeverRegresses", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 10, 1), HardState: raftpb.HardState{Term: 1, Commit: 10}}))
		cs := Snapshot(1, 1).Metadata.ConfState
		for _, n := range []uint64{7, 4, 7, 0} {
			must(t, s.SaveApplied(ctx, "group", n, cs))
		}
		if r := recover(t, s); r.AppliedHint != 7 || !reflect.DeepEqual(r.AppliedConfState, cs) || len(r.Entries) != 10 {
			t.Fatalf("applied is advisory and paired with membership: %+v", r)
		}
		if err := s.SaveApplied(ctx, "group", 11, cs); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("applied beyond commit: %v", err)
		}
	})
	t.Run("CompactionFloor", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 10, 1), HardState: raftpb.HardState{Term: 1, Commit: 10}}))
		if err := s.Compact(ctx, "group", 5); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("compaction without full snapshot: %v", err)
		}
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(5, 1)))
		must(t, s.Compact(ctx, "group", 5))
		if r := recover(t, s); !reflect.DeepEqual(r.Entries, Entries(6, 10, 1)) {
			t.Fatalf("tail after compaction: %+v", r.Entries)
		}
	})
	t.Run("OwnTailOverwrite", func(t *testing.T) {
		s := prepare(t, "a")
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 6, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}))
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(4, 6, 3), HardState: raftpb.HardState{Term: 3, Commit: 3}}))
		must(t, s.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		short := Entries(5, 5, 4)
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: short, HardState: raftpb.HardState{Term: 4, Commit: 3}}))
		want := append(Entries(4, 4, 3), short...)
		if r := recover(t, s); !reflect.DeepEqual(r.Entries, want) {
			t.Fatalf("shortened tail retained old suffix: %+v", r.Entries)
		}
	})
	t.Run("IdempotentResave", func(t *testing.T) {
		s := prepare(t, "a")
		write := storage.ReadyWrite{Entries: Entries(1, 3, 4), HardState: raftpb.HardState{Term: 4, Vote: 7, Commit: 3}}
		for range 3 {
			must(t, s.SaveReady(ctx, "group", write))
		}
		if r := recover(t, s); !reflect.DeepEqual(r.Entries, write.Entries) {
			t.Fatalf("duplicate entries: %+v", r.Entries)
		}
		for range 3 {
			must(t, s.SaveSnapshot(ctx, "group", Snapshot(3, 4)))
		}
		if r := recover(t, s); r.Snapshot.Metadata.Index != 3 || len(r.Entries) != 0 {
			t.Fatalf("duplicate snapshots: %+v", r)
		}
	})
	t.Run("TwoNodeIsolation", func(t *testing.T) {
		a, b := prepare(t, "a"), prepare(t, "b")
		must(t, a.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 3}}))
		must(t, b.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 2, 9), HardState: raftpb.HardState{Term: 9, Vote: 8, Commit: 2}}))
		must(t, a.SaveApplied(ctx, "group", 3, raftpb.ConfState{Voters: []uint64{7}}))
		must(t, b.SaveApplied(ctx, "group", 2, raftpb.ConfState{Voters: []uint64{8}}))
		before := recover(t, b)
		must(t, a.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		must(t, a.Compact(ctx, "group", 3))
		must(t, a.SaveSnapshot(ctx, "group", Snapshot(3, 1)))
		if after := recover(t, b); !reflect.DeepEqual(after, before) {
			t.Fatalf("peer altered by compaction: %+v", after)
		}
		_, err := a.Quarantine(ctx, "group", 7)
		must(t, err)
		if after := recover(t, b); !reflect.DeepEqual(after, before) {
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
		must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}))
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
		if r := recover(t, s); len(r.Entries) != 0 || !raft.IsEmptyHardState(r.HardState) {
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

func Entries(from, to, term uint64) []raftpb.Entry {
	var result []raftpb.Entry
	for n := from; n <= to; n++ {
		result = append(result, raftpb.Entry{Index: n, Term: term, Data: []byte{byte(n), byte(term)}})
	}
	return result
}
func Snapshot(index, term uint64) raftpb.Snapshot {
	return raftpb.Snapshot{Data: []byte(`{"commands":["one","two"],"values":{"key":"value"}}`), Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: raftpb.ConfState{Voters: []uint64{7, 9, 11}, Learners: []uint64{13}}}}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
}
func recover(t *testing.T, s storage.Store) storage.Recovery {
	t.Helper()
	r, err := s.Load(context.Background(), "group")
	must(t, err)
	return r
}
