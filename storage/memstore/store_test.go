//go:build !functional

package memstore

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	"go.etcd.io/raft/v3/raftpb"
)

func open(t *testing.T, node string) *Store {
	t.Helper()
	s, err := New(Options{NodeName: node, ClusterID: "test-cluster"})
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	return s
}

func bound(t *testing.T, replica uint64) *Store {
	t.Helper()
	s := open(t, "a")
	must(t, s.BindReplica(context.Background(), identity(replica)))
	return s
}

func identity(replica uint64) storage.Identity {
	return storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: replica}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func load(t *testing.T, s *Store) storage.Recovery {
	t.Helper()
	r, err := s.Load(context.Background(), "group")
	must(t, err)
	return r
}

func TestConformance(t *testing.T) {
	t.Parallel()
	storagetest.Run(t, func(t *testing.T, node string) storage.Store { return open(t, node) })
}

func TestNewRequiresOwner(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{NodeName: "a"}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestReceivedSnapshotReplacesDivergentTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := bound(t, 7)
	must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 5, 1), HardState: raftpb.HardState{Term: 1, Commit: 2}}))
	// The leader's snapshot at index 4 has term 2; the local entry 4 has term 1.
	must(t, s.SaveSnapshot(ctx, "group", storagetest.Snapshot(4, 2)))
	if r := load(t, s); len(r.Entries) != 0 || r.Snapshot.Metadata.Index != 4 || r.HardState.Commit != 4 || r.HardState.Term != 2 {
		t.Fatalf("divergent tail survived: %+v", r)
	}
	must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(5, 6, 2), HardState: raftpb.HardState{Term: 2, Commit: 6}}))
	if r := load(t, s); !reflect.DeepEqual(r.Entries, storagetest.Entries(5, 6, 2)) {
		t.Fatalf("log after snapshot: %+v", r.Entries)
	}
	// A matching local compaction snapshot keeps the continuing tail.
	must(t, s.SaveSnapshot(ctx, "group", storagetest.Snapshot(5, 2)))
	if r := load(t, s); !reflect.DeepEqual(r.Entries, storagetest.Entries(6, 6, 2)) {
		t.Fatalf("continuing tail dropped: %+v", r.Entries)
	}
}

func TestSnapshotsBoundRetainedEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := bound(t, 7)
	must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 100, 1), HardState: raftpb.HardState{Term: 1, Commit: 100}}))
	before, err := s.Stats(ctx, "group")
	must(t, err)
	must(t, s.SaveSnapshot(ctx, "group", storagetest.Snapshot(90, 1)))
	must(t, s.Compact(ctx, "group", 90))
	after, err := s.Stats(ctx, "group")
	must(t, err)
	if r := load(t, s); !reflect.DeepEqual(r.Entries, storagetest.Entries(91, 100, 1)) {
		t.Fatalf("retained entries: %d", len(r.Entries))
	}
	if after.SnapshotIndex != 90 || after.SnapshotBytes == 0 || after.LogBytes*5 > before.LogBytes {
		t.Fatalf("stats before %+v after %+v", before, after)
	}
	if err := s.Compact(ctx, "group", 91); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("compaction beyond the snapshot accepted", err)
	}
}

func TestQuarantineReleasesStateAndCollects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := bound(t, 7)
	must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}))
	if _, err := s.Quarantine(ctx, "group", 8); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("other replica quarantined", err)
	}
	token, err := s.Quarantine(ctx, "group", 7)
	must(t, err)
	retired, err := s.Retired(ctx)
	must(t, err)
	if len(retired) != 1 || retired[0].Token != token || retired[0].Identity != identity(7) || retired[0].QuarantinedAt.IsZero() {
		t.Fatal(retired)
	}
	if p, err := s.Probe(ctx, "group"); err != nil || p.Exists {
		t.Fatal("quarantined replica still live", p, err)
	}
	// A reseated successor binds a fresh identity in the same group.
	must(t, s.BindReplica(ctx, identity(9)))
	must(t, s.Ensure(ctx, "group"))
	if r := load(t, s); len(r.Entries) != 0 {
		t.Fatal("successor saw retired state", r)
	}
	stale := retired[0]
	stale.QuarantinedAt = stale.QuarantinedAt.Add(1)
	if err := s.Collect(ctx, stale); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("mismatched record collected", err)
	}
	must(t, s.Collect(ctx, retired[0]))
	must(t, s.Collect(ctx, retired[0]))
	if left, err := s.Retired(ctx); err != nil || len(left) != 0 {
		t.Fatal(left, err)
	}
	if p, err := s.Probe(ctx, "group"); err != nil || p.Identity != identity(9) {
		t.Fatal("collection touched the successor", p, err)
	}
}

func TestCollectRefusesALiveIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := bound(t, 7)
	must(t, s.Ensure(ctx, "group"))
	_, err := s.Quarantine(ctx, "group", 7)
	must(t, err)
	retired, err := s.Retired(ctx)
	must(t, err)
	must(t, s.BindReplica(ctx, identity(7)))
	if err := s.Collect(ctx, retired[0]); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("bound identity collected", err)
	}
}

func TestReturnedStateIsOwned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := bound(t, 7)
	entries := storagetest.Entries(1, 2, 1)
	must(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: entries, HardState: raftpb.HardState{Term: 1, Commit: 2}}))
	ss := storagetest.Snapshot(1, 1)
	must(t, s.SaveSnapshot(ctx, "group", ss))
	entries[1].Data[0] = 99
	ss.Data[0], ss.Metadata.ConfState.Voters[0] = 'x', 99
	got := load(t, s)
	got.Entries[0].Data[0], got.Snapshot.Data[0] = 98, 'y'
	again := load(t, s)
	if again.Entries[0].Data[0] != 2 || again.Snapshot.Data[0] != '{' || again.Snapshot.Metadata.ConfState.Voters[0] != 7 {
		t.Fatalf("store shares caller memory: %+v", again)
	}
}

func TestCloseStopsTheStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := New(Options{NodeName: "a", ClusterID: "test-cluster"})
	must(t, err)
	must(t, s.BindReplica(ctx, identity(7)))
	must(t, s.Close())
	must(t, s.Close())
	if _, err := s.Load(ctx, "group"); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := s.Ensure(ctx, "group"); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestRetirementConformance(t *testing.T) {
	t.Parallel()
	storagetest.RunRetirement(t, func(t *testing.T, node string) storage.Store { return open(t, node) })
}

func TestMemoryCollectionCannotProveDeletionAfterProcessLoss(t *testing.T) {
	t.Parallel()
	s := bound(t, 7)
	must(t, s.Ensure(t.Context(), "group"))
	_, err := s.Quarantine(t.Context(), "group", 7)
	must(t, err)
	rows, err := s.Retired(t.Context())
	must(t, err)
	must(t, s.Collect(t.Context(), rows[0]))
	must(t, s.Close())
	fresh := open(t, "a")
	if err := fresh.Collect(t.Context(), rows[0]); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("fresh volatile store fabricated collection proof", err)
	}
}
