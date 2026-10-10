package walstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	"go.etcd.io/raft/v3/raftpb"
)

const testSegmentBytes = 32 << 10

func openTest(t *testing.T, root, node, cluster string, prewrite bool) *Store {
	t.Helper()
	s, err := Open(Options{RootDir: root, NodeName: node, ClusterID: cluster, SegmentBytes: testSegmentBytes, Prewrite: &prewrite})
	check(t, err)
	t.Cleanup(func() { check(t, s.Close()) })
	return s
}
func boundTest(t *testing.T, root string, prewrite bool) *Store {
	t.Helper()
	s := openTest(t, root, "a", "test-cluster", prewrite)
	check(t, s.BindReplica(context.Background(), storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}))
	return s
}
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
}
func loadTest(t *testing.T, s *Store) storage.Recovery {
	t.Helper()
	r, err := s.Load(context.Background(), "group")
	check(t, err)
	return r
}
func seed(t *testing.T, s *Store, n uint64) {
	t.Helper()
	check(t, s.SaveReady(context.Background(), "group", storage.ReadyWrite{Entries: storagetest.Entries(1, n, 1), HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: n}}))
}
func segmentedSeed(t *testing.T, s *Store, batches int) uint64 {
	t.Helper()
	var index uint64
	for range batches {
		var entries []raftpb.Entry
		for range 20 {
			index++
			entries = append(entries, raftpb.Entry{Index: index, Term: 1, Data: make([]byte, 500)})
		}
		check(t, s.SaveReady(context.Background(), "group", storage.ReadyWrite{Entries: entries, HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: index}}))
	}
	return index
}

func TestWalStore_Conformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T, node string) storage.Store {
		return openTest(t, filepath.Join(t.TempDir(), node), node, "test-cluster", false)
	})
}

func TestWalStore_ReopenAcrossSegments(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	segmentedSeed(t, s, 100)
	before, err := segmentNames(filepath.Join(root, "group", WALDir))
	check(t, err)
	if len(before) < 5 {
		t.Fatalf("segment cuts: %d", len(before))
	}
	ss := storagetest.Snapshot(1500, 1)
	check(t, s.SaveSnapshot(ctx, "group", ss))
	check(t, s.Compact(ctx, "group", 1400))
	after, err := segmentNames(filepath.Join(root, "group", WALDir))
	check(t, err)
	if len(after) >= len(before) {
		t.Fatalf("compaction deleted no segments: %d -> %d", len(before), len(after))
	}
	check(t, s.Close())
	s = boundTest(t, root, false)
	r := loadTest(t, s)
	if !reflect.DeepEqual(r.Snapshot, ss) || len(r.Entries) != 500 || r.Entries[0].Index != 1501 || r.HardState.Commit != 2000 {
		t.Fatalf("recovery: snapshot=%+v entries=%d hs=%+v", r.Snapshot.Metadata, len(r.Entries), r.HardState)
	}
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(2001, 2001, 2), HardState: raftpb.HardState{Term: 2, Vote: 7, Commit: 2001}}))
	check(t, s.SaveApplied(ctx, "group", 2001, ss.Metadata.ConfState))
	if r = loadTest(t, s); len(r.Entries) != 501 || r.Entries[500].Index != 2001 || r.AppliedHint != 2001 {
		t.Fatalf("reopened write: %+v", r)
	}
}

func TestWalStore_BaseMarkerSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	// A replay base above zero must contain a complete machine state, not just a marker.
	if err := s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(10, 11, 2), HardState: raftpb.HardState{Term: 2, Commit: 11}}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("missing replay prefix accepted: %v", err)
	}
	ss := storagetest.Snapshot(9, 2)
	check(t, s.SaveSnapshot(ctx, "group", ss))
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(10, 11, 2), HardState: raftpb.HardState{Term: 2, Commit: 11}}))
	check(t, s.Close())
	s = boundTest(t, root, false)
	if r := loadTest(t, s); !reflect.DeepEqual(r.Snapshot, ss) || !reflect.DeepEqual(r.Entries, storagetest.Entries(10, 11, 2)) || r.HardState.Commit != 11 {
		t.Fatalf("base recovery: %+v", r)
	}
}

func TestWalStore_IdentityMismatch(t *testing.T) {
	for _, field := range []string{"node", "cluster", "group", "replica", "metadata"} {
		t.Run(field, func(t *testing.T) {
			root := t.TempDir()
			s := boundTest(t, root, false)
			seed(t, s, 2)
			check(t, s.Close())
			want := storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}
			node, cluster := "a", "test-cluster"
			switch field {
			case "node":
				node = "b"
				want.NodeName = node
			case "cluster":
				cluster = "other"
				want.ClusterID = cluster
			case "replica":
				want.ReplicaID = 8
			case "group":
				pin := want
				pin.GroupID = "other"
				check(t, writeIdentity(filepath.Join(root, "group"), pin))
			case "metadata":
				want.ReplicaID = 8
				check(t, writeIdentity(filepath.Join(root, "group"), want))
			}
			s = openTest(t, root, node, cluster, false)
			err := s.BindReplica(context.Background(), want)
			if field == "metadata" {
				check(t, err)
				_, err = s.Load(context.Background(), "group")
			}
			if !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatalf("%s mismatch: %v", field, err)
			}
			var typed *IdentityMismatchError
			if !errors.As(err, &typed) {
				t.Fatalf("identity details unavailable: %v", err)
			}
		})
	}
}

func TestWalStore_ProbeInputs(t *testing.T) {
	ctx := context.Background()
	t.Run("absent", func(t *testing.T) {
		root := t.TempDir()
		s := boundTest(t, root, false)
		p, err := s.Probe(ctx, "group")
		check(t, err)
		if p != (storage.Probe{}) {
			t.Fatalf("fresh probe: %+v", p)
		}
		if _, err := os.Stat(filepath.Join(root, "group")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("probe created files: %v", err)
		}
	})
	t.Run("identity only", func(t *testing.T) {
		s := boundTest(t, t.TempDir(), false)
		check(t, s.Ensure(ctx, "group"))
		p, err := s.Probe(ctx, "group")
		check(t, err)
		if !p.Exists || p.HasLog || p.Identity.ReplicaID != 7 {
			t.Fatalf("identity probe: %+v", p)
		}
		if err := s.SaveApplied(ctx, "group", 7, raftpb.ConfState{}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("orphan applied marker accepted: %v", err)
		}
	})
	for _, corrupt := range []string{"identity", "state", "segment", "missing WAL", "missing pin"} {
		t.Run(corrupt, func(t *testing.T) {
			root := t.TempDir()
			s := boundTest(t, root, false)
			seed(t, s, 3)
			check(t, s.Close())
			dir := filepath.Join(root, "group")
			switch corrupt {
			case "identity":
				check(t, os.WriteFile(filepath.Join(dir, IdentityFile), []byte("not json"), fileMode))
			case "state":
				check(t, os.WriteFile(filepath.Join(dir, StateFile), []byte("not a database"), fileMode))
			case "segment":
				names, err := segmentNames(filepath.Join(dir, WALDir))
				check(t, err)
				f, err := os.OpenFile(filepath.Join(dir, WALDir, names[0].name), os.O_WRONLY, fileMode)
				check(t, err)
				_, err = f.Write([]byte{255, 255, 255, 255, 255, 255, 255, 255})
				check(t, err)
				check(t, f.Close())
			case "missing WAL":
				check(t, os.RemoveAll(filepath.Join(dir, WALDir)))
			case "missing pin":
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
			}
			s = openTest(t, root, "a", "test-cluster", false)
			if _, err := s.Probe(ctx, "group"); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatalf("corrupt %s: %v", corrupt, err)
			}
		})
	}
	t.Run("unsupported schema", func(t *testing.T) {
		root := t.TempDir()
		s := boundTest(t, root, false)
		check(t, s.Ensure(ctx, "group"))
		check(t, s.Close())
		id := diskIdentity{SchemaVersion: 2, Identity: storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}}
		data, err := json.Marshal(id)
		check(t, err)
		check(t, os.WriteFile(filepath.Join(root, "group", IdentityFile), data, fileMode))
		s = openTest(t, root, "a", "test-cluster", false)
		if _, err := s.Probe(ctx, "group"); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatalf("unsupported schema: %v", err)
		}
	})
}

func TestWalStore_QuarantineSweep(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := boundTest(t, root, false)
	seed(t, s, 3)
	if _, err := s.Quarantine(ctx, "group", 8); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatalf("stale quarantine: %v", err)
	}
	if r := loadTest(t, s); r.HardState.Commit != 3 {
		t.Fatal("stale quarantine damaged live replica")
	}
	fresh, err := s.Quarantine(ctx, "group", 7)
	check(t, err)
	old := filepath.Join(root, "expired.quarantine-"+strconv.FormatInt(time.Now().Add(-8*24*time.Hour).UnixNano(), 10))
	check(t, os.Mkdir(old, dirMode))
	check(t, writeIdentity(old, storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "expired", ReplicaID: 2}))
	foreign := filepath.Join(root, "foreign.quarantine-1")
	check(t, os.Mkdir(foreign, dirMode))
	check(t, writeIdentity(foreign, storage.Identity{NodeName: "b", ClusterID: "test-cluster", GroupID: "foreign", ReplicaID: 2}))
	garbled := filepath.Join(root, "other.quarantine-invalid")
	check(t, os.Mkdir(garbled, dirMode))
	check(t, s.SweepQuarantine(ctx))
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired quarantine survived: %v", err)
	}
	for _, keep := range []string{fresh, foreign, garbled} {
		_, err := os.Stat(keep)
		check(t, err)
	}
	check(t, s.BindReplica(ctx, storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 8}))
	check(t, s.Ensure(ctx, "group"))
	if _, err := s.Quarantine(ctx, "group", 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatalf("old callback moved replacement: %v", err)
	}
}

func TestWalStore_RefillerRace(t *testing.T) {
	root := t.TempDir()
	s := boundTest(t, root, true)
	index := segmentedSeed(t, s, 120)
	check(t, s.SaveApplied(context.Background(), "group", index, raftpb.ConfState{Voters: []uint64{7}}))
	check(t, s.Close())
	segments, err := segmentNames(filepath.Join(root, "group", WALDir))
	check(t, err)
	if len(segments) < 10 {
		t.Fatalf("too few cuts: %d", len(segments))
	}
	for _, segment := range segments {
		info, err := os.Stat(filepath.Join(root, "group", WALDir, segment.name))
		check(t, err)
		if info.Size() < testSegmentBytes {
			t.Fatalf("torn segment %s: %d", segment.name, info.Size())
		}
	}
	s = boundTest(t, root, true)
	if r := loadTest(t, s); len(r.Entries) != int(index) || r.HardState.Commit != index || r.AppliedHint != index {
		t.Fatalf("refiller recovery: entries=%d hs=%+v applied=%d", len(r.Entries), r.HardState, r.AppliedHint)
	}
}

func TestWalStore_LoadWhileOpenSeesLastWrite(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	seed(t, s, 5)
	cs := raftpb.ConfState{Voters: []uint64{7, 9}, Learners: []uint64{11}}
	check(t, s.SaveApplied(context.Background(), "group", 5, cs))
	if r := loadTest(t, s); !reflect.DeepEqual(r.Entries, storagetest.Entries(1, 5, 1)) || r.AppliedHint != 5 || !reflect.DeepEqual(r.AppliedConfState, cs) {
		t.Fatalf("live replay skipped durable commands: %+v", r)
	}
}

func TestWalStore_SnapshotReadWriteConcurrent(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	seed(t, s, 40)
	errC := make(chan error, 2)
	go func() {
		for i := uint64(1); i <= 20; i++ {
			if err := s.SaveSnapshot(context.Background(), "group", storagetest.Snapshot(i, 1)); err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	go func() {
		for range 100 {
			if _, err := s.Load(context.Background(), "group"); err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	for range 2 {
		check(t, <-errC)
	}
	broken, err := filepath.Glob(filepath.Join(s.opts.RootDir, "group", SnapDir, "*.broken"))
	check(t, err)
	if len(broken) != 0 {
		t.Fatalf("concurrent read misclassified a snapshot: %v", broken)
	}
}

func TestWalStore_HardStateNoRegress(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	seed(t, s, 10)
	before := loadTest(t, s)
	for _, write := range []storage.ReadyWrite{
		{HardState: raftpb.HardState{Term: 1, Vote: 8, Commit: 10}},
		{HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 9}},
		{HardState: raftpb.HardState{Term: 2, Vote: 7, Commit: 11}},
		{Entries: storagetest.Entries(5, 10, 2), HardState: raftpb.HardState{Term: 2, Commit: 10}},
		{Entries: storagetest.Entries(5, 5, 1)},
	} {
		if err := s.SaveReady(context.Background(), "group", write); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("unsafe Ready accepted: %+v: %v", write, err)
		}
		if r := loadTest(t, s); !reflect.DeepEqual(r, before) {
			t.Fatal("rejected Ready changed durable state")
		}
	}
}

func TestWalStore_FloorFiltering(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	seed(t, s, 10)
	if err := s.Compact(context.Background(), "group", 5); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("floor without snapshot accepted: %v", err)
	}
	check(t, s.SaveSnapshot(context.Background(), "group", storagetest.Snapshot(8, 1)))
	check(t, s.Compact(context.Background(), "group", 8))
	if r := loadTest(t, s); r.Snapshot.Metadata.Index != 8 || !reflect.DeepEqual(r.Entries, storagetest.Entries(9, 10, 1)) {
		t.Fatalf("replay boundary: %+v", r)
	}
}

func TestWalStore_CorruptOnlySnapshotIsUnreadable(t *testing.T) {
	root := t.TempDir()
	s := boundTest(t, root, false)
	seed(t, s, 3)
	check(t, s.SaveSnapshot(context.Background(), "group", storagetest.Snapshot(3, 1)))
	check(t, s.Close())
	name := filepath.Join(root, "group", SnapDir, fmt.Sprintf("%016x-%016x.snap", 1, 3))
	check(t, os.WriteFile(name, []byte("corrupt"), fileMode))
	s = openTest(t, root, "a", "test-cluster", false)
	if _, err := s.Load(context.Background(), "group"); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatalf("incomplete recovery accepted: %v", err)
	}
	check(t, os.WriteFile(name, []byte("corrupt"), fileMode))
	if _, err := s.Probe(context.Background(), "group"); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatalf("corrupt probe: %v", err)
	}
}

func TestWalStore_CorruptNewestAfterCompactionFallsBack(t *testing.T) {
	root := t.TempDir()
	s := boundTest(t, root, false)
	segmentedSeed(t, s, 100)
	ctx := context.Background()
	old := storagetest.Snapshot(1000, 1)
	check(t, s.SaveSnapshot(ctx, "group", old))
	check(t, s.SaveSnapshot(ctx, "group", storagetest.Snapshot(1500, 1)))
	check(t, s.Compact(ctx, "group", 1500))
	check(t, s.Close())
	check(t, os.WriteFile(filepath.Join(root, "group", SnapDir, fmt.Sprintf("%016x-%016x.snap", 1, 1500)), []byte("corrupt"), fileMode))
	s = boundTest(t, root, false)
	r := loadTest(t, s)
	if !reflect.DeepEqual(r.Snapshot, old) || len(r.Entries) != 1000 || r.Entries[0].Index != 1001 || r.Entries[999].Index != 2000 || r.HardState.Commit != 2000 {
		t.Fatalf("fallback lost replay chain: snapshot=%+v tail=%d hs=%+v", r.Snapshot.Metadata, len(r.Entries), r.HardState)
	}
	check(t, s.Ensure(ctx, "group"))
	check(t, s.SaveReady(ctx, "group", storage.ReadyWrite{Entries: storagetest.Entries(2001, 2001, 2), HardState: raftpb.HardState{Term: 2, Commit: 2001}}))
}

func TestWalStore_Stats(t *testing.T) {
	s := boundTest(t, t.TempDir(), true)
	stats, err := s.Stats(context.Background(), "absent")
	check(t, err)
	if stats != (storage.Stats{}) {
		t.Fatalf("absent stats: %+v", stats)
	}
	seed(t, s, 3)
	stats, err = s.Stats(context.Background(), "group")
	check(t, err)
	if stats.LogBytes != testSegmentBytes || stats.SnapshotBytes != 0 {
		t.Fatalf("pipeline temp files counted: %+v", stats)
	}
	check(t, s.SaveSnapshot(context.Background(), "group", storagetest.Snapshot(3, 1)))
	stats, err = s.Stats(context.Background(), "group")
	check(t, err)
	if stats.SnapshotBytes <= 0 || stats.SnapshotIndex != 3 {
		t.Fatalf("snapshot stats: %+v", stats)
	}
}

func TestWalStore_CloseIdempotent(t *testing.T) {
	s := boundTest(t, t.TempDir(), true)
	seed(t, s, 2)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := s.SaveReady(context.Background(), "group", storage.ReadyWrite{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestWalStore_ContextAndOptions(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Ensure(ctx, "group"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled creation: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.opts.RootDir, "group")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled call created state")
	}
	if _, err := Open(Options{RootDir: t.TempDir(), NodeName: "a", ClusterID: "test-cluster", SegmentBytes: testSegmentBytes * 2}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("conflicting global segment options accepted: %v", err)
	}
	for _, id := range []string{"_meta", "../other", "a/b", "x.quarantine-1"} {
		if err := s.Ensure(context.Background(), id); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("unsafe group path %q: %v", id, err)
		}
	}
}

func TestWalStore_CallerCannotMutateStoredState(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	entries := storagetest.Entries(1, 3, 1)
	check(t, s.SaveReady(context.Background(), "group", storage.ReadyWrite{Entries: entries, HardState: raftpb.HardState{Term: 1, Commit: 3}}))
	entries[0].Data[0] = 99
	r := loadTest(t, s)
	r.Entries[0].Data[0] = 88
	if r = loadTest(t, s); !reflect.DeepEqual(r.Entries, storagetest.Entries(1, 3, 1)) {
		t.Fatal("caller mutated durable entries")
	}
	ss := storagetest.Snapshot(3, 1)
	check(t, s.SaveSnapshot(context.Background(), "group", ss))
	ss.Data[0] = 99
	ss.Metadata.ConfState.Voters[0] = 99
	r = loadTest(t, s)
	r.Snapshot.Data[0] = 88
	if r = loadTest(t, s); !reflect.DeepEqual(r.Snapshot, storagetest.Snapshot(3, 1)) {
		t.Fatal("caller mutated durable snapshot")
	}
}

func TestWalStore_ConcurrentWriters(t *testing.T) {
	s := boundTest(t, t.TempDir(), false)
	write := storage.ReadyWrite{Entries: storagetest.Entries(1, 3, 1), HardState: raftpb.HardState{Term: 1, Commit: 3}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				if err := s.SaveReady(context.Background(), "group", write); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if r := loadTest(t, s); !reflect.DeepEqual(r.Entries, write.Entries) {
		t.Fatalf("concurrent idempotent writes: %+v", r.Entries)
	}
}
