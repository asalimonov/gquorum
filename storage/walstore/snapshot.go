package walstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const openTimeout = time.Second

func validateGroup(id string) error { return topology.ValidateLoadID(id) }
func newSnapshotter(dir string) *snap.Snapshotter {
	return snap.New(zap.NewNop(), filepath.Join(dir, SnapDir))
}

func selectSnapshot(ss *snap.Snapshotter, wdir, dir string) (*raftpb.Snapshot, error) {
	markers, err := wal.ValidSnapshotEntries(zap.NewNop(), wdir)
	if err != nil {
		return nil, unreadable("read snapshot markers", err)
	}
	selected, err := ss.LoadNewestAvailable(markers)
	if !errors.Is(err, snap.ErrNoSnapshot) {
		if err != nil {
			return nil, unreadable("load full snapshot", err)
		}
		return selected, nil
	}
	for _, marker := range markers {
		if marker.Index > 0 {
			return nil, unreadable("no valid full snapshot for durable WAL marker", err)
		}
	}
	return nil, nil
}

func snapshotNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".snap") {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	return names, nil
}

// Retention counts complete, committed snapshots only. Orphan files left by an
// interrupted save cannot displace a usable fallback snapshot.
func retainedSnapshotNames(dir, wdir string) ([]string, error) {
	markers, err := wal.ValidSnapshotEntries(zap.NewNop(), wdir)
	if err != nil {
		return nil, err
	}
	names, err := snapshotNames(dir)
	if err != nil {
		return nil, err
	}
	var valid []string
	for _, name := range names {
		ss, err := snap.Read(zap.NewNop(), filepath.Join(dir, name))
		if err != nil {
			continue
		}
		for _, marker := range markers {
			if marker.Index == ss.Metadata.Index && marker.Term == ss.Metadata.Term {
				valid = append(valid, name)
				break
			}
		}
	}
	return valid, nil
}

func (s *Store) SaveSnapshot(ctx context.Context, id string, snapshot raftpb.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	if len(snapshot.Data) == 0 || snapshot.Metadata.Term == 0 || len(snapshot.Metadata.ConfState.Voters) == 0 {
		return fault.New(fault.Invalid, "snapshot requires full state and real membership", nil)
	}
	g, err := s.openGroup(id)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return fault.ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var snapshotIndex uint64
	if err := g.state.View(func(tx *bolt.Tx) error { snapshotIndex = getU64(tx.Bucket(bucketMeta), keySnapmark); return nil }); err != nil {
		return err
	}
	if snapshot.Metadata.Index <= snapshotIndex {
		return nil
	}
	if snapshot.Metadata.Index < g.hs.Commit {
		// A local compaction snapshot may lag commit, but cannot change the Raft term.
		if snapshot.Metadata.Term > g.hs.Term {
			return fault.New(fault.Invalid, "snapshot term exceeds current term", nil)
		}
	}
	if err := g.ss.SaveSnap(snapshot); err != nil {
		return err
	}
	name := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Term, snapshot.Metadata.Index)
	if err := os.Chmod(filepath.Join(g.dir, SnapDir, name), fileMode); err != nil {
		return err
	}
	if err := syncDir(filepath.Join(g.dir, SnapDir)); err != nil {
		return err
	}
	cs := snapshot.Metadata.ConfState
	if err := g.w.SaveSnapshot(walpb.Snapshot{Index: snapshot.Metadata.Index, Term: snapshot.Metadata.Term, ConfState: &cs}); err != nil {
		return err
	}
	next := g.hs
	if next.Term < snapshot.Metadata.Term {
		next.Term, next.Vote = snapshot.Metadata.Term, 0
	}
	next.Commit = max(next.Commit, snapshot.Metadata.Index)
	if err := g.w.Save(next, nil); err != nil {
		return err
	}
	// SaveSnapshot flushed the marker before this HardState was encoded. The
	// snapshot's recovery commit must also be durable before publishing it.
	if err := g.w.Sync(); err != nil {
		return err
	}
	g.hs = next
	g.last = max(g.last, snapshot.Metadata.Index)
	if err := g.state.Update(func(tx *bolt.Tx) error { return putU64(tx.Bucket(bucketMeta), keySnapmark, snapshot.Metadata.Index) }); err != nil {
		return err
	}
	names, err := retainedSnapshotNames(filepath.Join(g.dir, SnapDir), filepath.Join(g.dir, WALDir))
	if err != nil {
		return err
	}
	for len(names) > 2 {
		if err := os.Remove(filepath.Join(g.dir, SnapDir, names[0])); err != nil {
			return err
		}
		names = names[1:]
	}
	return syncDir(filepath.Join(g.dir, SnapDir))
}

func (s *Store) Compact(ctx context.Context, id string, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g, err := s.openGroup(id)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return fault.ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	latest, err := selectSnapshot(g.ss, filepath.Join(g.dir, WALDir), g.dir)
	if err != nil {
		return err
	}
	if latest == nil || index > latest.Metadata.Index {
		return fault.New(fault.Invalid, "compaction requires durable full snapshot", nil)
	}
	names, err := retainedSnapshotNames(filepath.Join(g.dir, SnapDir), filepath.Join(g.dir, WALDir))
	if err != nil {
		return err
	}
	limit := index
	for _, name := range names {
		ss, err := snap.Read(zap.NewNop(), filepath.Join(g.dir, SnapDir, name))
		if err != nil {
			return unreadable("read retained snapshot before compaction", err)
		}
		limit = min(limit, ss.Metadata.Index)
	}
	if err := g.state.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if getU64(b, keyFloor) < index {
			return putU64(b, keyFloor, index)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := g.w.ReleaseLockTo(limit); err != nil {
		return err
	}
	if err := unlinkSegmentsBelow(filepath.Join(g.dir, WALDir), limit); err != nil {
		return err
	}
	return syncDir(filepath.Join(g.dir, WALDir))
}
