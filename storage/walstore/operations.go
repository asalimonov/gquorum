package walstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (s *Store) SaveReady(ctx context.Context, id string, write storage.ReadyWrite) error {
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
	for i, e := range write.Entries {
		if e.Index == 0 || e.Term == 0 || i > 0 && e.Index != write.Entries[i-1].Index+1 {
			return fault.New(fault.Invalid, "noncontiguous or invalid WAL entries", nil)
		}
	}
	if len(write.Entries) > 0 {
		if write.Entries[0].Index > g.last+1 {
			return fault.New(fault.Invalid, "WAL entry gap", nil)
		}
		if write.Entries[0].Index <= g.hs.Commit {
			if write.Entries[len(write.Entries)-1].Index < g.hs.Commit {
				return fault.New(fault.Invalid, "WAL write truncates committed tail", nil)
			}
			selected, err := selectSnapshot(g.ss, filepath.Join(g.dir, WALDir), g.dir)
			if err != nil {
				return err
			}
			point := walpb.Snapshot{}
			if selected != nil {
				point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
			}
			_, _, old, err := readLog(filepath.Join(g.dir, WALDir), point)
			if err != nil {
				return unreadable("verify repeated Ready", err)
			}
			for _, incoming := range write.Entries {
				if incoming.Index > g.hs.Commit {
					break
				}
				position := incoming.Index - point.Index - 1
				if incoming.Index <= point.Index || position >= uint64(len(old)) {
					return fault.New(fault.Invalid, "committed entry is unavailable for retry verification", nil)
				}
				previous := old[position]
				if previous.Index != incoming.Index || previous.Term != incoming.Term || previous.Type != incoming.Type || !bytes.Equal(previous.Data, incoming.Data) {
					return fault.New(fault.Invalid, "WAL write changes committed entry", nil)
				}
			}
		}
	}
	next := write.HardState
	if raft.IsEmptyHardState(next) {
		next = g.hs
	}
	if next.Term < g.hs.Term || next.Commit < g.hs.Commit || next.Term == g.hs.Term && g.hs.Vote != 0 && next.Vote != g.hs.Vote {
		return fault.New(fault.Invalid, "WAL hard state would regress or change vote in one term", nil)
	}
	last := g.last
	if len(write.Entries) > 0 {
		last = write.Entries[len(write.Entries)-1].Index
	}
	if next.Commit > last {
		return fault.New(fault.Invalid, "WAL commit exceeds available log", nil)
	}
	if err := g.w.Save(next, write.Entries); err != nil {
		return err
	}
	// etcd can buffer commit-only updates, and its internal HardState may have
	// advanced after an uncertain failed Save. Flush every acknowledged write
	// before the separately durable bbolt applied marker can advance.
	if err := g.w.Sync(); err != nil {
		return err
	}
	g.hs, g.last = next, last
	return nil
}

func (s *Store) SaveApplied(ctx context.Context, id string, index uint64, cs raftpb.ConfState) error {
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
	if index > g.hs.Commit {
		return fault.New(fault.Invalid, "applied index exceeds durable commit", nil)
	}
	encoded, err := cs.Marshal()
	if err != nil {
		return err
	}
	return g.state.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if getU64(b, keyApplied) >= index {
			return nil
		}
		if err := putU64(b, keyApplied, index); err != nil {
			return err
		}
		return b.Put(keyConfState, encoded)
	})
}

func (s *Store) Load(ctx context.Context, id string) (storage.Recovery, error) {
	if err := ctx.Err(); err != nil {
		return storage.Recovery{}, err
	}
	if err := validateGroup(id); err != nil {
		return storage.Recovery{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.Recovery{}, fault.ErrStopped
	}
	g := s.groups[id]
	want, bound := s.bindings[id]
	dir := filepath.Join(s.opts.RootDir, id)
	owner, exists, err := readIdentity(dir)
	if err != nil {
		return storage.Recovery{}, err
	}
	if !exists {
		if _, statErr := os.Stat(dir); statErr == nil {
			return storage.Recovery{}, unreadable("group directory lacks identity pin", nil)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return storage.Recovery{}, statErr
		}
		return storage.Recovery{}, nil
	}
	if !bound {
		want = storage.Identity{NodeName: s.opts.NodeName, ClusterID: s.opts.ClusterID, GroupID: id, ReplicaID: owner.ReplicaID}
	}
	if owner != want {
		return storage.Recovery{}, &IdentityMismatchError{Stored: owner, Resolved: want}
	}
	if g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.closed {
			return storage.Recovery{}, fault.ErrStopped
		}
		if err := g.w.Sync(); err != nil {
			return storage.Recovery{}, err
		}
	}
	wdir := filepath.Join(dir, WALDir)
	if !wal.Exist(wdir) {
		return storage.Recovery{}, unreadable("identity pin exists without WAL", nil)
	}
	selected, err := selectSnapshot(newSnapshotter(dir), wdir, dir)
	if err != nil {
		return storage.Recovery{}, err
	}
	point := walpb.Snapshot{}
	if selected != nil {
		point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
	}
	metadata, hs, entries, err := readLog(wdir, point)
	if err != nil {
		return storage.Recovery{}, unreadable("recover WAL", err)
	}
	var recorded diskIdentity
	if err := json.Unmarshal(metadata, &recorded); err != nil || recorded.SchemaVersion != 1 || recorded.Identity != owner {
		return storage.Recovery{}, &IdentityMismatchError{Stored: recorded.Identity, Resolved: owner}
	}
	result := storage.Recovery{HardState: hs, Entries: entries}
	if selected != nil {
		result.Snapshot = *selected
	}
	expected := point.Index + 1
	for _, e := range entries {
		if e.Index != expected {
			return storage.Recovery{}, unreadable("missing WAL replay entry", nil)
		}
		expected++
	}
	if hs.Commit < point.Index || hs.Commit >= expected {
		return storage.Recovery{}, unreadable("commit outside available snapshot and log", nil)
	}
	if g != nil {
		result.AppliedHint, result.AppliedConfState, err = readApplied(g.state)
	} else {
		state, openErr := bolt.Open(filepath.Join(dir, StateFile), fileMode, &bolt.Options{ReadOnly: true, Timeout: openTimeout})
		if openErr != nil {
			if !errors.Is(openErr, bolt.ErrTimeout) {
				return storage.Recovery{}, unreadable("open WAL state for recovery", openErr)
			}
			return storage.Recovery{}, openErr
		}
		result.AppliedHint, result.AppliedConfState, err = readApplied(state)
		_ = state.Close()
	}
	if err != nil {
		return storage.Recovery{}, unreadable("read applied metadata", err)
	}
	if result.AppliedHint > hs.Commit {
		return storage.Recovery{}, unreadable("applied marker exceeds durable commit", nil)
	}
	return result, nil
}

func (s *Store) Probe(ctx context.Context, id string) (storage.Probe, error) {
	if err := ctx.Err(); err != nil {
		return storage.Probe{}, err
	}
	if err := validateGroup(id); err != nil {
		return storage.Probe{}, err
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return storage.Probe{}, fault.ErrStopped
	}
	dir := filepath.Join(s.opts.RootDir, id)
	owner, exists, err := readIdentity(dir)
	if err != nil {
		return storage.Probe{}, err
	}
	probe := storage.Probe{Exists: exists, Identity: owner}
	if !exists {
		if _, err := os.Stat(dir); err == nil {
			return probe, unreadable("group directory lacks identity pin", nil)
		} else if !os.IsNotExist(err) {
			return probe, err
		}
		return probe, nil
	}
	recovery, err := s.Load(ctx, id)
	if err != nil {
		return probe, err
	}
	probe.HasLog = !raft.IsEmptyHardState(recovery.HardState) || !raft.IsEmptySnap(recovery.Snapshot) || len(recovery.Entries) > 0
	return probe, nil
}
