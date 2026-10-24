package walbackend

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

// SaveReady validates and persists Raft entries and monotonic hard state to the
// WAL.
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
	if err := validateReadyEntries(g, write.Entries); err != nil {
		return err
	}
	next := write.HardState
	if raft.IsEmptyHardState(next) {
		next = g.hs
	}
	if next.Term < g.hs.Term || next.Commit < g.hs.Commit || next.Term == g.hs.Term && g.hs.Vote != 0 &&
		next.Vote != g.hs.Vote {
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
	// before the bbolt applied marker can advance. DisableFSync still flushes
	// the encoder to the OS; only the disk synchronization is skipped.
	if err := g.w.Sync(); err != nil {
		return err
	}
	g.hs, g.last = next, last
	return nil
}

// SaveApplied records advancing application progress and applied membership in
// bbolt.
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

// Load verifies retained identity and restores the complete snapshot, log tail
// and applied hints.
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
	owner, err := readIdentity(dir)
	if err != nil {
		return storage.Recovery{}, err
	}
	if !owner.Found {
		if _, statErr := os.Stat(dir); statErr == nil {
			return storage.Recovery{}, unreadable("group directory lacks identity pin", nil)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return storage.Recovery{}, statErr
		}
		return storage.Recovery{}, nil
	}
	if !bound {
		want = storage.Identity{NodeName: s.opts.NodeName, ClusterID: s.opts.ClusterID, GroupID: id,
			ReplicaID: owner.Identity.ReplicaID}
	}
	if owner.Identity != want {
		return storage.Recovery{}, &IdentityMismatchError{Stored: owner.Identity, Resolved: want}
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
	return loadGroupRecovery(dir, owner.Identity, g)
}

// Probe inspects retained identity and Raft evidence without creating group
// storage.
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
	owner, err := readIdentity(dir)
	if err != nil {
		return storage.Probe{}, err
	}
	probe := storage.Probe{Exists: owner.Found, Identity: owner.Identity}
	if !owner.Found {
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
	probe.HasLog = len(recovery.Entries) > 0 || !raft.IsEmptyHardState(recovery.HardState) ||
		!raft.IsEmptySnap(recovery.Snapshot)
	return probe, nil
}

func validateReadyEntries(g *group, entries []raftpb.Entry) error {
	for i, e := range entries {
		if e.Index == 0 || e.Term == 0 || i > 0 && e.Index != entries[i-1].Index+1 {
			return fault.New(fault.Invalid, "noncontiguous or invalid WAL entries", nil)
		}
	}
	if len(entries) > 0 {
		if entries[0].Index > g.last+1 {
			return fault.New(fault.Invalid, "WAL entry gap", nil)
		}
		if entries[0].Index <= g.hs.Commit {
			return verifyReadyRetry(g, entries)
		}
	}
	return nil
}

func verifyReadyRetry(g *group, entries []raftpb.Entry) error {
	if entries[len(entries)-1].Index < g.hs.Commit {
		return fault.New(fault.Invalid, "WAL write truncates committed tail", nil)
	}
	selected, err := selectSnapshotFrom(g.ss, g.markers)
	if err != nil {
		return err
	}
	point := walpb.Snapshot{}
	if selected != nil {
		point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
	}
	replay, err := readLog(filepath.Join(g.dir, WALDir), point)
	old := replay.entries
	if err != nil {
		return unreadable("verify repeated Ready", err)
	}
	return verifyCommittedRetry(entries, old, point, g.hs.Commit)
}

func verifyCommittedRetry(entries, old []raftpb.Entry, point walpb.Snapshot, commit uint64) error {
	for _, incoming := range entries {
		if incoming.Index > commit {
			break
		}
		position := incoming.Index - point.Index - 1
		if incoming.Index <= point.Index || position >= uint64(len(old)) {
			return fault.New(fault.Invalid, "committed entry is unavailable for retry verification", nil)
		}
		previous := old[position]
		if previous.Index != incoming.Index || previous.Term != incoming.Term ||
			previous.Type != incoming.Type || !bytes.Equal(previous.Data, incoming.Data) {
			return fault.New(fault.Invalid, "WAL write changes committed entry", nil)
		}
	}
	return nil
}

func loadGroupRecovery(dir string, identity storage.Identity, g *group) (storage.Recovery, error) {
	wdir := filepath.Join(dir, WALDir)
	if !wal.Exist(wdir) {
		return storage.Recovery{}, unreadable("identity pin exists without WAL", nil)
	}
	selected, err := selectSnapshot(newSnapshotter(dir), wdir)
	if err != nil {
		return storage.Recovery{}, err
	}
	point := walpb.Snapshot{}
	if selected != nil {
		point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
	}
	replay, err := readLog(wdir, point)
	metadata, hs, entries := replay.metadata, replay.hardState, replay.entries
	if err != nil {
		return storage.Recovery{}, unreadable("recover WAL", err)
	}
	var recorded diskIdentity
	if err := json.Unmarshal(metadata, &recorded); err != nil || recorded.SchemaVersion != 1 ||
		recorded.Identity != identity {
		return storage.Recovery{}, &IdentityMismatchError{Stored: recorded.Identity, Resolved: identity}
	}
	result := storage.Recovery{HardState: hs, Entries: entries}
	if selected != nil {
		result.Snapshot = *selected
	}
	if err := validateRecoveryTail(entries, hs, point); err != nil {
		return storage.Recovery{}, err
	}
	if err := loadRecoveryApplied(dir, g, &result); err != nil {
		return storage.Recovery{}, err
	}
	return result, nil
}

func validateRecoveryTail(entries []raftpb.Entry, hs raftpb.HardState, point walpb.Snapshot) error {
	expected := point.Index + 1
	for _, e := range entries {
		if e.Index != expected {
			return unreadable("missing WAL replay entry", nil)
		}
		expected++
	}
	if hs.Commit < point.Index || hs.Commit >= expected {
		return unreadable("commit outside available snapshot and log", nil)
	}
	return nil
}

func loadRecoveryApplied(dir string, g *group, result *storage.Recovery) error {
	var err error
	var applied appliedState
	if g != nil {
		applied, err = readApplied(g.state)
	} else {
		state, openErr := bolt.Open(filepath.Join(dir, StateFile), fileMode,
			&bolt.Options{ReadOnly: true, Timeout: openTimeout})
		if openErr != nil {
			if !errors.Is(openErr, bolt.ErrTimeout) {
				return unreadable("open WAL state for recovery", openErr)
			}
			return openErr
		}
		applied, err = readApplied(state)
		_ = state.Close()
	}
	result.AppliedHint, result.AppliedConfState = applied.Index, applied.ConfState
	if err != nil {
		return unreadable("read applied metadata", err)
	}
	if result.AppliedHint > result.HardState.Commit {
		return unreadable("applied marker exceeds durable commit", nil)
	}
	return nil
}
