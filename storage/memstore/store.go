// Package memstore keeps load replicas' Raft state in process memory. Nothing
// survives the process: a replica that restarts finds no state, and one that
// had joined its group must be replaced under a fresh replica identity.
// Writes follow the same validation rules as the WAL store.
package memstore

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Options binds the in-memory store to one node and cluster.
type Options struct{ NodeName, ClusterID string }

// Store is safe for concurrent use. The component that creates it owns Close.
type Store struct {
	mu       sync.Mutex
	opts     Options
	bindings map[string]storage.Identity
	groups   map[string]*group
	retired  []storage.RetiredReplica
	sequence uint64 // makes retirement tokens unique
	closed   bool
}

type group struct {
	identity storage.Identity
	hs       raftpb.HardState
	snap     raftpb.Snapshot // newest complete snapshot only
	entries  []raftpb.Entry  // indexes after snap.Metadata.Index
	applied  uint64
	conf     raftpb.ConfState
}

var (
	_ storage.Store           = (*Store)(nil)
	_ storage.RetirementStore = (*Store)(nil)
)

// New validates owner identity and creates an empty in-memory store.
func New(o Options) (*Store, error) {
	if o.NodeName == "" || o.ClusterID == "" {
		return nil, fault.New(fault.Invalid, "node and cluster must be supplied", nil)
	}
	return &Store{opts: o, bindings: make(map[string]storage.Identity), groups: make(map[string]*group)}, nil
}

// enter locks an open store. Callers unlock.
func (s *Store) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fault.ErrStopped
	}
	return nil
}

// open returns a group's state, creating it for a bound identity.
func (s *Store) open(id string) (*group, error) {
	if g := s.groups[id]; g != nil {
		return g, nil
	}
	want, bound := s.bindings[id]
	if !bound {
		return nil, fault.New(fault.Invalid, "bind replica before creating a group store", nil)
	}
	g := &group{identity: want}
	s.groups[id] = g
	return g, nil
}

func (g *group) last() uint64 {
	if n := len(g.entries); n > 0 {
		return g.entries[n-1].Index
	}
	return g.snap.Metadata.Index
}

// HasState reports in-process evidence: bound, live or retired replicas. A
// newly created store always reports false.
func (s *Store) HasState(ctx context.Context) (bool, error) {
	if err := s.enter(ctx); err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	return len(s.bindings) != 0 || len(s.groups) != 0 || len(s.retired) != 0, nil
}

// BindReplica binds an exact identity and refuses conflicting retained state.
func (s *Store) BindReplica(ctx context.Context, id storage.Identity) error {
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	if id.NodeName != s.opts.NodeName || id.ClusterID != s.opts.ClusterID || id.ReplicaID == 0 {
		return fault.New(fault.IdentityMismatch, "binding does not match store owner or has zero replica ID", nil)
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if old, exists := s.bindings[id.GroupID]; exists && old != id {
		return fault.New(fault.IdentityMismatch, "group is bound to another replica", nil)
	}
	if g := s.groups[id.GroupID]; g != nil && g.identity != id {
		return fault.New(fault.IdentityMismatch, "group holds another replica", nil)
	}
	s.bindings[id.GroupID] = id
	return nil
}

// Probe inspects a group's in-memory identity and Raft evidence without
// creating it.
func (s *Store) Probe(ctx context.Context, id string) (storage.Probe, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return storage.Probe{}, err
	}
	if err := s.enter(ctx); err != nil {
		return storage.Probe{}, err
	}
	defer s.mu.Unlock()
	g := s.groups[id]
	if g == nil {
		return storage.Probe{}, nil
	}
	hasLog := len(g.entries) > 0 || !raft.IsEmptyHardState(g.hs) || !raft.IsEmptySnap(g.snap)
	return storage.Probe{Exists: true, HasLog: hasLog, Identity: g.identity}, nil
}

// Ensure creates empty Raft state for an already bound group if needed.
func (s *Store) Ensure(ctx context.Context, id string) error {
	if err := topology.ValidateLoadID(id); err != nil {
		return err
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	_, err := s.open(id)
	return err
}

// SaveReady validates and copies Raft entries and monotonic hard state.
func (s *Store) SaveReady(ctx context.Context, id string, w storage.ReadyWrite) error {
	if err := topology.ValidateLoadID(id); err != nil {
		return err
	}
	for i, e := range w.Entries {
		if e.Index == 0 || e.Term == 0 || i > 0 && e.Index != w.Entries[i-1].Index+1 {
			return fault.New(fault.Invalid, "noncontiguous or invalid entries", nil)
		}
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	g, err := s.open(id)
	if err != nil {
		return err
	}
	last, base := g.last(), g.snap.Metadata.Index
	if n := len(w.Entries); n > 0 {
		first := w.Entries[0].Index
		if first > last+1 {
			return fault.New(fault.Invalid, "entry gap", nil)
		}
		if first <= g.hs.Commit {
			if err := verifyReadyRetry(g, w.Entries); err != nil {
				return err
			}
		}
		last = w.Entries[n-1].Index
	}
	return applyReadyWrite(g, w, last, base)
}

// SaveApplied records advancing application progress and an owned membership
// state.
func (s *Store) SaveApplied(ctx context.Context, id string, index uint64, cs raftpb.ConfState) error {
	if err := topology.ValidateLoadID(id); err != nil {
		return err
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	g, err := s.open(id)
	if err != nil {
		return err
	}
	if index > g.hs.Commit {
		return fault.New(fault.Invalid, "applied index exceeds commit", nil)
	}
	if index > g.applied {
		g.applied, g.conf = index, cloneConf(cs)
	}
	return nil
}

// SaveSnapshot keeps the log after the snapshot only when it continues from
// the snapshot's index and term, as for a local compaction snapshot. A snapshot
// received from a leader replaces a divergent or shorter local log.
func (s *Store) SaveSnapshot(ctx context.Context, id string, ss raftpb.Snapshot) error {
	if err := topology.ValidateLoadID(id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if raft.IsEmptySnap(ss) {
		return nil
	}
	if len(ss.Data) == 0 || ss.Metadata.Term == 0 || len(ss.Metadata.ConfState.Voters) == 0 {
		return fault.New(fault.Invalid, "snapshot requires full state and real membership", nil)
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	g, err := s.open(id)
	if err != nil {
		return err
	}
	base, index := g.snap.Metadata.Index, ss.Metadata.Index
	if index <= base {
		return nil
	}
	if index < g.hs.Commit && ss.Metadata.Term > g.hs.Term {
		return fault.New(fault.Invalid, "snapshot term exceeds current term", nil)
	}
	var tail []raftpb.Entry
	last := g.last()
	if index <= last && g.entries[index-base-1].Term == ss.Metadata.Term {
		tail = g.entries[index-base:]
	}
	g.entries = slices.Clone(tail) // a fresh array releases the compacted prefix
	g.snap = cloneSnapshot(ss)
	if g.hs.Term < ss.Metadata.Term {
		g.hs.Term, g.hs.Vote = ss.Metadata.Term, 0
	}
	g.hs.Commit = max(g.hs.Commit, index)
	return nil
}

// Compact validates the request. Entries covered by the snapshot were already
// released by SaveSnapshot.
func (s *Store) Compact(ctx context.Context, id string, index uint64) error {
	if err := topology.ValidateLoadID(id); err != nil {
		return err
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	g, err := s.open(id)
	if err != nil {
		return err
	}
	if index > g.snap.Metadata.Index {
		return fault.New(fault.Invalid, "compaction requires a full snapshot", nil)
	}
	return nil
}

// Load returns owned copies of the complete snapshot, retained log and applied
// hints.
func (s *Store) Load(ctx context.Context, id string) (storage.Recovery, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return storage.Recovery{}, err
	}
	if err := s.enter(ctx); err != nil {
		return storage.Recovery{}, err
	}
	defer s.mu.Unlock()
	g := s.groups[id]
	if g == nil {
		return storage.Recovery{}, nil
	}
	return storage.Recovery{
		Snapshot:         cloneSnapshot(g.snap),
		Entries:          cloneEntries(g.entries),
		HardState:        g.hs,
		AppliedHint:      g.applied,
		AppliedConfState: cloneConf(g.conf),
	}, nil
}

// Stats reports retained sizes of one group.
func (s *Store) Stats(ctx context.Context, id string) (storage.Stats, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return storage.Stats{}, err
	}
	if err := s.enter(ctx); err != nil {
		return storage.Stats{}, err
	}
	defer s.mu.Unlock()
	var result storage.Stats
	if g := s.groups[id]; g != nil {
		for _, e := range g.entries {
			result.LogBytes += int64(e.Size())
		}
		result.SnapshotBytes, result.SnapshotIndex = int64(len(g.snap.Data)), g.snap.Metadata.Index
	}
	return result, nil
}

// Close releases all state. Later calls return ErrStopped.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.bindings, s.groups, s.retired = nil, nil, nil
	return nil
}

func cloneEntries(in []raftpb.Entry) []raftpb.Entry {
	if len(in) == 0 {
		return nil
	}
	out := make([]raftpb.Entry, len(in))
	for i, e := range in {
		e.Data = bytes.Clone(e.Data)
		out[i] = e
	}
	return out
}

func cloneSnapshot(ss raftpb.Snapshot) raftpb.Snapshot {
	ss.Data = bytes.Clone(ss.Data)
	ss.Metadata.ConfState = cloneConf(ss.Metadata.ConfState)
	return ss
}

func cloneConf(cs raftpb.ConfState) raftpb.ConfState {
	cs.Voters, cs.Learners = slices.Clone(cs.Voters), slices.Clone(cs.Learners)
	cs.VotersOutgoing, cs.LearnersNext = slices.Clone(cs.VotersOutgoing), slices.Clone(cs.LearnersNext)
	return cs
}

func verifyReadyRetry(g *group, entries []raftpb.Entry) error {
	base := g.snap.Metadata.Index
	if entries[len(entries)-1].Index < g.hs.Commit {
		return fault.New(fault.Invalid, "write truncates committed tail", nil)
	}
	// A retried Ready may repeat committed entries, but never change
	// them.
	for _, incoming := range entries {
		if incoming.Index > g.hs.Commit {
			break
		}
		if incoming.Index <= base {
			return fault.New(fault.Invalid, "committed entry is unavailable for retry verification", nil)
		}
		old := g.entries[incoming.Index-base-1]
		if old.Term != incoming.Term || old.Type != incoming.Type || !bytes.Equal(old.Data, incoming.Data) {
			return fault.New(fault.Invalid, "write changes committed entry", nil)
		}
	}
	return nil
}

func applyReadyWrite(g *group, w storage.ReadyWrite, last, base uint64) error {
	next := w.HardState
	if raft.IsEmptyHardState(next) {
		next = g.hs
	}
	if next.Term < g.hs.Term || next.Commit < g.hs.Commit || next.Term == g.hs.Term && g.hs.Vote != 0 &&
		next.Vote != g.hs.Vote {
		return fault.New(fault.Invalid, "hard state would regress or change vote in one term", nil)
	}
	if next.Commit > last {
		return fault.New(fault.Invalid, "commit exceeds available log", nil)
	}
	if len(w.Entries) > 0 {
		keepIndex := w.Entries[0].Index - base - 1
		if keepIndex > uint64(len(g.entries)) {
			return fault.New(fault.Invalid, "entry offset exceeds stored log", nil)
		}
		//nolint:gosec // keepIndex is bounded by len(g.entries) above.
		keep := int(keepIndex)
		clear(g.entries[keep:]) // release an overwritten suffix
		g.entries = append(g.entries[:keep], cloneEntries(w.Entries)...)
	}
	g.hs = next
	return nil
}
