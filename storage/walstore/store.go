// Package walstore implements node-local etcd WAL persistence with full
// snapshots.
package walstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	defaultSegmentBytes = 64 << 20
)

// Replica storage names identify the durable pin, applied-state database, WAL
// and snapshots.
const (
	IdentityFile   = "identity.json"
	StateFile      = "state.bolt"
	WALDir         = "wal"
	SnapDir        = "snap"
	fileMode       = 0o600
	dirMode        = 0o700
	refillInterval = 100 * time.Millisecond
)

// Options configures node ownership, the caller-selected storage root and WAL
// writes.
type Options struct {
	RootDir      string
	NodeName     string
	ClusterID    string
	SegmentBytes int64
	Prewrite     *bool
	// DisableFSync skips fsync for WAL and applied-state writes. Writes still
	// reach the OS, but host crashes can lose acknowledged data or corrupt
	// state. Snapshot files, identity pins and directory synchronization are
	// unchanged.
	DisableFSync bool
	Logger       *slog.Logger
}

// IdentityMismatchError preserves both the retained identity and the requested
// binding.
type IdentityMismatchError struct{ Stored, Resolved storage.Identity }

func (e *IdentityMismatchError) Error() string {
	return fmt.Sprintf("WAL identity mismatch: stored %+v, resolved %+v", e.Stored, e.Resolved)
}

func (*IdentityMismatchError) Unwrap() error { return fault.ErrIdentityMismatch }

type group struct {
	mu       sync.Mutex
	dir      string
	identity storage.Identity
	w        *wal.WAL
	ss       *snap.Snapshotter
	state    *bolt.DB
	hs       raftpb.HardState
	last     uint64
	// markers caches the durable WAL snapshot markers. It is scanned once when
	// the group opens and then maintained by SaveSnapshot and Compact, so those
	// hot paths never re-read the whole WAL.
	markers    []walpb.Snapshot
	closed     bool
	refillStop chan struct{}
	refillDone chan struct{}
}

// Store owns node-local replica WALs, snapshotters and applied-state databases.
type Store struct {
	mu        sync.Mutex
	opts      Options
	logger    *slog.Logger
	groups    map[string]*group
	bindings  map[string]storage.Identity
	closed    bool
	stopC     chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

var segmentConfig struct {
	sync.Mutex
	size int64
}
var _ storage.Store = (*Store)(nil)

// Open validates options and prepares a WAL root, fixing the process-wide
// segment size.
func Open(opts Options) (*Store, error) {
	if opts.RootDir == "" || opts.NodeName == "" || opts.ClusterID == "" {
		return nil, fault.New(fault.Invalid, "root, node and cluster must be supplied", nil)
	}
	if opts.SegmentBytes < 0 {
		return nil, fault.New(fault.Invalid, "negative segment size", nil)
	}
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	segmentConfig.Lock()
	if segmentConfig.size != 0 && segmentConfig.size != opts.SegmentBytes {
		segmentConfig.Unlock()
		return nil, fault.New(fault.Invalid, "etcd WAL segment size is already fixed for this process", nil)
	}
	if segmentConfig.size == 0 {
		segmentConfig.size = opts.SegmentBytes
		wal.SegmentSizeBytes = opts.SegmentBytes
	}
	segmentConfig.Unlock()
	if opts.Prewrite == nil {
		enabled := true
		opts.Prewrite = &enabled
	} else {
		enabled := *opts.Prewrite
		opts.Prewrite = &enabled
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(opts.RootDir, dirMode); err != nil {
		return nil, err
	}
	return &Store{
		opts:     opts,
		logger:   opts.Logger,
		groups:   make(map[string]*group),
		bindings: make(map[string]storage.Identity),
		stopC:    make(chan struct{}),
	}, nil
}

// BindReplica validates an exact owner identity against existing bindings and
// durable pins.
func (s *Store) BindReplica(ctx context.Context, id storage.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	if id.NodeName != s.opts.NodeName || id.ClusterID != s.opts.ClusterID || id.ReplicaID == 0 {
		return fault.New(fault.IdentityMismatch, "binding does not match store owner or has zero replica ID", nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	if old, exists := s.bindings[id.GroupID]; exists && old != id {
		return &IdentityMismatchError{Stored: old, Resolved: id}
	}
	old, err := readIdentity(filepath.Join(s.opts.RootDir, id.GroupID))
	if err != nil {
		return err
	}
	if old.Found && old.Identity != id {
		return &IdentityMismatchError{Stored: old.Identity, Resolved: id}
	}
	s.bindings[id.GroupID] = id
	return nil
}

// HasState reports bound or retained evidence, including unknown and partial
// root entries.
func (s *Store) HasState(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, fault.ErrStopped
	}
	if len(s.bindings) != 0 || len(s.groups) != 0 {
		return true, nil
	}
	entries, err := os.ReadDir(s.opts.RootDir)
	if err != nil {
		return false, err
	}
	// Unknown/partial directories are evidence too; do not silently filter
	// them.
	return len(entries) != 0, ctx.Err()
}

func (s *Store) openGroup(id string) (*group, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fault.ErrStopped
	}
	if g := s.groups[id]; g != nil {
		return g, nil
	}
	want, bound := s.bindings[id]
	if !bound {
		return nil, fault.New(fault.Invalid, "bind replica before creating a group store", nil)
	}
	dir := filepath.Join(s.opts.RootDir, id)
	g, err := s.openGroupFiles(dir, want)
	if err != nil {
		return nil, err
	}
	wdir := filepath.Join(dir, WALDir)
	s.groups[id] = g
	if *s.opts.Prewrite {
		g.refillStop, g.refillDone = make(chan struct{}), make(chan struct{})
		s.wg.Add(1)
		go func() { defer close(g.refillDone); s.refill(wdir, g.refillStop) }()
	}
	return g, nil
}

// Ensure opens or creates a group's durable resources after exact identity
// binding.
func (s *Store) Ensure(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.openGroup(id)
	return err
}

// Close joins prewrite workers and closes every acquired WAL and applied-state
// database.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.stopC)
		s.wg.Wait()
		defer s.mu.Unlock()
		for _, g := range s.groups {
			g.mu.Lock()
			g.closed = true
			s.closeErr = errors.Join(s.closeErr, g.w.Close(), g.state.Close())
			g.mu.Unlock()
		}
	})
	return s.closeErr
}

func unreadable(message string, cause error) error {
	return fault.New(fault.Unreadable, message, cause)
}

func (s *Store) openGroupFiles(dir string, want storage.Identity) (*group, error) {
	if err := validateUnpinnedDirectory(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, SnapDir), dirMode); err != nil {
		return nil, err
	}
	state, err := openState(filepath.Join(dir, StateFile), s.opts.DisableFSync)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = state.Close()
		}
	}()
	old, err := ensureGroupPin(dir, want)
	if err != nil {
		return nil, err
	}
	g := &group{dir: dir, state: state, identity: want, ss: snap.New(zap.NewNop(), filepath.Join(dir, SnapDir))}
	w, err := acquireGroupWAL(g, old)
	if err != nil {
		return nil, err
	}
	if s.opts.DisableFSync {
		w.SetUnsafeNoFsync()
	}
	g.w = w
	if err := syncDir(dir); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := syncDir(s.opts.RootDir); err != nil {
		_ = w.Close()
		return nil, err
	}
	cleanup = false
	return g, nil
}

func validateUnpinnedDirectory(dir string) error {
	if exists, err := readIdentity(dir); err != nil {
		return err
	} else if !exists.Found {
		if _, statErr := os.Stat(dir); statErr == nil {
			return unreadable("group directory lacks identity pin", nil)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}
	return nil
}

func ensureGroupPin(dir string, want storage.Identity) (identityLookup, error) {
	old, err := readIdentity(dir)
	if err != nil {
		return identityLookup{}, err
	}
	if old.Found && old.Identity != want {
		return identityLookup{}, &IdentityMismatchError{Stored: old.Identity, Resolved: want}
	}
	if !old.Found {
		if err := writeIdentity(dir, want); err != nil {
			return identityLookup{}, err
		}
	}
	return old, nil
}

func acquireGroupWAL(g *group, old identityLookup) (*wal.WAL, error) {
	wdir := filepath.Join(g.dir, WALDir)
	if !wal.Exist(wdir) {
		if old.Found {
			return nil, unreadable("existing replica identity has no recoverable WAL", nil)
		}
		metadata, err := json.Marshal(diskIdentity{SchemaVersion: 1, Identity: g.identity})
		if err != nil {
			return nil, err
		}
		w, err := wal.Create(zap.NewNop(), wdir, metadata)
		if err != nil {
			return nil, err
		}
		g.markers = []walpb.Snapshot{{}}
		return w, nil
	}
	return reopenGroupWAL(g)
}

func reopenGroupWAL(g *group) (*wal.WAL, error) {
	wdir := filepath.Join(g.dir, WALDir)
	var err error
	g.markers, err = wal.ValidSnapshotEntries(zap.NewNop(), wdir)
	if err != nil {
		return nil, unreadable("read snapshot markers", err)
	}
	selected, err := selectSnapshotFrom(g.ss, g.markers)
	if err != nil {
		return nil, err
	}
	point := walpb.Snapshot{}
	if selected != nil {
		point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
	}
	w, err := wal.Open(zap.NewNop(), wdir, point)
	if err != nil {
		return nil, err
	}
	metadata, hs, entries, err := w.ReadAll()
	if err != nil {
		_ = w.Close()
		return nil, unreadable("read WAL", err)
	}
	var recorded diskIdentity
	if err := json.Unmarshal(metadata, &recorded); err != nil || recorded.SchemaVersion != 1 ||
		recorded.Identity != g.identity {
		_ = w.Close()
		return nil, &IdentityMismatchError{Stored: recorded.Identity, Resolved: g.identity}
	}
	g.hs, g.last = hs, point.Index
	if len(entries) > 0 {
		g.last = entries[len(entries)-1].Index
	}
	return w, nil
}
