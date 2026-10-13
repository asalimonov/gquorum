// Package walstore implements node-local etcd WAL persistence with full snapshots.
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
	IdentityFile   = "identity.json"
	StateFile      = "state.bolt"
	WALDir         = "wal"
	SnapDir        = "snap"
	fileMode       = 0o600
	dirMode        = 0o700
	refillInterval = 100 * time.Millisecond
)

type Options struct {
	RootDir      string
	NodeName     string
	ClusterID    string
	SegmentBytes int64
	Prewrite     *bool
	Logger       *slog.Logger
}

type IdentityMismatchError struct{ Stored, Resolved storage.Identity }

func (e *IdentityMismatchError) Error() string {
	return fmt.Sprintf("WAL identity mismatch: stored %+v, resolved %+v", e.Stored, e.Resolved)
}
func (_ *IdentityMismatchError) Unwrap() error { return fault.ErrIdentityMismatch }

type group struct {
	mu         sync.Mutex
	dir        string
	identity   storage.Identity
	w          *wal.WAL
	ss         *snap.Snapshotter
	state      *bolt.DB
	hs         raftpb.HardState
	last       uint64
	closed     bool
	refillStop chan struct{}
	refillDone chan struct{}
}

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

func Open(opts Options) (*Store, error) {
	if opts.RootDir == "" || opts.NodeName == "" || opts.ClusterID == "" {
		return nil, fault.New(fault.Invalid, "root, node and cluster must be supplied", nil)
	}
	if opts.SegmentBytes < 0 {
		return nil, fault.New(fault.Invalid, "negative segment size", nil)
	}
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = 64 << 20
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
	return &Store{opts: opts, logger: opts.Logger, groups: make(map[string]*group), bindings: make(map[string]storage.Identity), stopC: make(chan struct{})}, nil
}

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
	old, found, err := readIdentity(filepath.Join(s.opts.RootDir, id.GroupID))
	if err != nil {
		return err
	}
	if found && old != id {
		return &IdentityMismatchError{Stored: old, Resolved: id}
	}
	s.bindings[id.GroupID] = id
	return nil
}

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
	// Unknown/partial directories are evidence too; do not silently filter them.
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
	if _, exists, err := readIdentity(dir); err != nil {
		return nil, err
	} else if !exists {
		if _, statErr := os.Stat(dir); statErr == nil {
			return nil, unreadable("group directory lacks identity pin", nil)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, SnapDir), dirMode); err != nil {
		return nil, err
	}
	state, err := openState(filepath.Join(dir, StateFile))
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = state.Close()
		}
	}()
	old, found, err := readIdentity(dir)
	if err != nil {
		return nil, err
	}
	if found && old != want {
		return nil, &IdentityMismatchError{Stored: old, Resolved: want}
	}
	if !found {
		if err := writeIdentity(dir, want); err != nil {
			return nil, err
		}
	}
	g := &group{dir: dir, state: state, identity: want, ss: snap.New(zap.NewNop(), filepath.Join(dir, SnapDir))}
	wdir := filepath.Join(dir, WALDir)
	var w *wal.WAL
	if !wal.Exist(wdir) {
		if found {
			return nil, unreadable("existing replica identity has no recoverable WAL", nil)
		}
		metadata, err := json.Marshal(diskIdentity{SchemaVersion: 1, Identity: want})
		if err != nil {
			return nil, err
		}
		w, err = wal.Create(zap.NewNop(), wdir, metadata)
		if err != nil {
			return nil, err
		}
	} else {
		selected, err := selectSnapshot(g.ss, wdir)
		if err != nil {
			return nil, err
		}
		point := walpb.Snapshot{}
		if selected != nil {
			point.Index, point.Term = selected.Metadata.Index, selected.Metadata.Term
		}
		w, err = wal.Open(zap.NewNop(), wdir, point)
		if err != nil {
			return nil, err
		}
		metadata, hs, entries, err := w.ReadAll()
		if err != nil {
			_ = w.Close()
			return nil, unreadable("read WAL", err)
		}
		var recorded diskIdentity
		if err := json.Unmarshal(metadata, &recorded); err != nil || recorded.SchemaVersion != 1 || recorded.Identity != want {
			_ = w.Close()
			return nil, &IdentityMismatchError{Stored: recorded.Identity, Resolved: want}
		}
		g.hs, g.last = hs, point.Index
		if len(entries) > 0 {
			g.last = entries[len(entries)-1].Index
		}
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
	s.groups[id] = g
	cleanup = false
	if *s.opts.Prewrite {
		g.refillStop, g.refillDone = make(chan struct{}), make(chan struct{})
		s.wg.Add(1)
		go func() { defer close(g.refillDone); s.refill(wdir, g.refillStop) }()
	}
	return g, nil
}

func (s *Store) Ensure(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.openGroup(id)
	return err
}

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
