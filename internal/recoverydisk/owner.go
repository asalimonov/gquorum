// Package recoverydisk owns recovery filesystem boundaries and durable journals.
package recoverydisk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

// LockName is stable across archival and must never be renamed or unlinked.
const LockName = "@gquorum-owner.lock"

// Namespace holds private recovery records inside each configured root.
const Namespace = "@gquorum-recovery"
const directoryMode = 0o700
const fileMode = 0o600

// Lease retains one exact root descriptor and exclusive lock for its lifetime.
type Lease struct {
	mu              sync.Mutex
	requested, path string
	info            os.FileInfo
	lock            *fileutil.LockedFile
	root            *os.Root
	closed          bool
	history         map[string]bool // validated system-journal names; never inferred from WAL entries
}

// Owner holds system and optional WAL roots until work and callbacks are joined.
type Owner struct {
	system, wal *Lease
	journalMu   sync.Mutex
	now         func() time.Time
	boundary    func(string) error
}

// Acquire locks both roots nonblocking, releasing partial acquisitions on error.
func Acquire(ctx context.Context, paths recoverymodel.Paths) (*Owner, error) {
	system, err := AcquireRoot(ctx, paths.StorageDir)
	if err != nil {
		return nil, err
	}
	owner := &Owner{system: system}
	if paths.LoadWALDir != "" {
		wal, err := AcquireRoot(ctx, paths.LoadWALDir)
		if err != nil {
			return nil, errors.Join(err, owner.Close())
		}
		owner.wal = wal
		if within(system.path, wal.path) {
			return nil, errors.Join(fault.New(fault.Invalid, "system root is inside the WAL root", nil),
				owner.Close())
		}
	}
	return owner, nil
}

// AcquireRoot acquires an explicit root; it does not inspect or open replica stores.
func AcquireRoot(ctx context.Context, path string) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requested, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if path == "" || privateRoot(requested) {
		return nil, fault.New(fault.Invalid, "invalid storage root", nil)
	}
	if err := rejectRootLink(requested); err != nil {
		return nil, err
	}
	canonical, err := resolveRootPath(requested)
	if err != nil {
		return nil, err
	}
	if privateRoot(canonical) {
		return nil, fault.New(fault.Invalid, "storage root resolves inside recovery namespace", nil)
	}
	if err := os.MkdirAll(canonical, directoryMode); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(canonical)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	lock, err := lockRootFile(canonical)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	lease := &Lease{requested: requested, path: canonical, info: info, root: root, lock: lock}
	if err := lease.CheckRoot(); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	return lease, nil
}

func lockRootFile(path string) (*fileutil.LockedFile, error) {
	old, err := os.Lstat(filepath.Join(path, LockName))
	if err == nil && !old.Mode().IsRegular() {
		return nil, fault.New(fault.Unreadable, "invalid storage owner lock", nil)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	lock, err := fileutil.TryLockFile(filepath.Join(path, LockName), os.O_CREATE|os.O_RDWR, fileMode)
	if errors.Is(err, fileutil.ErrLocked) {
		return nil, fault.New(fault.Unavailable, "source_busy", err)
	}
	return lock, err
}

func rejectRootLink(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fault.New(fault.Unreadable, "storage root is not an independent directory", nil)
	}
	return nil
}

func privateRoot(path string) bool {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == Namespace {
			return true
		}
	}
	return false
}

func within(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// CheckRoot refuses changed root paths or replaced/symlinked owner locks.
func (l *Lease) CheckRoot() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fault.ErrStopped
	}
	current, err := filepath.EvalSymlinks(l.requested)
	if err != nil {
		return err
	}
	if current != l.path {
		return fault.New(fault.Unreadable, "storage root binding changed", nil)
	}
	info, err := os.Lstat(l.path)
	if err != nil {
		return err
	}
	if !info.IsDir() || !os.SameFile(info, l.info) {
		return fault.New(fault.Unreadable, "storage root changed", nil)
	}
	named, err := os.Lstat(filepath.Join(l.path, LockName))
	if err != nil {
		return err
	}
	held, err := l.lock.Stat()
	if err != nil {
		return err
	}
	if !named.Mode().IsRegular() || !os.SameFile(named, held) {
		return fault.New(fault.Unreadable, "storage owner lock changed", nil)
	}
	return nil
}

// CheckPath verifies that an acquisition is adopting the exact locked root.
func (l *Lease) CheckPath(path string) error {
	if err := l.CheckRoot(); err != nil {
		return err
	}
	requested, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	actual, err := filepath.EvalSymlinks(requested)
	if err != nil {
		return err
	}
	if actual != l.path {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// Close releases the lock only when the owner has joined its work.
func (l *Lease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	err := l.lock.Close()
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	l.closed = true
	return l.root.Close()
}

// CheckRoots verifies every root still denotes the same owned resource.
func (o *Owner) CheckRoots() error {
	if err := o.system.CheckRoot(); err != nil {
		return err
	}
	if o.wal != nil {
		return o.wal.CheckRoot()
	}
	return nil
}

// TakeWALLease transfers the WAL lifetime to the Node-owned store.
func (o *Owner) TakeWALLease() (*Lease, error) {
	if err := o.CheckRoots(); err != nil {
		return nil, err
	}
	lease := o.wal
	o.wal = nil
	return lease, nil
}

// ReturnWALLease restores ownership after a store failed to adopt the lease.
// The caller must serialize this with acquisition and joined teardown.
func (o *Owner) ReturnWALLease(lease *Lease) error {
	if o.wal != nil || lease == nil {
		return fault.ErrInvalid
	}
	o.wal = lease
	return nil
}

// Close releases remaining ownership after joined callbacks and stores.
func (o *Owner) Close() error {
	var err error
	if o.wal != nil {
		err = o.wal.Close()
	}
	if err != nil {
		return err
	}
	if o.system != nil {
		return o.system.Close()
	}
	return nil
}
