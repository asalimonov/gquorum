package walstore

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
)

const quarantineRetention = 7 * 24 * time.Hour

// Quarantine closes and moves precisely the specified local replica. A stale
// caller cannot move a replacement replica's directory.
func (s *Store) Quarantine(ctx context.Context, id string, expectedReplicaID uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateGroup(id); err != nil {
		return "", err
	}
	if expectedReplicaID == 0 {
		return "", fault.New(fault.Invalid, "expected replica ID must be positive", nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", fault.ErrStopped
	}
	dir := filepath.Join(s.opts.RootDir, id)
	owner, exists, err := readIdentity(dir)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", unreadable("quarantine requires an identity pin", nil)
	}
	want := storage.Identity{NodeName: s.opts.NodeName, ClusterID: s.opts.ClusterID, GroupID: id, ReplicaID: expectedReplicaID}
	if owner != want {
		return "", &IdentityMismatchError{Stored: owner, Resolved: want}
	}
	if g := s.groups[id]; g != nil {
		g.mu.Lock()
		g.closed = true
		if g.refillStop != nil {
			close(g.refillStop)
			<-g.refillDone
		}
		err = errors.Join(g.w.Close(), g.state.Close())
		g.mu.Unlock()
		delete(s.groups, id)
		if err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var target string
	for n := time.Now().UnixNano(); ; n++ {
		target = dir + ".quarantine-" + strconv.FormatInt(n, 10)
		if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", err
		}
	}
	if err := os.Rename(dir, target); err != nil {
		return "", err
	}
	delete(s.bindings, id)
	if err := syncDir(s.opts.RootDir); err != nil {
		return target, err
	}
	return target, nil
}

// SweepQuarantine removes only this store's expired quarantine directories.
// Live groups and unrelated application directories are never selected.
func (s *Store) SweepQuarantine(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	entries, err := os.ReadDir(s.opts.RootDir)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-quarantineRetention)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}
		base, stamp, found := strings.Cut(entry.Name(), ".quarantine-")
		if !found || validateGroup(base) != nil {
			continue
		}
		ns, err := strconv.ParseInt(stamp, 10, 64)
		if err != nil || ns <= 0 || !time.Unix(0, ns).Before(cutoff) {
			continue
		}
		dir := filepath.Join(s.opts.RootDir, entry.Name())
		owner, found, err := readIdentity(dir)
		if err != nil {
			return err
		}
		if !found || owner.NodeName != s.opts.NodeName || owner.ClusterID != s.opts.ClusterID || owner.GroupID != base {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return syncDir(s.opts.RootDir)
}

func (s *Store) Stats(ctx context.Context, id string) (storage.Stats, error) {
	if err := ctx.Err(); err != nil {
		return storage.Stats{}, err
	}
	if err := validateGroup(id); err != nil {
		return storage.Stats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.Stats{}, fault.ErrStopped
	}
	if g := s.groups[id]; g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
	}
	dir := filepath.Join(s.opts.RootDir, id)
	var result storage.Stats
	for _, part := range []string{WALDir, SnapDir} {
		err := filepath.WalkDir(filepath.Join(dir, part), func(path string, d fs.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if part == WALDir && !strings.HasSuffix(d.Name(), ".wal") || part == SnapDir && !strings.HasSuffix(d.Name(), ".snap") {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if part == WALDir {
				result.LogBytes += info.Size()
			} else {
				result.SnapshotBytes += info.Size()
			}
			return nil
		})
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
	}
	if wal.Exist(filepath.Join(dir, WALDir)) {
		selected, err := selectSnapshot(newSnapshotter(dir), filepath.Join(dir, WALDir), dir)
		if err != nil {
			return result, err
		}
		if selected != nil {
			result.SnapshotIndex = selected.Metadata.Index
		}
	}
	return result, nil
}
