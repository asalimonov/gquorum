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

var _ storage.RetirementStore = (*Store)(nil)

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

// Retired enumerates this store's exact quarantined identities. Age selection is
// the lifecycle policy's responsibility. Unrelated and foreign entries stay put.
func (s *Store) Retired(ctx context.Context) ([]storage.RetiredReplica, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fault.ErrStopped
	}
	entries, err := os.ReadDir(s.opts.RootDir)
	if err != nil {
		return nil, err
	}
	var result []storage.RetiredReplica
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() {
			continue
		}
		base, at, ok := retirementToken(entry.Name())
		if !ok {
			continue
		}
		dir := filepath.Join(s.opts.RootDir, entry.Name())
		owner, found, err := retirementIdentity(dir)
		if err != nil {
			return nil, err
		}
		if !found || owner.NodeName != s.opts.NodeName || owner.ClusterID != s.opts.ClusterID || owner.GroupID != base {
			continue
		}
		result = append(result, storage.RetiredReplica{Identity: owner, Token: entry.Name(), QuarantinedAt: at})
	}
	return result, nil
}

// Collect removes precisely an enumerated retirement record after rechecking
// confinement, its timestamp and durable pin. A successor in the live namespace
// is untouched; the same identity still bound or live is refused.
func (s *Store) Collect(ctx context.Context, record storage.RetiredReplica) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	base, at, ok := retirementToken(record.Token)
	if !ok || !at.Equal(record.QuarantinedAt) {
		return fault.ErrInvalid
	}
	want := record.Identity
	if want.NodeName != s.opts.NodeName || want.ClusterID != s.opts.ClusterID || want.GroupID != base || want.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	if bound, exists := s.bindings[base]; exists && bound == want {
		return fault.ErrIdentityMismatch
	}
	if live := s.groups[base]; live != nil && live.identity == want {
		return fault.ErrIdentityMismatch
	}
	live, found, err := retirementIdentity(filepath.Join(s.opts.RootDir, base))
	if err != nil {
		return err
	}
	if found && live == want {
		return fault.ErrIdentityMismatch
	}
	dir := filepath.Join(s.opts.RootDir, record.Token)
	owner, found, err := retirementIdentity(dir)
	if err != nil {
		return err
	}
	if !found {
		// A missing pin in an existing directory cannot prove prior collection.
		if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		return unreadable("retired directory lacks identity pin", nil)
	}
	if owner != want {
		return &IdentityMismatchError{Stored: owner, Resolved: want}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDir(s.opts.RootDir)
}

func retirementToken(token string) (string, time.Time, bool) {
	if token == "" || filepath.Base(token) != token || strings.ContainsAny(token, `/\\`) {
		return "", time.Time{}, false
	}
	base, stamp, found := strings.Cut(token, ".quarantine-")
	if !found || validateGroup(base) != nil {
		return "", time.Time{}, false
	}
	ns, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil || ns <= 0 || strconv.FormatInt(ns, 10) != stamp {
		return "", time.Time{}, false
	}
	return base, time.Unix(0, ns).UTC(), true
}

// Check directory and pin without following links. RemoveAll itself does not
// follow links inside the selected directory. RootDir remains caller-owned.
func retirementIdentity(dir string) (storage.Identity, bool, error) {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return storage.Identity{}, false, nil
	}
	if err != nil {
		return storage.Identity{}, false, err
	}
	if !info.IsDir() {
		return storage.Identity{}, false, unreadable("retirement location is not a directory", nil)
	}
	info, err = os.Lstat(filepath.Join(dir, IdentityFile))
	if errors.Is(err, os.ErrNotExist) {
		return storage.Identity{}, false, nil
	}
	if err != nil {
		return storage.Identity{}, false, err
	}
	if !info.Mode().IsRegular() {
		return storage.Identity{}, false, unreadable("retirement pin is not a regular file", nil)
	}
	return readIdentity(dir)
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
