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

const (
	decimalBase      = 10
	timestampBitSize = 64
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
	owner, err := readIdentity(dir)
	if err != nil {
		return "", err
	}
	if !owner.Found {
		return "", unreadable("quarantine requires an identity pin", nil)
	}
	want := storage.Identity{NodeName: s.opts.NodeName, ClusterID: s.opts.ClusterID, GroupID: id,
		ReplicaID: expectedReplicaID}
	if owner.Identity != want {
		return "", &IdentityMismatchError{Stored: owner.Identity, Resolved: want}
	}
	if g := s.groups[id]; g != nil {
		err = closeQuarantinedGroup(g)
		delete(s.groups, id)
		if err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	target, err := nextQuarantinePath(dir)
	if err != nil {
		return "", err
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

// Retired enumerates this store's exact quarantined identities. Age selection
// is
// the lifecycle hooks' responsibility. Unrelated and foreign entries stay put.
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
		base, ok := retirementToken(entry.Name())
		if !ok {
			continue
		}
		dir := filepath.Join(s.opts.RootDir, entry.Name())
		owner, err := retirementIdentity(dir)
		if err != nil {
			return nil, err
		}
		if !owner.Found || owner.Identity.NodeName != s.opts.NodeName ||
			owner.Identity.ClusterID != s.opts.ClusterID || owner.Identity.GroupID != base.GroupID {
			continue
		}
		result = append(result, storage.RetiredReplica{Identity: owner.Identity,
			Token: entry.Name(), QuarantinedAt: base.At})
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
	base, ok := retirementToken(record.Token)
	if !ok || !base.At.Equal(record.QuarantinedAt) {
		return fault.ErrInvalid
	}
	want := record.Identity
	if want.NodeName != s.opts.NodeName || want.ClusterID != s.opts.ClusterID || want.GroupID != base.GroupID ||
		want.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	if err := s.validateRetirementInactive(base.GroupID, want); err != nil {
		return err
	}
	dir := filepath.Join(s.opts.RootDir, record.Token)
	present, err := retiredDirectoryMatches(dir, want)
	if err != nil || !present {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDir(s.opts.RootDir)
}

type retirementPath struct {
	GroupID string
	At      time.Time
}

func retirementToken(token string) (retirementPath, bool) {
	if token == "" || filepath.Base(token) != token || strings.ContainsAny(token, `/\\`) {
		return retirementPath{}, false
	}
	base, stamp, found := strings.Cut(token, ".quarantine-")
	if !found || validateGroup(base) != nil {
		return retirementPath{}, false
	}
	ns, err := strconv.ParseInt(stamp, decimalBase, timestampBitSize)
	if err != nil || ns <= 0 || strconv.FormatInt(ns, decimalBase) != stamp {
		return retirementPath{}, false
	}
	return retirementPath{GroupID: base, At: time.Unix(0, ns).UTC()}, true
}

// Check directory and pin without following links. RemoveAll itself does not
// follow links inside the selected directory. RootDir remains caller-owned.
func retirementIdentity(dir string) (identityLookup, error) {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return identityLookup{}, nil
	}
	if err != nil {
		return identityLookup{}, err
	}
	if !info.IsDir() {
		return identityLookup{}, unreadable("retirement location is not a directory", nil)
	}
	info, err = os.Lstat(filepath.Join(dir, IdentityFile))
	if errors.Is(err, os.ErrNotExist) {
		return identityLookup{}, nil
	}
	if err != nil {
		return identityLookup{}, err
	}
	if !info.Mode().IsRegular() {
		return identityLookup{}, unreadable("retirement pin is not a regular file", nil)
	}
	return readIdentity(dir)
}

// Stats reports retained WAL and snapshot file sizes and the newest usable
// snapshot index.
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
		err := walkStoragePart(ctx, dir, part, &result)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
	}
	if wal.Exist(filepath.Join(dir, WALDir)) {
		selected, err := selectSnapshot(newSnapshotter(dir), filepath.Join(dir, WALDir))
		if err != nil {
			return result, err
		}
		if selected != nil {
			result.SnapshotIndex = selected.Metadata.Index
		}
	}
	return result, nil
}

func closeQuarantinedGroup(g *group) error {
	g.mu.Lock()
	g.closed = true
	if g.refillStop != nil {
		close(g.refillStop)
		<-g.refillDone
	}
	err := errors.Join(g.w.Close(), g.state.Close())
	g.mu.Unlock()
	return err
}

func nextQuarantinePath(dir string) (string, error) {
	var target string
	for nNano := time.Now().UnixNano(); ; nNano++ {
		target = dir + ".quarantine-" + strconv.FormatInt(nNano, decimalBase)
		if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", err
		}
	}
	return target, nil
}

func retiredDirectoryMatches(dir string, want storage.Identity) (bool, error) {
	owner, err := retirementIdentity(dir)
	if err != nil {
		return false, err
	}
	if !owner.Found {
		// A missing pin in an existing directory cannot prove prior collection.
		if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		return false, unreadable("retired directory lacks identity pin", nil)
	}
	if owner.Identity != want {
		return false, &IdentityMismatchError{Stored: owner.Identity, Resolved: want}
	}
	return true, nil
}

func walkStoragePart(ctx context.Context, dir, part string, result *storage.Stats) error {
	return filepath.WalkDir(filepath.Join(dir, part), func(_ string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if part == WALDir && !strings.HasSuffix(d.Name(), ".wal") || part == SnapDir &&
			!strings.HasSuffix(d.Name(), ".snap") {
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
}

func (s *Store) validateRetirementInactive(groupID string, want storage.Identity) error {
	if bound, exists := s.bindings[groupID]; exists && bound == want {
		return fault.ErrIdentityMismatch
	}
	if live := s.groups[groupID]; live != nil && live.identity == want {
		return fault.ErrIdentityMismatch
	}
	live, err := retirementIdentity(filepath.Join(s.opts.RootDir, groupID))
	if err != nil {
		return err
	}
	if live.Found && live.Identity == want {
		return fault.ErrIdentityMismatch
	}
	return nil
}
