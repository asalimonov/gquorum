package walstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

// Keep the exact pin until all other contents have been removed and synced.
// A retry with no pin may only remove an empty directory, never recurse.
func removeCollectedDirectory(ctx context.Context, dir string, want storage.Identity) error {
	owner, err := retirementIdentity(dir)
	if err != nil {
		return err
	}
	if owner.Found {
		if owner.Identity != want {
			return &IdentityMismatchError{Stored: owner.Identity, Resolved: want}
		}
		if err := removeCollectionContents(ctx, dir); err != nil {
			return err
		}
		if err := syncDir(dir); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(dir, IdentityFile)); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Remove(dir)
}

func removeCollectionContents(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Name() == IdentityFile {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// ForgetCollection releases only the exact marker after the retired directory
// is absent. The caller retains its collected journal row until this succeeds.
func (s *Store) ForgetCollection(ctx context.Context, record storage.RetiredReplica) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateCollectionRecord(record); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	if err := s.validateCollectionRoot(); err != nil {
		return err
	}
	dir := filepath.Join(s.opts.RootDir, record.Token)
	if _, err := os.Lstat(dir); err == nil {
		return fault.ErrIdentityMismatch
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	marker, err := readCollectionMarker(dir + collectionSuffix)
	if err != nil {
		return err
	}
	if marker != nil {
		if !sameRetiredRecord(marker.Retired, record) {
			return fault.ErrIdentityMismatch
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(dir + collectionSuffix); err != nil {
			return err
		}
	}
	return syncDir(s.opts.RootDir)
}
