package recoverydisk

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"golang.org/x/sys/unix"
)

// RootPath resolves only a held configured root, never a serialized location.
func (o *Owner) RootPath(kind string) (string, error) {
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return "", err
	}
	return lease.path, nil
}

// WalkEntries enumerates one directory through no-follow held-root handles.
func (o *Owner) WalkEntries(ctx context.Context, kind, relative string, visit func(os.DirEntry) error) error {
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return err
	}
	if err := checkDirectoryMounts(lease.root, relative); err != nil {
		return err
	}
	dir, err := openDirectory(lease.root, relative)
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(scanPageSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (o *Owner) archiveRoot(kind string) (*Lease, error) {
	if err := o.CheckRoots(); err != nil {
		return nil, err
	}
	switch kind {
	case "system":
		return o.system, nil
	case "wal":
		if o.wal != nil {
			return o.wal, nil
		}
	default:
		return nil, fault.ErrInvalid
	}
	return nil, fault.ErrInvalid
}

// EnsureDirectory durably prepares a derived private directory on the held mount.
func (o *Owner) EnsureDirectory(kind, relative string) error {
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return err
	}
	if err := checkDirectoryMounts(lease.root, relative); err != nil {
		return err
	}
	if err := mkdirAllDurable(lease.root, relative); err != nil {
		return err
	}
	return checkDirectoryMounts(lease.root, relative)
}

// ArtifactExists observes a no-follow path; any existing type remains evidence.
func (o *Owner) ArtifactExists(kind, relative string) (bool, error) {
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return false, err
	}
	parent, err := openParent(lease.root, relative)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var info unix.Stat_t
	err = unix.Fstatat(int(parent.Fd()), filepath.Base(relative), &info, unix.AT_SYMLINK_NOFOLLOW)
	closeErr := parent.Close()
	if errors.Is(err, os.ErrNotExist) {
		return false, closeErr
	}
	return err == nil, errors.Join(err, closeErr)
}

// MoveArtifact never replaces a destination and joins both parent synchronizations.
// The semantic executor verifies the exact accepted evidence before this call.
func (o *Owner) MoveArtifact(ctx context.Context, kind, from, to string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return err
	}
	if err := checkDirectoryMounts(lease.root, filepath.Dir(to)); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "archive.rename"); err != nil {
		return err
	}
	if err := RenameNoReplace(lease.root, from, to); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "archive.parent_sync"); err != nil {
		return err
	}
	return SyncParents(lease.root, from, to)
}

// SyncArtifactParents reconciles an observed move before journal acknowledgement.
func (o *Owner) SyncArtifactParents(kind, from, to string) error {
	lease, err := o.archiveRoot(kind)
	if err != nil {
		return err
	}
	return SyncParents(lease.root, from, to)
}

func checkDirectoryMounts(root *os.Root, relative string) error {
	if !safeRelative(relative) {
		return fault.ErrInvalid
	}
	base, err := root.Open(rootRelative)
	if err != nil {
		return err
	}
	mount, err := openedMount(base)
	if err := errors.Join(err, base.Close()); err != nil {
		return err
	}
	current := rootRelative
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		dir, err := openDirectory(root, current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		actual, err := openedMount(dir)
		if err := errors.Join(err, dir.Close()); err != nil {
			return err
		}
		if actual != mount {
			return fault.New(fault.Unreadable, "private recovery directory crosses a mount boundary", nil)
		}
	}
	return nil
}

// CheckRecoveryNamespaces validates both journals and WAL history without assuming
// that pending work is an ordinary-start authorization.
func (o *Owner) CheckRecoveryNamespaces(ctx context.Context) error {
	if err := o.CheckRoots(); err != nil {
		return err
	}
	records, err := scanManifests(ctx, o.system.path)
	if err != nil {
		return err
	}
	names := make(map[string]bool, len(records))
	for _, record := range records {
		names[record.BackupName] = true
	}
	if o.wal != nil {
		return checkWALNamespace(ctx, o.wal.path, names)
	}
	return nil
}
