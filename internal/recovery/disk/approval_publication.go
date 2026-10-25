package disk

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"golang.org/x/sys/unix"
)

const approvalPrefix = "@approve-"
const approvalEntropyHexLength = 32
const manifestFile = "manifest.json"

// Only this reserved staging shape is nonauthorizing. It can hold a partial
// manifest, but never source files, application backups or progress. The directory
// rename publishes the first guard; a crash before that leaves A usable.
func (o *Owner) publishApproval(name string, data []byte) error {
	root := o.system.root
	if err := mkdirAllDurable(root, Namespace); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "approval.namespace"); err != nil {
		return err
	}
	temporary, err := temporaryName(Namespace)
	if err != nil {
		return err
	}
	temporary = filepath.Join(Namespace, approvalPrefix+strings.TrimPrefix(filepath.Base(temporary), "@tmp-"))
	if err := mkdirAllDurable(root, temporary); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "approval.mkdir"); err != nil {
		return err
	}
	if err := o.writeApproval(filepath.Join(temporary, manifestFile), data); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "manifest.rename"); err != nil {
		return err
	}
	final := filepath.Join(Namespace, name)
	if err := RenameNoReplace(root, temporary, final); err != nil {
		return err
	}
	if err := atBoundary(o.boundary, "manifest.parent_sync"); err != nil {
		return err
	}
	return SyncParents(root, temporary, final)
}

func (o *Owner) writeApproval(relative string, data []byte) error {
	file, err := openRelative(o.system.root, relative, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, fileMode)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = atBoundary(o.boundary, "manifest.temp_sync")
	}
	if err == nil {
		err = file.Sync()
	}
	if err := errors.Join(err, file.Close()); err != nil {
		return err
	}
	return SyncParents(o.system.root, relative)
}

func approvalTemporary(name string) bool {
	if !strings.HasPrefix(name, approvalPrefix) {
		return false
	}
	suffix := strings.TrimPrefix(name, approvalPrefix)
	if len(suffix) != approvalEntropyHexLength || suffix != strings.ToLower(suffix) {
		return false
	}
	_, err := hex.DecodeString(suffix)
	return err == nil
}

func validateApprovalTemporary(root *os.Root, relative string) error {
	dir, err := openDirectory(root, relative)
	if errors.Is(err, os.ErrNotExist) { // Concurrent publication moved it to its final name.
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if len(entries) != 1 || entries[0].Name() != manifestFile {
		return fault.New(fault.Unreadable, "unowned approval staging contents", nil)
	}
	var info unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), manifestFile, &info, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Nlink != 1 || info.Size < 0 ||
		uint64(info.Size) > model.ManifestMaxBytes {
		return fault.New(fault.Unreadable, "invalid approval staging file", nil)
	}
	return nil
}

// Cleanup is serialized with mutations by the owner lease. Read-only discovery
// neither adopts nor deletes staging, and malformed or foreign contents refuse.
func (o *Owner) cleanupApprovals(ctx context.Context) error {
	dir, err := openNamespace(o.system.root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(scanPageSize)
		if err := o.cleanupApprovalPage(ctx, entries); err != nil {
			return err
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (o *Owner) cleanupApprovalPage(ctx context.Context, entries []os.DirEntry) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if approvalTemporary(entry.Name()) {
			if err := o.removeApprovalTemporary(filepath.Join(Namespace, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Owner) removeApprovalTemporary(relative string) error {
	root := o.system.root
	if err := validateApprovalTemporary(root, relative); err != nil {
		return err
	}
	if err := removeRelative(root, filepath.Join(relative, manifestFile)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := removeEmptyDirectory(root, relative); err != nil {
		return err
	}
	return SyncParents(root, relative)
}
