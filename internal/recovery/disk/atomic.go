package disk

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"golang.org/x/sys/unix"
)

// AtomicWrite publishes a fully synced private file under exclusive root ownership.
// replace=false refuses every existing destination, including a symlink.
func AtomicWrite(root *os.Root, relative string, data []byte, replace bool) error {
	return writeAtomic(root, relative, data, atomicOptions{tempDir: filepath.Dir(relative), replace: replace})
}

type atomicOptions struct {
	tempDir  string
	replace  bool
	boundary func(string) error
}

func writeAtomic(root *os.Root, relative string, data []byte, options atomicOptions) error {
	if !safeRelative(relative) || !safeRelative(options.tempDir) {
		return fault.ErrInvalid
	}
	temporary, err := temporaryName(options.tempDir)
	if err != nil {
		return err
	}
	file, err := openRelative(root, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, fileMode)
	if err != nil {
		return err
	}
	defer func() { _ = removeRelative(root, temporary) }()
	if _, err = file.Write(data); err == nil {
		err = atBoundary(options.boundary, "temp_sync")
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err := atBoundary(options.boundary, "rename"); err != nil {
		return err
	}
	if options.replace {
		err = renameRelative(root, temporary, relative, replaceRegular)
	} else {
		err = RenameNoReplace(root, temporary, relative)
	}
	if err != nil {
		return err
	}
	if err := atBoundary(options.boundary, "parent_sync"); err != nil {
		return err
	}
	return SyncParents(root, temporary, relative)
}

func temporaryName(parent string) (string, error) {
	var entropy [16]byte
	if _, err := io.ReadFull(rand.Reader, entropy[:]); err != nil {
		return "", err
	}
	return filepath.Join(parent, "@tmp-"+hex.EncodeToString(entropy[:])), nil
}
func atBoundary(boundary func(string) error, point string) error {
	if boundary != nil {
		return boundary(point)
	}
	return nil
}

// RenameNoReplace uses the exclusive-owner protocol and no-follow parent handles.
// Unsupported privileged concurrent filesystem mutation is never a lock protocol.
func RenameNoReplace(root *os.Root, from, to string) error {
	return renameRelative(root, from, to, noReplace)
}

type replacementMode uint8

const (
	noReplace replacementMode = iota
	replaceRegular
)

func renameRelative(root *os.Root, from, to string, mode replacementMode) error {
	source, err := openParent(root, from)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := openParent(root, to)
	if err != nil {
		return err
	}
	defer target.Close()
	var info unix.Stat_t
	if err := unix.Fstatat(int(target.Fd()), filepath.Base(to), &info, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		if mode == noReplace {
			return os.ErrExist
		}
		if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Nlink != 1 {
			return fault.ErrUnreadable
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err := unix.Fstatat(int(source.Fd()), filepath.Base(from), &info, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	kind := info.Mode & unix.S_IFMT
	if kind != unix.S_IFREG && kind != unix.S_IFDIR || kind == unix.S_IFREG && info.Nlink != 1 {
		return fault.ErrUnreadable
	}
	return unix.Renameat(int(source.Fd()), filepath.Base(from), int(target.Fd()), filepath.Base(to))
}

// SyncParents syncs both names after publication, including cross-directory renames.
func SyncParents(root *os.Root, relatives ...string) error {
	seen := make(map[string]bool, len(relatives))
	for _, relative := range relatives {
		parent := filepath.Dir(relative)
		if seen[parent] {
			continue
		}
		seen[parent] = true
		file, err := openDirectory(root, parent)
		if err != nil {
			return err
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return nil
}
