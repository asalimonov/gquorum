package gquorum

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

const (
	metaDBFilename      = "_meta.bolt"
	authorityDBFilename = "_authority.bolt"
)

func (o NodeOptions) metaDBPath() string { return filepath.Join(o.StorageDir, metaDBFilename) }
func (o NodeOptions) authorityDBPath() string {
	return filepath.Join(o.StorageDir, authorityDBFilename)
}

// Validation is lexical: NewNode must not inspect or acquire caller resources.
func validateStorageDir(o NodeOptions) error {
	if strings.TrimSpace(o.StorageDir) == "" || strings.ContainsRune(o.StorageDir, 0) {
		return fault.New(fault.Invalid, "StorageDir requires an explicit directory", nil)
	}
	if wal := o.Loads.Storage.WAL; wal != nil && pathWithin(o.StorageDir, wal.RootDir) {
		return fault.New(fault.Invalid, "StorageDir must be outside the load WAL retirement root", nil)
	}
	return nil
}

// Resolve existing ancestors as well as complete files, so a symlinked parent
// cannot hide an absent authority file inside a replaceable storage directory.
func resolveStoragePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	current := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if info, linkErr := os.Lstat(current); linkErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fault.New(fault.Invalid, "storage path contains a dangling symlink", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func (n *node) checkAuthorityPath() error {
	dir, err := resolveStoragePath(n.opts.StorageDir)
	if err != nil {
		return err
	}
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return fault.New(fault.Invalid, "StorageDir is not a directory", nil)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A directory with former system filenames is retained evidence, never a
	// fresh node. There is no implicit migration or permission to reset its pins.
	for _, name := range []string{"meta.bolt", "authority.bolt"} {
		if _, err := os.Lstat(filepath.Join(n.opts.StorageDir, name)); err == nil {
			return fault.New(fault.Unreadable, "StorageDir contains unsupported system filename "+name, nil)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	path, err := resolveStoragePath(n.opts.authorityDBPath())
	if err != nil {
		return err
	}
	meta, err := resolveStoragePath(n.opts.metaDBPath())
	if err != nil {
		return err
	}
	if path == meta {
		return fault.New(fault.Invalid, "authority and meta databases resolve to the same path", nil)
	}
	a, aerr := os.Stat(path)
	m, merr := os.Stat(meta)
	if aerr != nil && !errors.Is(aerr, os.ErrNotExist) {
		return aerr
	}
	if merr != nil && !errors.Is(merr, os.ErrNotExist) {
		return merr
	}
	if merr == nil && !m.Mode().IsRegular() {
		return fault.New(fault.Invalid, "metadata database is not a regular file", nil)
	}
	if aerr == nil && (!a.Mode().IsRegular() || merr == nil && os.SameFile(a, m)) {
		return fault.New(fault.Invalid, "authority database is not an independent regular file", nil)
	}
	if wal := n.opts.Loads.Storage.WAL; wal != nil {
		root, err := resolveStoragePath(wal.RootDir)
		if err != nil {
			return err
		}
		if pathWithin(dir, root) || pathWithin(path, root) || pathWithin(meta, root) {
			return fault.New(fault.Invalid, "system storage resolves inside the load WAL retirement root", nil)
		}
	}
	return nil
}
