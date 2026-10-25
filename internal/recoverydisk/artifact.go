package recoverydisk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"golang.org/x/sys/unix"
)

// ArtifactDigest streams a canonical whole-content digest of an opaque artifact.
// Names are bounded separately from file content; no link or device is followed.
func (o *Owner) ArtifactDigest(ctx context.Context, kind, relative string, maxNames uint64) (string, error) {
	walk, err := o.artifactWalk(ctx, kind, maxNames)
	if err != nil {
		return "", err
	}
	walk.digest = sha256.New()
	walk.base = relative
	if err := walk.visit(relative, 0); err != nil {
		return "", err
	}
	return hex.EncodeToString(walk.digest.Sum(nil)), nil
}

// ValidateArtifact checks move safety without hashing active WAL preallocation.
func (o *Owner) ValidateArtifact(ctx context.Context, kind, relative string, maxNames uint64) error {
	walk, err := o.artifactWalk(ctx, kind, maxNames)
	if err != nil {
		return err
	}
	return walk.visit(relative, 0)
}

type artifactWalk struct {
	ctx       context.Context
	root      *os.Root
	device    unix.Stat_t
	remaining uint64
	digest    hash.Hash
	base      string
}

func (o *Owner) artifactWalk(ctx context.Context, kind string, maxNames uint64) (*artifactWalk, error) {
	if err := o.CheckRoots(); err != nil {
		return nil, err
	}
	lease := o.system
	if kind == "wal" {
		lease = o.wal
	} else if kind != "system" {
		return nil, fault.ErrInvalid
	}
	if lease == nil || maxNames == 0 {
		return nil, fault.ErrInvalid
	}
	file, err := lease.root.Open(rootRelative)
	if err != nil {
		return nil, err
	}
	var info unix.Stat_t
	err = unix.Fstat(int(file.Fd()), &info)
	if err := errors.Join(err, file.Close()); err != nil {
		return nil, err
	}
	return &artifactWalk{ctx: ctx, root: lease.root, device: info, remaining: maxNames}, nil
}

func (w *artifactWalk) visit(relative string, depth int) error {
	const maxArtifactDepth = 128
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if depth > maxArtifactDepth || !safeRelative(relative) || uint64(len(relative)) > w.remaining {
		return fault.New(fault.Unreadable, "artifact names exceed recovery budget", nil)
	}
	w.remaining -= uint64(len(relative))
	parent, err := openParent(w.root, relative)
	if err != nil {
		return err
	}
	var info unix.Stat_t
	err = unix.Fstatat(int(parent.Fd()), filepath.Base(relative), &info, unix.AT_SYMLINK_NOFOLLOW)
	if err := errors.Join(err, parent.Close()); err != nil {
		return err
	}
	if info.Dev != w.device.Dev {
		return fault.New(fault.Unreadable, "nested artifact mount refuses movement", nil)
	}
	kind := info.Mode & unix.S_IFMT
	if kind == unix.S_IFDIR {
		return w.directory(relative, depth)
	}
	if kind != unix.S_IFREG || info.Nlink != 1 || info.Size < 0 {
		return fault.New(fault.Unreadable, "artifact contains a link or special file", nil)
	}
	if w.digest == nil {
		return nil
	}
	return w.file(relative, info.Size)
}

func (w *artifactWalk) directory(relative string, depth int) error {
	dir, err := openDirectory(w.root, relative)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := w.names(dir)
	if err != nil {
		return err
	}
	slices.Sort(names)
	if w.digest != nil {
		w.frame("directory", relative, 0)
	}
	for _, name := range names {
		if err := w.visit(filepath.Join(relative, name), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (w *artifactWalk) file(relative string, size int64) error {
	if size < 0 {
		return fault.ErrUnreadable
	}
	file, err := openRelative(w.root, relative, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	w.frame("file", relative, uint64(size))
	var buffer [64 << 10]byte
	remaining := size
	for remaining > 0 {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		count, err := file.Read(buffer[:min(int64(len(buffer)), remaining)])
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrUnexpectedEOF
		}
		_, _ = w.digest.Write(buffer[:count])
		remaining -= int64(count)
	}
	return nil
}

func (w *artifactWalk) frame(kind, name string, size uint64) {
	within, _ := filepath.Rel(w.base, name)
	var encoded [8]byte
	for _, value := range []string{kind, within} {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(value)))
		_, _ = w.digest.Write(encoded[:])
		_, _ = w.digest.Write([]byte(value))
	}
	binary.BigEndian.PutUint64(encoded[:], size)
	_, _ = w.digest.Write(encoded[:])
}

func (w *artifactWalk) names(dir *os.File) ([]string, error) {
	var names []string
	for {
		entries, readErr := dir.ReadDir(scanPageSize)
		for _, entry := range entries {
			if err := w.ctx.Err(); err != nil {
				return nil, err
			}
			if uint64(len(entry.Name())) > w.remaining {
				return nil, fault.ErrUnreadable
			}
			w.remaining -= uint64(len(entry.Name()))
			names = append(names, entry.Name())
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return names, nil
}
