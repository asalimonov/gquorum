package disk

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"golang.org/x/sys/unix"
)

const rootRelative = "."

func safeRelative(path string) bool {
	return path != "" && path != ".." && !filepath.IsAbs(path) && filepath.Clean(path) == path &&
		!strings.HasPrefix(path, ".."+string(filepath.Separator)) && !strings.ContainsRune(path, 0)
}
func openDirectory(root *os.Root, path string) (*os.File, error) {
	if !safeRelative(path) {
		return nil, fault.ErrInvalid
	}
	current, err := root.Open(rootRelative)
	if err != nil {
		return nil, err
	}
	if path == rootRelative {
		return current, nil
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		fd, err := unix.Openat(int(current.Fd()), part,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := current.Close()
		if err != nil {
			return nil, errors.Join(err, closeErr)
		}
		current = os.NewFile(uintptr(fd), part)
		if closeErr != nil {
			return nil, errors.Join(closeErr, current.Close())
		}
	}
	return current, nil
}
func openParent(root *os.Root, relative string) (*os.File, error) {
	if relative == rootRelative || !safeRelative(relative) {
		return nil, fault.ErrInvalid
	}
	return openDirectory(root, filepath.Dir(relative))
}
func openRelative(root *os.Root, relative string, flags int, mode uint32) (*os.File, error) {
	parent, err := openParent(root, relative)
	if err != nil {
		return nil, err
	}
	fd, openErr := unix.Openat(int(parent.Fd()), filepath.Base(relative),
		flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
	closeErr := parent.Close()
	if openErr != nil {
		return nil, errors.Join(openErr, closeErr)
	}
	file := os.NewFile(uintptr(fd), relative)
	if closeErr != nil {
		return nil, errors.Join(closeErr, file.Close())
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(fault.ErrUnreadable, err, file.Close())
	}
	return file, nil
}
func removeRelative(root *os.Root, relative string) error {
	parent, err := openParent(root, relative)
	if err != nil {
		return err
	}
	return errors.Join(unix.Unlinkat(int(parent.Fd()), filepath.Base(relative), 0), parent.Close())
}
func mkdirAllDurable(root *os.Root, relative string) error {
	if !safeRelative(relative) {
		return fault.ErrInvalid
	}
	parent := rootRelative
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == rootRelative {
			continue
		}
		file, err := openDirectory(root, parent)
		if err != nil {
			return err
		}
		err = unix.Mkdirat(int(file.Fd()), part, directoryMode)
		if errors.Is(err, unix.EEXIST) {
			err = nil
		}
		if err == nil {
			err = file.Sync()
		}
		if err = errors.Join(err, file.Close()); err != nil {
			return err
		}
		parent = filepath.Join(parent, part)
		check, err := openDirectory(root, parent)
		if err != nil {
			return err
		}
		if err := check.Close(); err != nil {
			return err
		}
	}
	return nil
}
