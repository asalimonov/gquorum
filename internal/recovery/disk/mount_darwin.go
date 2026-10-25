//go:build darwin

package disk

import (
	"os"

	"golang.org/x/sys/unix"
)

func openedMount(file *os.File) (string, error) {
	var info unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &info); err != nil {
		return "", err
	}
	return unix.ByteSliceToString(info.Mntonname[:]), nil
}
