//go:build linux

package recoverydisk

import (
	"os"
	"strconv"

	"github.com/asalimonov/gquorum/fault"
	"golang.org/x/sys/unix"
)

func openedMount(file *os.File) (string, error) {
	var info unix.Statx_t
	err := unix.Statx(int(file.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &info)
	if err != nil {
		return "", err
	}
	if info.Mask&unix.STATX_MNT_ID == 0 {
		return "", fault.New(fault.Unsupported, "filesystem mount identity is unavailable", nil)
	}
	const decimal = 10
	return strconv.FormatUint(info.Mnt_id, decimal), nil
}
