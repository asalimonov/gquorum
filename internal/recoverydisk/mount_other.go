//go:build !linux && !darwin

package recoverydisk

import (
	"os"

	"github.com/asalimonov/gquorum/fault"
)

func openedMount(*os.File) (string, error) {
	return "", fault.New(fault.Unsupported, "recovery mount-boundary proof is unsupported on this platform", nil)
}
