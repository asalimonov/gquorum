package disk

import (
	"errors"
	"io"
	"math"
	"os"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"golang.org/x/sys/unix"
)

func readRecordFrom(root *os.Root, relative string, value any, maxBytes uint64) error {
	if maxBytes > math.MaxInt64-1 {
		return fault.ErrInvalid
	}
	file, err := openRelative(root, relative, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return errors.Join(err, file.Close())
	}
	size := info.Size()
	if size < 0 {
		return errors.Join(fault.ErrUnreadable, file.Close())
	}
	if uint64(size) > maxBytes {
		return errors.Join(fault.ErrUnreadable, file.Close())
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	if err := model.DecodeStrict(data, value, maxBytes); err != nil {
		return err
	}
	switch r := value.(type) {
	case *model.LocalManifest:
		return model.ValidateManifest(*r)
	case *model.Current:
		return model.ValidateCurrent(*r)
	default:
		return fault.ErrInvalid
	}
}
