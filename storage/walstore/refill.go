package walstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"go.etcd.io/etcd/server/v3/storage/wal"
)

func (s *Store) refill(wdir string, groupStop <-chan struct{}) {
	defer s.wg.Done()
	t := time.NewTicker(refillInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopC:
			return
		case <-groupStop:
			return
		case <-t.C:
		}
		if _, err := os.Stat(wdir); errors.Is(err, os.ErrNotExist) {
			return
		}
		for _, n := range []int{0, 1} {
			if _, err := os.Stat(filepath.Join(wdir, fmt.Sprintf("%d.tmp", n))); errors.Is(err, os.ErrNotExist) {
				if err := prewriteTmp(wdir, n); err != nil {
					s.logger.Warn("WAL prewrite failed", "path", filepath.Join(wdir, fmt.Sprintf("%d.tmp", n)), "error", err)
				}
			}
		}
	}
}

func prewriteTmp(wdir string, n int) error {
	path := filepath.Join(wdir, fmt.Sprintf("%d.tmp", n))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil
		}
		_ = os.Remove(path)
		return err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	zeros := make([]byte, 1<<20)
	for left := wal.SegmentSizeBytes; left > 0; left -= int64(len(zeros)) {
		chunk := zeros
		if left < int64(len(zeros)) {
			chunk = zeros[:left]
		}
		if _, err := f.Write(chunk); err != nil {
			_ = os.Remove(path)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return syncDir(wdir)
}
