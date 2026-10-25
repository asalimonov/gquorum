package loadsnapshot

import (
	"context"
	"io"
	"math"

	"github.com/asalimonov/gquorum/load/snapshot"
)

// TODO: Add a disk-backed snapshotStaging in the group owner's storage namespace,
// using one stable incoming-snapshot name per group rather than configurable
// per-transfer paths; preserve exact transfer fencing and joined cleanup.
type snapshotStaging interface {
	Append(context.Context, uint64, []byte) error
	ReadAt(context.Context, []byte, uint64) (int, error)
	Seal(context.Context) error
	Materialize(context.Context) ([]byte, error)
	Close() error
}

type memorySnapshotStaging struct {
	data           []byte
	written        uint64
	sealed, closed bool
	broken         error
}

// newMemorySnapshotStaging requires the caller to hold the complete reservation.
func newMemorySnapshotStaging(total uint64) (snapshotStaging, error) {
	if total == 0 {
		return nil, transferError(snapshot.ReasonMalformed)
	}
	if total > uint64(math.MaxInt) {
		return nil, &snapshot.TransferError{Reason: snapshot.ReasonSizeLimit, Size: total, Limit: uint64(math.MaxInt)}
	}
	return &memorySnapshotStaging{data: make([]byte, int(total))}, nil
}
func (s *memorySnapshotStaging) available(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return transferError(snapshot.ReasonStopped)
	}
	if s.broken != nil {
		return s.broken
	}
	return nil
}
func (s *memorySnapshotStaging) Append(ctx context.Context, off uint64, p []byte) error {
	err := s.available(ctx)
	if err == nil && (len(p) == 0 || s.sealed || off != s.written || off > uint64(len(s.data)) ||
		uint64(len(p)) > uint64(len(s.data))-off) {
		err = transferError(snapshot.ReasonMalformed)
	}
	if err != nil {
		s.broken = err
		return err
	}
	copy(s.data[off:], p)
	s.written += uint64(len(p))
	return nil
}
func (s *memorySnapshotStaging) ReadAt(ctx context.Context, dst []byte, off uint64) (int, error) {
	if err := s.available(ctx); err != nil {
		return 0, err
	}
	if off > s.written {
		return 0, io.EOF
	}
	n := copy(dst, s.data[off:s.written])
	if n != len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *memorySnapshotStaging) Seal(ctx context.Context) error {
	if err := s.available(ctx); err != nil {
		return err
	}
	if s.written != uint64(len(s.data)) {
		return transferError(snapshot.ReasonMalformed)
	}
	s.sealed = true
	return nil
}
func (s *memorySnapshotStaging) Materialize(ctx context.Context) ([]byte, error) {
	if err := s.available(ctx); err != nil {
		return nil, err
	}
	if !s.sealed {
		return nil, transferError(snapshot.ReasonMalformed)
	}
	return s.data, nil
}
func (s *memorySnapshotStaging) Close() error {
	s.data = nil
	s.closed = true
	s.broken = nil
	return nil
}
