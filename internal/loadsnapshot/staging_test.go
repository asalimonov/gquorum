//go:build !functional

package loadsnapshot

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/asalimonov/gquorum/load/snapshot"
)

func TestMemoryStagingOwnsChunksAndBorrowsSealedView(t *testing.T) {
	t.Parallel()
	s, err := newMemorySnapshotStaging(6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Materialize(t.Context()); err == nil {
		t.Fatal("unsealed materialization")
	}
	p := []byte("abc")
	if err := s.Append(t.Context(), 0, p); err != nil {
		t.Fatal(err)
	}
	p[0] = 'X'
	dst := make([]byte, 4)
	if n, err := s.ReadAt(t.Context(), dst, 0); n != 3 || !errors.Is(err, io.EOF) || string(dst[:3]) != "abc" {
		t.Fatal(n, err, dst)
	}
	if err := s.Seal(t.Context()); err == nil {
		t.Fatal("short seal")
	}
	if err := s.Append(t.Context(), 3, []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := s.Materialize(t.Context())
	if err != nil || string(view) != "abcdef" {
		t.Fatal(view, err)
	}
	again, err := s.Materialize(t.Context())
	if err != nil || &view[0] != &again[0] {
		t.Fatal("materialize copied")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if string(view) != "abcdef" {
		t.Fatal("close recycled native-retained data")
	}
	if _, err := s.Materialize(t.Context()); err == nil {
		t.Fatal("closed materialization")
	}
}
func TestMemoryStagingRefusesUncertainAppend(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		off  uint64
		data []byte
	}{{"hole", 1, []byte("a")}, {"overflow", math.MaxUint64, []byte("a")}, {"long", 0, []byte("abcde")}, {"empty", 0, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := newMemorySnapshotStaging(4)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Append(t.Context(), tc.off, tc.data); err == nil {
				t.Fatal("invalid append accepted")
			}
			if err := s.Append(t.Context(), 0, []byte("abcd")); err == nil {
				t.Fatal("failed append resumed")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, n := range []uint64{0, math.MaxUint64} {
		if _, err := newMemorySnapshotStaging(n); err == nil {
			t.Fatal("invalid allocation", n)
		}
	}
	s, err := newMemorySnapshotStaging(4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Append(ctx, 0, []byte("abcd")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Append(t.Context(), 0, []byte("abcd")); err == nil {
		t.Fatal("canceled append resumed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadAt(t.Context(), make([]byte, 1), 0); err == nil {
		t.Fatal("read closed")
	}
}
func TestMemoryStagingRejectsWriteAfterSeal(t *testing.T) {
	t.Parallel()
	s, err := newMemorySnapshotStaging(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(t.Context(), 0, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := s.Materialize(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wantTransferReason(t, s.Append(t.Context(), 0, []byte{2}), snapshot.ReasonMalformed)
	if view[0] != 1 {
		t.Fatal("sealed data mutated")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
