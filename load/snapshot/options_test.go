//go:build !functional

package snapshot

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestOptionsDefaults(t *testing.T) {
	t.Parallel()
	input := Options{}
	got, err := input.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	want := Options{
		MaxSnapshotBytes: 256 << 20, MaxIncoming: 1, MaxOutgoing: 1,
		MaxReceiveBytes: 256 << 20, MaxSendBytes: 256 << 20,
		ChunkBytes: 1 << 20, WindowChunks: 8,
		TotalTimeout: 30 * time.Minute, ProgressTimeout: time.Minute,
	}
	if got != want || input != (Options{}) {
		t.Fatalf("defaults=%+v input=%+v", got, input)
	}
	again, err := got.Resolve()
	if err != nil || again != got || input.Validate() != nil {
		t.Fatalf("resolution must be idempotent: %+v %v", again, err)
	}
}

func TestOptionsLargerThanQualificationCeiling(t *testing.T) {
	t.Parallel()
	// Validate tens of gigabytes without allocating a snapshot or touching I/O.
	input := Options{MaxSnapshotBytes: 64 << 30, MaxIncoming: 2, MaxOutgoing: 3}
	got, err := input.Resolve()
	if strconv.IntSize < 64 {
		if !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("unrepresentable platform allocation accepted", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxSnapshotBytes != input.MaxSnapshotBytes || got.MaxReceiveBytes != 128<<30 ||
		got.MaxSendBytes != 192<<30 {
		t.Fatalf("test ceiling must not cap production settings: %+v", got)
	}
}

func TestOptionsIndependentBudgets(t *testing.T) {
	t.Parallel()
	input := Options{MaxSnapshotBytes: 1024, MaxIncoming: 4, MaxOutgoing: 8,
		MaxReceiveBytes: 1024, MaxSendBytes: 2048, ChunkBytes: 4096,
		TotalTimeout: time.Hour, ProgressTimeout: 2 * time.Minute}
	got, err := input.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxReceiveBytes != 1024 || got.MaxSendBytes != 2048 || got.MaxIncoming != 4 ||
		got.MaxOutgoing != 8 || got.TotalTimeout != time.Hour || got.ProgressTimeout != 2*time.Minute {
		t.Fatalf("explicit count, byte and deadline bounds changed: %+v", got)
	}
	// A snapshot smaller than a chunk is a valid final chunk, not bad policy.
	if got.ChunkBytes != input.ChunkBytes {
		t.Fatal("chunk size was silently changed")
	}
}

func TestOptionsRefuseInvalidSettings(t *testing.T) {
	t.Parallel()
	maxInt := int(^uint(0) >> 1)
	cases := []struct {
		name string
		in   Options
	}{
		{"incoming negative", Options{MaxIncoming: -1}},
		{"outgoing negative", Options{MaxOutgoing: -1}},
		{"chunk negative", Options{ChunkBytes: -1}},
		{"window negative", Options{WindowChunks: -1}},
		{"total negative", Options{TotalTimeout: -1}},
		{"progress negative", Options{ProgressTimeout: -1}},
		{"progress exceeds total", Options{TotalTimeout: time.Second}},
		{"deadline addition overflow", Options{TotalTimeout: time.Duration(math.MaxInt64)}},
		{"unrepresentable snapshot", Options{MaxSnapshotBytes: math.MaxUint64}},
		{"snapshot framing overflow", Options{MaxSnapshotBytes: uint64(maxInt)}},
		{"incoming bookkeeping overflow", Options{MaxIncoming: maxInt/4 + 1}},
		{"outgoing bookkeeping overflow", Options{MaxOutgoing: maxInt/2 + 1}},
		{"window byte overflow", Options{ChunkBytes: 2, WindowChunks: maxInt}},
		{"incoming byte product overflow", Options{MaxSnapshotBytes: 1 << 40, MaxIncoming: 1 << 25}},
		{"outgoing byte product overflow", Options{MaxSnapshotBytes: 1 << 40, MaxOutgoing: 1 << 25}},
		{"receive cannot fit one", Options{MaxSnapshotBytes: 1024, MaxReceiveBytes: 1023}},
		{"send cannot fit one", Options{MaxSnapshotBytes: 1024, MaxSendBytes: 1023}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.in.Resolve()
			if !errors.Is(err, fault.ErrInvalid) || got != (Options{}) {
				t.Fatalf("invalid policy returned usable settings: %+v %v", got, err)
			}
			if !errors.Is(tt.in.Validate(), fault.ErrInvalid) {
				t.Fatal("Validate and Resolve disagree")
			}
		})
	}
}

func TestOptionsWireChunkAndArithmeticBoundaries(t *testing.T) {
	t.Parallel()
	if strconv.IntSize == 64 {
		overWire := uint64(math.MaxUint32) + 1
		_, err := (Options{ChunkBytes: int(overWire)}).Resolve()
		if !errors.Is(err, fault.ErrInvalid) || !strings.Contains(err.Error(), "chunk") {
			t.Fatal("chunk cannot be encoded as uint32", err)
		}
		for _, input := range []Options{
			{MaxSnapshotBytes: 1 << 40, MaxIncoming: 1 << 25},
			{MaxSnapshotBytes: 1 << 40, MaxOutgoing: 1 << 25},
		} {
			_, err := input.Resolve()
			if !errors.Is(err, fault.ErrInvalid) || !strings.Contains(err.Error(), "byte budget overflows") {
				t.Fatal("derived byte budget overflow must be detected", err)
			}
		}
	}
	maxInt := int(^uint(0) >> 1)
	input := Options{MaxSnapshotBytes: uint64(maxInt - MaxNativeHeaderBytes - 64),
		TotalTimeout: time.Duration(math.MaxInt64) - time.Minute, ProgressTimeout: time.Minute}
	if err := input.Validate(); err != nil {
		t.Fatal("last representable snapshot/deadline rejected", err)
	}
	input.MaxSnapshotBytes++
	if !errors.Is(input.Validate(), fault.ErrInvalid) {
		t.Fatal("native frame headroom was not enforced")
	}
}
