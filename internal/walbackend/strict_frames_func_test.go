//go:build functional

package walbackend

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/internal/recovery/model"
)

func TestStrictZeroFrameCannotHideLaterData(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"preallocation", "hidden record", "earlier segment"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			data := make([]byte, 128<<10)
			if shape == "hidden record" {
				data[len(data)-1] = 1
			}
			path := filepath.Join(t.TempDir(), "segment")
			check(t, os.WriteFile(path, data, 0o600))
			file, err := os.Open(path)
			check(t, err)
			defer file.Close()
			d := strictFrames{ctx: t.Context(), final: shape != "earlier segment", limits: offlineLimits()}
			_, err = d.next(file, int64(len(data)))
			if shape == "preallocation" {
				if !errors.Is(err, io.EOF) || d.torn != nil {
					t.Fatal("clean padding refused", err)
				}
			} else if err == nil || errors.Is(err, io.EOF) {
				t.Fatal("zero frame hid later data or an earlier segment", err)
			}
		})
	}
}

func TestStrictOversizedIncompleteFrameChecksSegmentBoundaryBeforeBudget(t *testing.T) {
	t.Parallel()
	for _, final := range []bool{true, false} {
		t.Run(map[bool]string{true: "final", false: "earlier"}[final], func(t *testing.T) {
			t.Parallel()
			var data [16]byte
			binary.LittleEndian.PutUint64(data[:], 1<<40)
			path := filepath.Join(t.TempDir(), "segment")
			check(t, os.WriteFile(path, data[:], 0o600))
			file, err := os.Open(path)
			check(t, err)
			defer file.Close()
			d := strictFrames{ctx: t.Context(), final: final, file: "segment", limits: model.ReadBounds{MaxRecordBytes: 8, MaxGroupBytes: 8}}
			_, err = d.next(file, int64(len(data)))
			if final {
				if !errors.Is(err, io.EOF) || d.torn == nil {
					t.Fatal("final torn header treated as budget violation", err)
				}
			} else if !errors.Is(err, io.ErrUnexpectedEOF) || d.torn != nil {
				t.Fatal("earlier segment accepted or lost truncation cause", err)
			}
		})
	}
}
