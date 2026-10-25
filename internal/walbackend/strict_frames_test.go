//go:build !functional

package walbackend

import (
	"errors"
	"io"
	"testing"
)

func TestStrictWALFrameSizeRejectsAmbiguousPadding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		field   uint64
		length  uint64
		padding uint8
		valid   bool
	}{
		{field: 8, length: 8, valid: true},
		{field: 9 | uint64(0x87)<<56, length: 9, padding: 7, valid: true},
		{field: 0},
		{field: 8 | uint64(0x81)<<56},
		{field: 9},
		{field: 8 | uint64(0x88)<<56},
	} {
		frame, err := checkedFrameSize(tc.field)
		if tc.valid && (err != nil || frame.length != tc.length || frame.padding != tc.padding) {
			t.Fatal(tc, frame, err)
		}
		if !tc.valid && err == nil {
			t.Fatal("invalid frame accepted", tc)
		}
	}
}
func TestTornCRCHeuristicIsRestrictedToFinalSegment(t *testing.T) {
	t.Parallel()
	cause := errors.New("CRC mismatch")
	decoder := strictFrames{offset: 64, file: "segment"}
	data := make([]byte, 1024)
	if err := decoder.corruption(data, cause); errors.Is(err, io.EOF) || decoder.torn != nil {
		t.Fatal("earlier zero sector treated as clean EOF", err)
	}
	decoder.final = true
	if err := decoder.corruption(data, cause); !errors.Is(err, io.EOF) || decoder.torn == nil || decoder.torn.Offset != 64 {
		t.Fatal("final torn position lost", err, decoder.torn)
	}
	for i := range data {
		data[i] = 1
	}
	if tornSector(data, 0) {
		t.Fatal("nonzero CRC corruption treated as torn write")
	}
}
