package walbackend

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
)

// Framing follows etcd v3.6.12 storage/wal/decoder.go, with bounds before
// allocation and explicit per-segment termination. CRC/torn-sector semantics
// remain compatible; only the final segment can supply a torn-tail candidate.
const walFrameHeaderBytes = 8
const walSectorBytes = 512

var walCRCTable = crc32.MakeTable(crc32.Castagnoli)

type strictFrames struct {
	ctx    context.Context
	limits model.ReadBounds
	crc    uint32
	offset int64
	final  bool
	file   string
	torn   *model.TornTail
}

func (d *strictFrames) next(file *os.File, size int64) (walpb.Record, error) {
	if err := d.ctx.Err(); err != nil {
		return walpb.Record{}, err
	}
	var header [walFrameHeaderBytes]byte
	count, err := io.ReadFull(file, header[:])
	if err != nil {
		if count == 0 && errors.Is(err, io.EOF) {
			return walpb.Record{}, io.EOF
		}
		return walpb.Record{}, d.tornEnd(err)
	}
	field := binary.LittleEndian.Uint64(header[:])
	if field == 0 {
		return walpb.Record{}, d.zeroEnd(file)
	}
	frame, err := checkedFrameSize(field)
	length, padding := frame.length, frame.padding
	if err != nil {
		return walpb.Record{}, err
	}
	if length > math.MaxInt64-walFrameHeaderBytes {
		return walpb.Record{}, fault.ErrUnreadable
	}
	frameBytes := int64(length) + int64(padding)
	if size-d.offset-walFrameHeaderBytes < frameBytes {
		return walpb.Record{}, d.tornEnd(io.ErrUnexpectedEOF)
	}
	if length > d.limits.MaxRecordBytes || length > d.limits.MaxGroupBytes {
		return walpb.Record{}, fault.New(fault.Unreadable, "WAL record exceeds recovery byte budget", nil)
	}
	if length > math.MaxInt-walFrameHeaderBytes {
		return walpb.Record{}, fault.ErrUnreadable
	}
	data := make([]byte, int(frameBytes))
	if _, err := io.ReadFull(file, data); err != nil {
		return walpb.Record{}, d.tornEnd(err)
	}
	record, err := d.decodeFrame(data, length)
	if err != nil {
		return walpb.Record{}, err
	}
	d.offset += walFrameHeaderBytes + frameBytes
	return record, nil
}

// A zero frame is preallocation only when the entire remaining final segment is
// zero. Never hide later records behind a corrupted length field.
func (d *strictFrames) zeroEnd(file *os.File) error {
	if !d.final {
		return unreadable("zero frame before final WAL segment", nil)
	}
	var buffer [64 << 10]byte
	for {
		if err := d.ctx.Err(); err != nil {
			return err
		}
		count, err := file.Read(buffer[:])
		for _, value := range buffer[:count] {
			if value != 0 {
				return unreadable("nonzero data after zero WAL frame", nil)
			}
		}
		if err != nil {
			return err
		}
	}
}
func (d *strictFrames) decodeFrame(data []byte, length uint64) (walpb.Record, error) {
	var record walpb.Record
	if err := record.Unmarshal(data[:length]); err != nil {
		return walpb.Record{}, d.corruption(data, err)
	}
	if record.Type == wal.CrcType {
		if d.crc != 0 && record.Crc != d.crc {
			return walpb.Record{}, wal.ErrCRCMismatch
		}
		d.crc = record.Crc
	} else {
		next := crc32.Update(d.crc, walCRCTable, record.Data)
		if err := record.Validate(next); err != nil {
			return walpb.Record{}, d.corruption(data, err)
		}
		d.crc = next
	}
	return record, nil
}

type frameSize struct {
	length  uint64
	padding uint8
}

func checkedFrameSize(field uint64) (frameSize, error) {
	const lengthBits = 56
	const highMask = uint64(0xf8)
	const paddingMask = uint64(0x7)
	const paddingFlag = uint64(0x80)
	const lengthMask = uint64(1)<<lengthBits - 1
	length := field & lengthMask
	padding := uint8(0)
	high := field >> lengthBits
	if high != 0 {
		if high&highMask != paddingFlag {
			return frameSize{}, fault.ErrUnreadable
		}
		padding = uint8(high & paddingMask)
	}
	if length == 0 || uint64(padding) != (walFrameHeaderBytes-length%walFrameHeaderBytes)%walFrameHeaderBytes {
		return frameSize{}, fault.ErrUnreadable
	}
	return frameSize{length: length, padding: padding}, nil
}

func (d *strictFrames) tornEnd(err error) error {
	if !d.final {
		return unreadable("incomplete record before final WAL segment", err)
	}
	d.torn = &model.TornTail{File: d.file, Offset: d.offset}
	return io.EOF
}
func (d *strictFrames) corruption(data []byte, cause error) error {
	if d.final && tornSector(data, d.offset+walFrameHeaderBytes) {
		return d.tornEnd(cause)
	}
	return unreadable("corrupt WAL record", cause)
}
func tornSector(input []byte, initialOffset int64) bool {
	data, fileOffset := input, initialOffset
	for len(data) > 0 {
		length := min(int(walSectorBytes-fileOffset%walSectorBytes), len(data))
		zero := true
		for _, value := range data[:length] {
			if value != 0 {
				zero = false
				break
			}
		}
		if zero {
			return true
		}
		data = data[length:]
		fileOffset += int64(length)
	}
	return false
}
