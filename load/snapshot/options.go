// Package snapshot defines the transport-facing contracts for Node-owned load
// snapshot transfers. It does not export application snapshots or own an
// independent lifecycle.
package snapshot

import (
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

// MaxNativeHeaderBytes bounds the native Raft message header without its snapshot
// payload. Individual transport frames must also fit the adapter's wire limit.
const MaxNativeHeaderBytes = 64 << 10

const (
	defaultMaxSnapshotBytes  uint64 = 256 << 20
	defaultChunkBytes               = 1 << 20
	defaultWindowChunks             = 8
	defaultTotalTimeout             = 30 * time.Minute
	defaultProgressTimeout          = time.Minute
	incomingBookkeepingScale        = 4
	outgoingBookkeepingScale        = 2
)

// Options configures local snapshot transfer admission and deadlines. Zero fields
// select defaults. These limits account for transfer payload ownership, not total
// process RSS, application state or the engine's resident snapshots.
type Options struct {
	// MaxSnapshotBytes bounds the complete encoded Snapshot.Data, not raw
	// application bytes. Zero selects 256 MiB; larger values are configurable.
	MaxSnapshotBytes uint64
	// MaxIncoming and MaxOutgoing default to one each, shared across groups.
	MaxIncoming, MaxOutgoing int
	// Byte budgets default to MaxSnapshotBytes times the directional count.
	// Each must accommodate at least one maximum-sized snapshot.
	MaxReceiveBytes, MaxSendBytes uint64
	// ChunkBytes defaults to 1 MiB. Transport frame bounds may negotiate a
	// smaller payload. WindowChunks defaults to eight unacknowledged chunks.
	ChunkBytes, WindowChunks int
	// TotalTimeout defaults to 30 minutes; ProgressTimeout defaults to one
	// minute without verified progress, including disconnection. Reconnects
	// do not reset either transfer clock.
	TotalTimeout, ProgressTimeout time.Duration
}

// Resolve validates and returns a copy with zero values replaced by defaults.
// It performs no I/O, callbacks, worker creation or payload allocation.
func (o Options) Resolve() (Options, error) {
	if o.MaxIncoming < 0 || o.MaxOutgoing < 0 || o.ChunkBytes < 0 || o.WindowChunks < 0 ||
		o.TotalTimeout < 0 || o.ProgressTimeout < 0 {
		return Options{}, invalidOptions("counts, chunk sizes and durations must be nonnegative")
	}
	o = o.defaults()
	if err := o.validateArithmetic(); err != nil {
		return Options{}, err
	}
	if o.MaxReceiveBytes == 0 {
		if uint64(o.MaxIncoming) > math.MaxUint64/o.MaxSnapshotBytes {
			return Options{}, invalidOptions("incoming byte budget overflows")
		}
		o.MaxReceiveBytes = o.MaxSnapshotBytes * uint64(o.MaxIncoming)
	}
	if o.MaxSendBytes == 0 {
		if uint64(o.MaxOutgoing) > math.MaxUint64/o.MaxSnapshotBytes {
			return Options{}, invalidOptions("outgoing byte budget overflows")
		}
		o.MaxSendBytes = o.MaxSnapshotBytes * uint64(o.MaxOutgoing)
	}
	if o.MaxReceiveBytes < o.MaxSnapshotBytes || o.MaxSendBytes < o.MaxSnapshotBytes {
		return Options{}, invalidOptions("directional byte budgets must accommodate one maximum snapshot")
	}
	return o, nil
}

// Validate checks options with the same zero-value defaults as Resolve.
func (o Options) Validate() error {
	_, err := o.Resolve()
	return err
}

func (o Options) defaults() Options {
	if o.MaxSnapshotBytes == 0 {
		o.MaxSnapshotBytes = defaultMaxSnapshotBytes
	}
	if o.MaxIncoming == 0 {
		o.MaxIncoming = 1
	}
	if o.MaxOutgoing == 0 {
		o.MaxOutgoing = 1
	}
	if o.ChunkBytes == 0 {
		o.ChunkBytes = defaultChunkBytes
	}
	if o.WindowChunks == 0 {
		o.WindowChunks = defaultWindowChunks
	}
	if o.TotalTimeout == 0 {
		o.TotalTimeout = defaultTotalTimeout
	}
	if o.ProgressTimeout == 0 {
		o.ProgressTimeout = defaultProgressTimeout
	}
	return o
}

func (o Options) validateArithmetic() error {
	maxInt := int(^uint(0) >> 1)
	// Native/protobuf length fields and the retained snapshot wrapper need
	// headroom beyond the payload. This is a platform bound, not a 2 GB cap.
	const framingHeadroom = MaxNativeHeaderBytes + 64
	if o.MaxSnapshotBytes > uint64(maxInt-framingHeadroom) {
		return invalidOptions("snapshot and native framing exceed platform allocation size")
	}
	// The receiver has two attachment slots and four completed-cache entries
	// per configured incoming slot; bookkeeping totals must also fit int.
	if o.MaxIncoming > maxInt/incomingBookkeepingScale || o.MaxOutgoing > maxInt/outgoingBookkeepingScale {
		return invalidOptions("concurrency bookkeeping overflows")
	}
	chunkBytes := o.ChunkBytes
	if chunkBytes <= 0 || o.WindowChunks <= 0 {
		return invalidOptions("resolved chunk and window must be positive")
	}
	if o.WindowChunks > maxInt/chunkBytes || uint64(chunkBytes) > math.MaxUint32 {
		return invalidOptions("chunk or window bytes exceed representable size")
	}
	if o.ProgressTimeout > o.TotalTimeout || o.TotalTimeout > time.Duration(math.MaxInt64)-o.ProgressTimeout {
		return invalidOptions("progress timeout exceeds total or combined deadlines overflow")
	}
	return nil
}

func invalidOptions(message string) error {
	return fault.New(fault.Invalid, "snapshot transfer options: "+message, nil)
}
