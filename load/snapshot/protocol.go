//revive:disable:max-public-structs Keep the wire and transport value contracts together.
package snapshot

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/load"
)

// Key binds a delivery to its exact source and destination replicas and node
// incarnations. A group name alone never identifies a resumable transfer.
type Key struct {
	From, To   load.Peer
	DeliveryID uint64
}

// Manifest is immutable for one delivery. NativeHeader is the binary native
// MsgSnap with Snapshot.Data cleared; SHA256 covers complete encoded Data.
// ChunkBytes is the original requested size, not the negotiated payload size.
// Retaining a manifest requires an owned copy of its header.
type Manifest struct {
	Key          Key
	NativeHeader []byte
	TotalBytes   uint64
	SHA256       [32]byte
	ChunkBytes   uint32
}

// Session identifies one receiver-issued attachment generation. An older
// attachment cannot write, detach or abort a replacement attachment.
type Session struct {
	Key        Key
	Attachment uint64
}

// BeginRequest creates or resumes a transfer. Relative durations may only narrow
// receiver policy; resume never renews stored deadlines or changes the manifest.
type BeginRequest struct {
	Manifest        Manifest
	RemainingTotal  time.Duration
	ProgressTimeout time.Duration
	Resume          bool
}

// Chunk carries one complete, ordered payload fragment. Receivers copy Data
// before returning; an exact duplicate does not advance useful progress.
type Chunk struct {
	Session Session
	Ordinal uint64
	Offset  uint64
	Data    []byte
	SHA256  [32]byte
}

// Ack reports a verified cumulative offset and negotiated policy. Accepted is
// native ingress acceptance, not durable installation. NotNeeded is an exact
// owner-confirmed native skip and does not claim receipt of the payload.
// The zero value proves neither acceptance nor installation.
type Ack struct {
	Session         Session
	NextOffset      uint64
	ChunkBytes      uint32
	State           DeliveryState
	RestartNeeded   bool
	RemainingTotal  time.Duration
	ProgressTimeout time.Duration
	ReceiverRunID   string
}

// Send borrows an immutable pinned payload for the duration of SendSnapshot.
// The adapter must join its workers before returning and must not retain or
// mutate Data afterward. ReportProgress is a nonblocking library callback for
// authenticated cumulative acknowledgements; it may be nil for direct callers.
type Send struct {
	Manifest       Manifest
	Data           []byte
	ReportProgress func(Progress)
}

// Progress reports only verified acknowledgement evidence. Duplicate offsets
// do not refresh progress. The owner stamps receipt time locally and derives
// repair-episode state; callback input cannot extend an episode deadline.
type Progress struct {
	ReceiverRunID   string
	VerifiedBytes   uint64
	State           DeliveryState
	RemainingTotal  time.Duration
	ProgressTimeout time.Duration
}

// Transport is the additional capability required for managed load snapshots.
// SendSnapshot joins its workers and returns native ingress acceptance or a
// terminal error. It can run alongside ordinary Raft sends to the same peer.
// It must not call ReportLoadDelivery itself: the Node-owned job reports once.
// ReportProgress cannot be called after SendSnapshot returns.
type Transport interface {
	SendSnapshot(context.Context, Send) (Ack, error)
}
