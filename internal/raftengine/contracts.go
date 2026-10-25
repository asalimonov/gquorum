//revive:disable:max-public-structs // Internal engine extension contracts are kept together.
package raftengine

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

// Machine operations are serialized. Apply failures halt the replica; business
// rejection should return a deterministic Result.Code. Restore(nil) initializes
// empty state. Snapshot must return a complete, nonempty representation.
type Machine interface {
	Apply(context.Context, Command) (Result, error)
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}

// AppliedObserver is an internal integration seam for materialized meta state.
// The engine invokes it after the committed batch and before Advance/messages.
type AppliedObserver interface {
	AfterApply(context.Context, uint64, uint64, raftpb.ConfState) error
}

// Transport owns routing and identity validation. Send receives owned copies;
// it must honor cancellation. Calls for different destinations can run
// concurrently; calls for a single destination are serialized. Asynchronous
// transports report later failures
// through ReportUnreachable and ReportSnapshot.
type Transport interface {
	Send(context.Context, []raftpb.Message) error
}

// SnapshotJob owns one admitted bulk attempt and its terminal report.
// Discard releases an unstarted job and must be idempotent and nonblocking.
// Run joins all work before returning, including after cancellation.
type SnapshotJob interface {
	Run(context.Context)
	Discard(error)
}

// SnapshotJobTransport optionally admits jobs without I/O, hashing or copying
// payloads on the Raft owner. Only managed load adapters opt into this seam.
type SnapshotJobTransport interface {
	PrepareSnapshot(SnapshotOffer) (SnapshotJob, error)
}
