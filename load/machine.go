// Package load exposes application Raft replicas without leaking the internal
// engine's lifecycle and membership controls. Node manages cluster replicas;
// Standalone owns an explicitly independent engine.
package load

import (
	"context"
	"time"
)

type Command struct {
	Index, Term uint64
	RequestID   string
	Data        []byte
}
type Result struct {
	Code string
	Data []byte
}
type Proposal struct {
	RequestID string
	Data      []byte
}

// StateMachine calls are serialized. Apply errors halt the replica; deterministic
// business rejections belong in Result.Code. Restore(nil) initializes empty
// state. Snapshot must contain the complete, nonempty application representation.
// Recovery restores the snapshot and committed tail, regardless of applied hints.
// Implementations must honor their engine-owned contexts and deduplicate commands
// by application identity where retrying requests can repeat a committed effect.
type StateMachine interface {
	Apply(context.Context, Command) (Result, error)
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}

type Timing struct {
	TickInterval                  time.Duration
	ElectionTicks, HeartbeatTicks int
	WALWriteWait                  time.Duration
}
type ReadOptions struct {
	Timeout         time.Duration
	MaxPending      int
	MaxRequestBytes int
	MaxResultBytes  int
}

// QueryMachine is an optional read-only extension. Query is serialized with
// Apply, Snapshot and Restore, must honor cancellation, and must not wait for
// its own engine or shutdown. Query errors fail only the read.
type QueryMachine interface {
	Query(context.Context, []byte) (Result, error)
}

type EngineOptions struct {
	Reads                                   ReadOptions
	Timing                                  Timing
	ProposalTimeout                         time.Duration // default 30s; caller's shorter deadline wins
	SnapshotThreshold                       uint64
	MaxPendingProposals                     int
	MaxUncommittedBytes, MaxApplyBatchBytes uint64
}
type ProposalStats struct{ Limit, InFlight, Queued, AwaitingCommit int }
