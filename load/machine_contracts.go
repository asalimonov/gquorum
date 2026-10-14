package load

import (
	"context"
)

// StateMachine calls are serialized. Apply errors halt the replica;
// deterministic
// business rejections belong in Result.Code. Restore(nil) initializes empty
// state. Snapshot must contain the complete, nonempty application
// representation. Recovery restores the snapshot and committed tail, regardless
// of applied hints. Implementations must honor their engine-owned contexts and
// deduplicate commands by application identity where retrying requests can
// repeat a committed effect.
type StateMachine interface {
	Apply(context.Context, Command) (Result, error)
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}

// QueryMachine is an optional read-only extension. Query is serialized with
// Apply, Snapshot and Restore, must honor cancellation, and must not wait for
// its own engine or shutdown. Query errors fail only the read.
type QueryMachine interface {
	Query(context.Context, []byte) (Result, error)
}

// ProposalStats reports the local proposal limit and pending-work counts.
type ProposalStats struct{ Limit, InFlight, Queued, AwaitingCommit int }
