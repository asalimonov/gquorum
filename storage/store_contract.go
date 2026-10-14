package storage

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

// Recovery contains the full snapshot, retained tail and persisted application
// hints.
type Recovery struct {
	Snapshot         raftpb.Snapshot
	Entries          []raftpb.Entry
	HardState        raftpb.HardState
	AppliedHint      uint64
	AppliedConfState raftpb.ConfState
}

// Store persists one owner's Raft replicas independently of application state.
// Recovery restores the full snapshot and committed tail; applied hints do not
// authorize skipping application replay. The component supplying the store
// controls its lifetime unless it explicitly transfers ownership.
type Store interface {
	// Inventory and identity. These reads must preserve retained evidence.

	// HasState reports any retained replica, pin, partial acquisition, or
	// retired resource in this store, including groups absent from the metadata
	// catalog.
	// Return an error when absence cannot be established. This is read-only and
	// must not create, repair or discard evidence. Owners exclude concurrent
	// use while deciding whether it is safe to initialize a fresh authority
	// database.
	HasState(context.Context) (bool, error)
	// BindReplica binds an exact durable identity before replica state is used.
	BindReplica(context.Context, Identity) error
	// Probe inspects a group without creating or repairing its state.
	Probe(context.Context, string) (Probe, error)

	// Active Raft persistence. Ready writes precede publication of committed
	// work.

	// Ensure prepares storage for an already bound group.
	Ensure(context.Context, string) error
	// SaveReady persists entries and hard state from one Raft Ready batch.
	SaveReady(context.Context, string, ReadyWrite) error
	// SaveApplied records the application-applied index and configuration
	// state.
	SaveApplied(context.Context, string, uint64, raftpb.ConfState) error
	// SaveSnapshot persists a complete Raft snapshot.
	SaveSnapshot(context.Context, string, raftpb.Snapshot) error
	// Load returns retained Raft state for replay.
	Load(context.Context, string) (Recovery, error)
	// Compact discards log entries covered by durable state through an index.
	Compact(context.Context, string, uint64) error

	// Retirement and store lifetime.

	// Quarantine removes an exact replica from active use and returns its
	// token.
	Quarantine(context.Context, string, uint64) (string, error)
	// Close releases resources owned by the store implementation.
	Close() error
}

// Stats reports retained log and snapshot sizes and the snapshot index.
type Stats struct {
	LogBytes, SnapshotBytes int64
	SnapshotIndex           uint64
}
