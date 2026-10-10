// Package storage defines durable Raft state independent of application state machines.
package storage

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

type Identity struct {
	NodeName  string `json:"node_name"`
	ClusterID string `json:"cluster_id"`
	GroupID   string `json:"group_id"`
	ReplicaID uint64 `json:"replica_id"`
}
type Probe struct {
	Exists, HasLog bool
	Identity       Identity
}
type ReadyWrite struct {
	Entries   []raftpb.Entry
	HardState raftpb.HardState
}
type Recovery struct {
	Snapshot         raftpb.Snapshot
	Entries          []raftpb.Entry
	HardState        raftpb.HardState
	AppliedHint      uint64
	AppliedConfState raftpb.ConfState
}

// Store persists Raft state; application recovery always restores the full snapshot and committed tail.
type Store interface {
	BindReplica(context.Context, Identity) error
	Probe(context.Context, string) (Probe, error)
	Ensure(context.Context, string) error
	SaveReady(context.Context, string, ReadyWrite) error
	SaveApplied(context.Context, string, uint64, raftpb.ConfState) error
	SaveSnapshot(context.Context, string, raftpb.Snapshot) error
	Load(context.Context, string) (Recovery, error)
	Compact(context.Context, string, uint64) error
	Quarantine(context.Context, string, uint64) (string, error)
	Close() error
}

type Stats struct {
	LogBytes, SnapshotBytes int64
	SnapshotIndex           uint64
}
