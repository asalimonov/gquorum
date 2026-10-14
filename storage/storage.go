// Package storage defines durable Raft state independent of application state
// machines.
package storage

import (
	"go.etcd.io/raft/v3/raftpb"
)

// Identity names an exact node-owned Raft replica within a cluster.
type Identity struct {
	NodeName  string `json:"node_name"`
	ClusterID string `json:"cluster_id"`
	GroupID   string `json:"group_id"`
	ReplicaID uint64 `json:"replica_id"`
}

// Probe reports retained group identity and whether Raft log evidence exists.
type Probe struct {
	Exists, HasLog bool
	Identity       Identity
}

// ReadyWrite contains the entries and hard state from one Raft Ready batch.
type ReadyWrite struct {
	Entries   []raftpb.Entry
	HardState raftpb.HardState
}
