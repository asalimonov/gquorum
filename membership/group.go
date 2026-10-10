// Package membership enforces fixed membership safety for meta and load Raft groups.
package membership

import (
	"context"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftGroup interface {
	GroupID() string
	IsLeader() bool
	Status() raft.Status
	ProposeConfChangeV2(ctx context.Context, cc raftpb.ConfChangeV2) error
	TransferLeader(ctx context.Context, targetReplicaID uint64) error
}

type HealthSource interface {
	Live(groupID string) (replicaIDs map[uint64]bool, known bool)
}

func singleChange(t raftpb.ConfChangeType, replicaID uint64) raftpb.ConfChangeV2 {
	return raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes:    []raftpb.ConfChangeSingle{{Type: t, NodeID: replicaID}},
	}
}
