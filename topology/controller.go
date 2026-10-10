package topology

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

type Health string

const (
	HealthUnknown Health = "unknown"
	HealthAlive   Health = "alive"
	HealthDead    Health = "dead"
)

type GroupHealth string

const (
	GroupUnknown    GroupHealth = "unknown"
	GroupHealthy    GroupHealth = "healthy"
	GroupDegraded   GroupHealth = "degraded"
	GroupQuorumLost GroupHealth = "quorum_lost"
)

type ReplicaProgress struct {
	Match        uint64 `json:"match"`
	RecentActive bool   `json:"recent_active"`
}
type GroupStatus struct {
	GroupID     string                     `json:"group_id"`
	HostNode    string                     `json:"host_node"`
	Found       bool                       `json:"found"`
	LeaderID    uint64                     `json:"leader_id"`
	Commit      uint64                     `json:"commit"`
	Applied     uint64                     `json:"applied"`
	ConfState   raftpb.ConfState           `json:"conf_state"`
	Progress    map[uint64]ReplicaProgress `json:"progress,omitempty"`
	LiveVoters  map[uint64]bool            `json:"live_voters,omitempty"`
	HealthKnown bool                       `json:"health_known"`
}

type NodeStatus struct {
	Node           Node      `json:"node"`
	EffectiveState NodeState `json:"effective_state"`
	Health         Health    `json:"health"`
	Address        string    `json:"address,omitempty"`
}
type ClusterStatus struct {
	Lifecycle     string           `json:"lifecycle"`
	ErrorCode     string           `json:"error_code,omitempty"`
	LatestVersion uint64           `json:"latest_version"`
	TargetVersion uint64           `json:"target_version"`
	ActiveVersion uint64           `json:"active_version"`
	Nodes         []NodeStatus     `json:"nodes"`
	Groups        []GroupStatus    `json:"groups"`
	MetaLeader    uint64           `json:"meta_leader"`
	SessionLive   bool             `json:"session_live"`
	Execution     *ExecutionStatus `json:"execution,omitempty"`
}

// GroupController implements idempotent effects on the exact authorized load identities.
// Receivers must revalidate authorization and verify existing results before succeeding.
type GroupController interface {
	Status(context.Context, string) (GroupStatus, error)
	CreateGroup(context.Context, AuthorizedStep) error
	PrepareReplica(context.Context, AuthorizedStep) error
	ApplyMembership(context.Context, AuthorizedStep) error
	TransferLeader(context.Context, AuthorizedStep) error
	StopReplica(context.Context, AuthorizedStep) error
	CleanupReplicaState(context.Context, AuthorizedStep) error
	DeleteGroupState(context.Context, AuthorizedStep) error
}

type AddressResolver interface {
	Resolve(context.Context, string) (string, error)
}
type HealthSource interface {
	Snapshot(context.Context) (map[string]Health, error)
}
