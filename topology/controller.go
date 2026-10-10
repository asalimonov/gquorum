package topology

import (
	"context"
	"maps"
	"slices"

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
	Term        uint64                     `json:"term"`
	Commit      uint64                     `json:"commit"`
	Applied     uint64                     `json:"applied"`
	ConfState   raftpb.ConfState           `json:"conf_state"`
	Progress    map[uint64]ReplicaProgress `json:"progress,omitempty"`
	LiveVoters  map[uint64]bool            `json:"live_voters,omitempty"`
	HealthKnown bool                       `json:"health_known"`
	ErrorCode   string                     `json:"error_code,omitempty"`
}

func (s GroupStatus) Clone() GroupStatus {
	s.ConfState.Voters = slices.Clone(s.ConfState.Voters)
	s.ConfState.Learners = slices.Clone(s.ConfState.Learners)
	s.ConfState.VotersOutgoing = slices.Clone(s.ConfState.VotersOutgoing)
	s.ConfState.LearnersNext = slices.Clone(s.ConfState.LearnersNext)
	s.Progress = maps.Clone(s.Progress)
	s.LiveVoters = maps.Clone(s.LiveVoters)
	return s
}

type NodeStatus struct {
	Node           Node      `json:"node"`
	EffectiveState NodeState `json:"effective_state"`
	Health         Health    `json:"health"`
	Address        string    `json:"address,omitempty"`
}
type ClusterStatus struct {
	Lifecycle      string               `json:"lifecycle"`
	ErrorCode      string               `json:"error_code,omitempty"`
	LatestVersion  uint64               `json:"latest_version"`
	TargetVersion  uint64               `json:"target_version"`
	ActiveVersion  uint64               `json:"active_version"`
	Nodes          []NodeStatus         `json:"nodes"`
	Groups         []GroupStatus        `json:"groups"`
	MetaLeader     uint64               `json:"meta_leader"`
	SessionLive    bool                 `json:"session_live"`
	Execution      *ExecutionStatus     `json:"execution,omitempty"`
	Reconciliation ReconciliationStatus `json:"reconciliation"`
	Observation    HealthObservation    `json:"observation"`
	SelfRemoval    *SelfRemovalStatus   `json:"self_removal,omitempty"`
}

// GroupController implements idempotent effects on the exact authorized load identities.
// Receivers must revalidate authorization and verify existing results before succeeding.
type GroupController interface {
	Status(context.Context, string) (GroupStatus, error)
	// CreateGroup seeds only the approved founding voters. Intentional learners
	// are prepared and admitted by later exact steps.
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
	// Snapshot returns an owned, coherent node observation. Nil means globally
	// unknown; a missing node or HealthUnknown remains unknown, never dead.
	Snapshot(context.Context) (map[string]Health, error)
}
