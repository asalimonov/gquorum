package topology

import (
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// GroupStatus records observed Raft membership, leadership, progress and voter
// liveness.
type GroupStatus struct {
	Reporter         Identity                   `json:"reporter"`
	ReceivedAt       time.Time                  `json:"received_at"`
	SourceVersion    uint64                     `json:"source_version"`
	ConvergedVersion uint64                     `json:"converged_version"`
	OperationID      OperationID                `json:"operation_id,omitempty"`
	GroupID          string                     `json:"group_id"`
	HostNode         string                     `json:"host_node"`
	Found            bool                       `json:"found"`
	LeaderID         uint64                     `json:"leader_id"`
	Term             uint64                     `json:"term"`
	Commit           uint64                     `json:"commit"`
	Applied          uint64                     `json:"applied"`
	ConfState        raftpb.ConfState           `json:"conf_state"`
	Progress         map[uint64]ReplicaProgress `json:"progress,omitempty"`
	LiveVoters       map[uint64]bool            `json:"live_voters,omitempty"`
	HealthKnown      bool                       `json:"health_known"`
	ErrorCode        string                     `json:"error_code,omitempty"`
	// LeaderNodeName is resolved from LeaderID through the captured Active view
	// in cluster status, local meta status and scheduler inputs. In those
	// views,
	// empty means leadership is unknown or the replica is absent from Active.
	// Raw replica status does not resolve node names; use LeaderID there.
	LeaderNodeName string `json:"leader_node_name"`
}

// NodeStatus combines a node declaration with its effective state, health and
// endpoint.
type NodeStatus struct {
	Node           Node      `json:"node"`
	EffectiveState NodeState `json:"effective_state"`
	Health         Health    `json:"health"`
	Address        string    `json:"address,omitempty"`
}

// ClusterStatus combines captured topology versions with local and peer
// observations.
type ClusterStatus struct {
	FullyConverged      FullyConverged    `json:"fully_converged"`
	Operations          []OperationStatus `json:"operations"`
	OperationsTruncated bool              `json:"operations_truncated"`
	Cleanup             CleanupSummary    `json:"cleanup"`
	Lifecycle           string            `json:"lifecycle"`
	ErrorCode           string            `json:"error_code,omitempty"`
	LatestVersion       uint64            `json:"latest_version"`
	TargetVersion       uint64            `json:"target_version"`
	ActiveVersion       uint64            `json:"active_version"`
	Nodes               []NodeStatus      `json:"nodes"`
	Groups              []GroupStatus     `json:"groups"`
	MetaLeaderID        uint64            `json:"meta_leader_id"`
	// MetaLeaderNodeName is resolved from the numeric MetaLeaderID through
	// Active.
	// Empty means leadership is unknown or that member is absent from Active.
	MetaLeaderNodeName string            `json:"meta_leader_node_name"`
	SessionLive        bool              `json:"session_live"`
	Operator           *OperatorStatus   `json:"operator,omitempty"`
	Scheduling         SchedulingStatus  `json:"scheduling"`
	Observation        HealthObservation `json:"observation"`
	// Placement is a static report for the captured Active and Target
	// documents.
	// It is not a native health or quorum observation.
	Placement   []PlacementReport  `json:"placement,omitempty"`
	SelfRemoval *SelfRemovalStatus `json:"self_removal,omitempty"`
}
