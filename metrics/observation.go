package metrics

import "time"

// ReplicaObservation combines a received sample with observer-derived health
// evidence.
type ReplicaObservation struct {
	SeriesID       string        `json:"series_id"`
	Sequence       uint64        `json:"sequence"` // observer history cursor, not source seq
	ReceivedAt     time.Time     `json:"received_at"`
	State          Validity      `json:"state"`
	Sample         ReplicaSample `json:"sample"`
	Reachability   string        `json:"reachability"` // alive, unreachable, unknown
	ReplicationLag UnsignedValue `json:"replication_lag"`
	ApplyBacklog   UnsignedValue `json:"apply_backlog"`
	Evidence       Evidence      `json:"evidence"`
}

// GroupObservation summarizes observed membership and replica health for one
// group.
type GroupObservation struct {
	GroupID           string               `json:"group_id"`
	SourceVersion     uint64               `json:"source_version"`
	LeaderID          uint64               `json:"leader_id"`
	Term              uint64               `json:"term"`
	MembershipDigest  string               `json:"membership_digest"`
	State             Validity             `json:"state"`
	TargetVoters      int                  `json:"target_voters"`
	NativeVoters      int                  `json:"native_voters"`
	EligibleStandbys  int                  `json:"eligible_standbys"`
	Shortfall         int                  `json:"shortfall"`
	CooldownRemaining time.Duration        `json:"cooldown_remaining_ns"`
	Replicas          []ReplicaObservation `json:"replicas"`
}

// ClusterSnapshot contains owned local and collected group observations.
type ClusterSnapshot struct {
	ObserverNodeName   string               `json:"observer_node_name"`
	ObserverInstanceID string               `json:"observer_instance_id"`
	CollectorRunID     string               `json:"collector_run_id"`
	At                 time.Time            `json:"at"`
	ActiveVersion      uint64               `json:"active_version"`
	TargetVersion      uint64               `json:"target_version"`
	IsCollectorLeader  bool                 `json:"is_collector_leader"`
	Local              []ReplicaObservation `json:"local"`
	Groups             []GroupObservation   `json:"groups"`
}
