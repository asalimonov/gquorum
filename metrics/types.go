// Package metrics exposes owned, process-local observations. Measurements are
// diagnostics, never membership authority or linearizable reads.
package metrics

import "time"

type Validity string

const (
	Unknown Validity = "unknown"
	Current Validity = "current"
	Stale   Validity = "stale"
)

type ReplicaIdentity struct {
	ClusterID  string `json:"cluster_id"`
	GroupID    string `json:"group_id"`
	NodeName   string `json:"node_name"`
	ReplicaID  uint64 `json:"replica_id"`
	InstanceID string `json:"instance_id"`
	RunID      string `json:"run_id"`
}
type Stamp struct {
	Sequence uint64        `json:"sequence"`
	Age      time.Duration `json:"age_ns"`
	State    Validity      `json:"state"`
}
type Latency struct {
	Count   uint64        `json:"count"`
	Sum     time.Duration `json:"sum_ns"`
	EWMA    time.Duration `json:"ewma_ns"`
	LastAge time.Duration `json:"last_age_ns"`
	State   Validity      `json:"state"`
}
type Counters struct {
	Proposals     uint64 `json:"proposals"`
	Results       uint64 `json:"results"`
	Timeouts      uint64 `json:"timeouts"`
	Rejected      uint64 `json:"rejected"`
	WALRetries    uint64 `json:"wal_retries"`
	SendErrors    uint64 `json:"send_errors"`
	LeaderChanges uint64 `json:"leader_changes"`
}
type Rates struct {
	State     Validity      `json:"state"`
	Window    time.Duration `json:"window_ns"`
	Proposals float64       `json:"proposals_per_second"`
	Results   float64       `json:"results_per_second"`
	Timeouts  float64       `json:"timeouts_per_second"`
	Rejected  float64       `json:"rejected_per_second"`
}
type PeerProgress struct {
	ReplicaID    uint64        `json:"replica_id"`
	Match        uint64        `json:"match"`
	RecentActive bool          `json:"recent_active"`
	AckSequence  uint64        `json:"ack_sequence"`
	AckAge       time.Duration `json:"ack_age_ns"`
	AckKnown     bool          `json:"ack_known"`
}

// ReadQueue describes one owner-loop sample of bounded read work. It has the
// enclosing Raft stamp's age and validity; it is never a read barrier.
type ReadQueue struct {
	Limit          int `json:"limit"`
	InFlight       int `json:"in_flight"`
	Queued         int `json:"queued"`
	AwaitingQuorum int `json:"awaiting_quorum"`
	AwaitingApply  int `json:"awaiting_apply"`
	LocalRounds    int `json:"local_rounds"`
	RemoteRounds   int `json:"remote_rounds"`
}

type ReplicaSample struct {
	ReadQueue          ReadQueue       `json:"read_queue"`
	Identity           ReplicaIdentity `json:"identity"`
	At                 time.Time       `json:"at"`
	Meter              Stamp           `json:"meter"`
	Raft               Stamp           `json:"raft"`
	Writer             Stamp           `json:"writer"`
	Ready              bool            `json:"ready"`
	Running            bool            `json:"running"`
	Restoring          bool            `json:"restoring"`
	LeaderID           uint64          `json:"leader_id"`
	Term               uint64          `json:"term"`
	Commit             uint64          `json:"commit"`
	Applied            uint64          `json:"applied"`
	Voters             []uint64        `json:"voters"`
	Learners           []uint64        `json:"learners"`
	MembershipDigest   string          `json:"membership_digest"`
	Progress           []PeerProgress  `json:"progress,omitempty"`
	Pending            int             `json:"pending"`
	Queued             int             `json:"queued"`
	ProposalLimit      int             `json:"proposal_limit"`
	OutboundQueued     int             `json:"outbound_queued"`
	OutboundLimit      int             `json:"outbound_limit"`
	WALStall           time.Duration   `json:"wal_stall_ns"`
	Counters           Counters        `json:"counters"`
	Rates              Rates           `json:"rates"`
	ResultLatency      Latency         `json:"result_latency"`
	PersistenceLatency Latency         `json:"persistence_latency"`
	ApplyLatency       Latency         `json:"apply_latency"`
}
type UnsignedValue struct {
	State Validity `json:"state"`
	Value uint64   `json:"value"`
}
type DurationValue struct {
	State Validity      `json:"state"`
	Value time.Duration `json:"value_ns"`
}
type Evidence struct {
	UnavailableFor            DurationValue `json:"unavailable_for"`
	ReplicationLagFor         DurationValue `json:"replication_lag_for"`
	NoProgressFor             DurationValue `json:"no_progress_for"`
	StandbyReadyFor           DurationValue `json:"standby_ready_for"`
	DemotionCooldownRemaining time.Duration `json:"demotion_cooldown_remaining_ns"`
	Reasons                   []string      `json:"reasons"` // bounded library vocabulary
}
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
type GroupObservation struct {
	GroupID           string               `json:"group_id"`
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
type ClusterSnapshot struct {
	ObserverNodeName   string               `json:"observer_node_name"`
	ObserverInstanceID string               `json:"observer_instance_id"`
	CollectorRunID     string               `json:"collector_run_id"`
	At                 time.Time            `json:"at"`
	ActiveVersion      uint64               `json:"active_version"`
	IsCollectorLeader  bool                 `json:"is_collector_leader"`
	Local              []ReplicaObservation `json:"local"`
	Groups             []GroupObservation   `json:"groups"`
}
type HistoryQuery struct {
	SeriesID string `json:"series_id"`
	After    uint64 `json:"after"`
	Limit    int    `json:"limit"`
}
type HistoryPage struct {
	SeriesID  string               `json:"series_id"`
	Points    []ReplicaObservation `json:"points"`
	Next      uint64               `json:"next"`
	Truncated bool                 `json:"truncated"`
}
