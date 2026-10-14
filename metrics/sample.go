package metrics

import "time"

// PeerProgress describes a peer's observed replication and acknowledgement
// progress.
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

// ReplicaSample combines local engine, persistence and proposal measurements.
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
