package topology

import (
	"time"
)

// ReplicaRole distinguishes voting members from learners.
type ReplicaRole string

// Replica declares one exact group member on a named node. ReplicaID is
// monotonic within its group and is not the node name or process instance ID.
type Replica struct {
	// Durable member identity and desired voting role.
	NodeName  string      `json:"node_name"`
	ReplicaID uint64      `json:"replica_id"`
	Role      ReplicaRole `json:"role"`
	// PinnedLearner prevents this exact replica from becoming a voter.
	PinnedLearner bool `json:"pinned_learner,omitempty"`
	// Creation and last-declaration-change timestamps.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Group declares one Raft group and its desired replicas. The reserved _meta
// group contains cluster membership; other groups host application loads.
// Labels are descriptive and do not implicitly select nodes.
type Group struct {
	// Group identity, replica assignments and monotonic allocation cursor.
	ID            string    `json:"id"`
	Replicas      []Replica `json:"replicas"`
	NextReplicaID uint64    `json:"next_replica_id"`
	// Placement, voting and explicit hosting declarations.
	Placement   *PlacementRule `json:"placement,omitempty"`
	VoterConfig *VoterConfig   `json:"voter_config,omitempty"`
	Hosting     *HostingRule   `json:"hosting,omitempty"`
	// Descriptive labels, immutable storage class and declaration timestamps.
	Labels    map[string]string `json:"labels,omitempty"`
	Storage   StorageClass      `json:"storage,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}
