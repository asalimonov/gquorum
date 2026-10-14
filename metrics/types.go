// Package metrics exposes owned, process-local observations. Measurements are
// diagnostics, never membership authority or linearizable reads.
package metrics

import "time"

// Validity describes whether a measurement is known and current.
type Validity string

// Measurement validity distinguishes unavailable, current and stale
// observations.
const (
	Unknown Validity = "unknown"
	Current Validity = "current"
	Stale   Validity = "stale"
)

// ReplicaIdentity identifies an exact replica, process incarnation and engine
// run.
type ReplicaIdentity struct {
	ClusterID  string `json:"cluster_id"`
	GroupID    string `json:"group_id"`
	NodeName   string `json:"node_name"`
	ReplicaID  uint64 `json:"replica_id"`
	InstanceID string `json:"instance_id"`
	RunID      string `json:"run_id"`
}

// Stamp records a measurement's sequence, age and validity.
type Stamp struct {
	Sequence uint64        `json:"sequence"`
	Age      time.Duration `json:"age_ns"`
	State    Validity      `json:"state"`
}
