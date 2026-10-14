// Package replicastate holds the process-local observations shared by the load
// facade and its internal engine. It is not a persisted or replicated encoding.
package replicastate

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

// State describes the observed lifecycle of one local replica run.
type State string

// Replica states describe startup, readiness, teardown and terminal failure.
const (
	Starting State = "starting"
	Ready    State = "ready"
	Stopping State = "stopping"
	Stopped  State = "stopped"
	Failed   State = "failed"
)

// Observation describes one exact engine run. Revision gaps mean coalescing;
// Ready, leadership, term and membership are observations, never quorum proof.
// AppliedIndex is the local applied index of this run; it changes, and bumps
// Revision, whenever committed entries or a snapshot are applied.
type Observation struct {
	Identity   storage.Identity `json:"identity"`
	InstanceID string           `json:"instance_id"`
	RunID      string           `json:"run_id"`
	Revision   uint64           `json:"revision"`
	State      State            `json:"state"`
	Ready      bool             `json:"ready"`
	IsLeader   bool             `json:"is_leader"`
	IsVoter    bool             `json:"is_voter"`
	LeaderID   uint64           `json:"leader_id"`
	Term       uint64           `json:"term"`
	// AppliedIndex is the highest log index applied by this local replica.
	AppliedIndex uint64     `json:"applied_index"`
	ErrorCode    fault.Code `json:"error_code,omitempty"`
	Error        string     `json:"error,omitempty"`
}
