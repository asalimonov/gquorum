package topology

import "time"

// SchedulingState describes a local repair scheduler's observed activity.
type SchedulingState string

// Scheduling states distinguish disabled, standby, idle, waiting, blocked,
// running and failed work.
const (
	SchedulingDisabled SchedulingState = "disabled"
	SchedulingStandby  SchedulingState = "standby"
	SchedulingIdle     SchedulingState = "idle"
	SchedulingWaiting  SchedulingState = "waiting"
	SchedulingBlocked  SchedulingState = "blocked"
	SchedulingRunning  SchedulingState = "running"
	SchedulingError    SchedulingState = "error"
)

// SchedulingActionStatus identifies the scheduler's current repair subject.
type SchedulingActionStatus struct {
	Kind      string `json:"kind"`
	GroupID   string `json:"group_id,omitempty"`
	NodeName  string `json:"node_name,omitempty"`
	ReplicaID uint64 `json:"replica_id,omitempty"`
}

// SchedulingStatus reports local policy activity, blocking intent and current
// execution.
type SchedulingStatus struct {
	Enabled            bool                    `json:"enabled"`
	IsLeader           bool                    `json:"is_leader"`
	State              SchedulingState         `json:"state"`
	Reason             string                  `json:"reason"`
	RepairPolicy       string                  `json:"repair_policy"`
	Since              time.Time               `json:"since"`
	BlockingVersion    *uint64                 `json:"blocking_version,omitempty"`
	CurrentOperationID *OperationID            `json:"current_operation_id,omitempty"`
	CurrentAction      *SchedulingActionStatus `json:"current_action,omitempty"`
}

// Clone copies optional version, operator and action observations.
func (s SchedulingStatus) Clone() SchedulingStatus {
	if s.BlockingVersion != nil {
		s.BlockingVersion = valuePtr(*s.BlockingVersion)
	}
	if s.CurrentOperationID != nil {
		s.CurrentOperationID = valuePtr(*s.CurrentOperationID)
	}
	if s.CurrentAction != nil {
		s.CurrentAction = valuePtr(*s.CurrentAction)
	}
	return s
}
