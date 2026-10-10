package topology

import "time"

type ReconciliationState string

const (
	ReconciliationDisabled ReconciliationState = "disabled"
	ReconciliationStandby  ReconciliationState = "standby"
	ReconciliationIdle     ReconciliationState = "idle"
	ReconciliationWaiting  ReconciliationState = "waiting"
	ReconciliationBlocked  ReconciliationState = "blocked"
	ReconciliationRunning  ReconciliationState = "running"
	ReconciliationError    ReconciliationState = "error"
)

type ReconciliationActionStatus struct {
	Kind      string `json:"kind"`
	GroupID   string `json:"group_id,omitempty"`
	NodeName  string `json:"node_name,omitempty"`
	ReplicaID uint64 `json:"replica_id,omitempty"`
}
type ReconciliationStatus struct {
	Enabled            bool                        `json:"enabled"`
	IsLeader           bool                        `json:"is_leader"`
	State              ReconciliationState         `json:"state"`
	Reason             string                      `json:"reason"`
	Policy             string                      `json:"policy"`
	Since              time.Time                   `json:"since"`
	BlockingVersion    *uint64                     `json:"blocking_version,omitempty"`
	CurrentExecutionID *ExecutionID                `json:"current_execution_id,omitempty"`
	CurrentAction      *ReconciliationActionStatus `json:"current_action,omitempty"`
}

func (s ReconciliationStatus) Clone() ReconciliationStatus {
	if s.BlockingVersion != nil {
		s.BlockingVersion = valuePtr(*s.BlockingVersion)
	}
	if s.CurrentExecutionID != nil {
		s.CurrentExecutionID = valuePtr(*s.CurrentExecutionID)
	}
	if s.CurrentAction != nil {
		s.CurrentAction = valuePtr(*s.CurrentAction)
	}
	return s
}
