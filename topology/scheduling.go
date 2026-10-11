package topology

import "time"

type SchedulingState string

const (
	SchedulingDisabled SchedulingState = "disabled"
	SchedulingStandby  SchedulingState = "standby"
	SchedulingIdle     SchedulingState = "idle"
	SchedulingWaiting  SchedulingState = "waiting"
	SchedulingBlocked  SchedulingState = "blocked"
	SchedulingRunning  SchedulingState = "running"
	SchedulingError    SchedulingState = "error"
)

type SchedulingActionStatus struct {
	Kind      string `json:"kind"`
	GroupID   string `json:"group_id,omitempty"`
	NodeName  string `json:"node_name,omitempty"`
	ReplicaID uint64 `json:"replica_id,omitempty"`
}
type SchedulingStatus struct {
	Enabled           bool                    `json:"enabled"`
	IsLeader          bool                    `json:"is_leader"`
	State             SchedulingState         `json:"state"`
	Reason            string                  `json:"reason"`
	Policy            string                  `json:"policy"`
	Since             time.Time               `json:"since"`
	BlockingVersion   *uint64                 `json:"blocking_version,omitempty"`
	CurrentOperatorID *OperatorID             `json:"current_operator_id,omitempty"`
	CurrentAction     *SchedulingActionStatus `json:"current_action,omitempty"`
}

func (s SchedulingStatus) Clone() SchedulingStatus {
	if s.BlockingVersion != nil {
		s.BlockingVersion = valuePtr(*s.BlockingVersion)
	}
	if s.CurrentOperatorID != nil {
		s.CurrentOperatorID = valuePtr(*s.CurrentOperatorID)
	}
	if s.CurrentAction != nil {
		s.CurrentAction = valuePtr(*s.CurrentAction)
	}
	return s
}
