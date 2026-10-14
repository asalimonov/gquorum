package topology

import (
	"time"
)

// Operator combines execution status with its immutable plan and edit
// provenance.
type Operator struct {
	Status    OperatorStatus `json:"status"`
	Plan      Plan           `json:"plan"`
	Actor     string         `json:"actor"`
	Origin    Origin         `json:"origin"`
	StartedAt time.Time      `json:"started_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// OperatorTransition records a timestamped execution-state change for an exact
// step.
type OperatorTransition struct {
	OperatorID    OperatorID    `json:"operator_id"`
	Version       uint64        `json:"version"`
	Attempt       uint32        `json:"attempt"`
	StepSeq       int           `json:"step_seq"`
	State         OperatorState `json:"state"`
	GroupID       string        `json:"group_id,omitempty"`
	NodeName      string        `json:"node_name,omitempty"`
	WaitingReason string        `json:"waiting_reason,omitempty"`
	Error         string        `json:"error,omitempty"`
	At            time.Time     `json:"at"`
}
