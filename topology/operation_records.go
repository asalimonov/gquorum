package topology

import "time"

// Operation combines an attempt's block status, immutable plan and edit provenance.
// Previous retains the original authority of consumed receipts across an exact retry.
type Operation struct {
	Status    OperationStatus `json:"status"`
	Plan      Plan            `json:"plan"`
	Previous  OperationID     `json:"previous,omitempty"`
	Actor     string          `json:"actor"`
	Origin    Origin          `json:"origin"`
	StartedAt time.Time       `json:"started_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// OperationTransition records a timestamped change for an exact block and step.
type OperationTransition struct {
	OperationID OperationID    `json:"operation_id"`
	Version     uint64         `json:"version"`
	GroupID     string         `json:"group_id"`
	Attempt     uint32         `json:"attempt"`
	StepSeq     int            `json:"step_seq"`
	State       OperationState `json:"state"`
	Blocked     *BlockedReason `json:"blocked,omitempty"`
	ErrorCode   string         `json:"error_code,omitempty"`
	Error       string         `json:"error,omitempty"`
	At          time.Time      `json:"at"`
}

// Clone returns an operation whose plan and block diagnostics are independently owned.
func (o Operation) Clone() Operation {
	owned := o
	owned.Status = o.Status.Clone()
	owned.Plan = o.Plan.Clone()
	return owned
}

// Clone returns a transition with an independently owned diagnostic.
func (t OperationTransition) Clone() OperationTransition {
	owned := t
	owned.Blocked = cloneBlockedReason(t.Blocked)
	return owned
}
