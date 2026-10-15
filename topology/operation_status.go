package topology

import "slices"

// OperationStatus reports the complete aggregate outcome, even when Blocks is paged.
type OperationStatus struct {
	ID              OperationID    `json:"id"`
	Version         uint64         `json:"version"`
	Attempt         uint32         `json:"attempt"`
	State           OperationState `json:"state"`
	Blocks          []BlockStatus  `json:"blocks"`
	TotalBlocks     int            `json:"total_blocks"`
	BlocksTruncated bool           `json:"blocks_truncated"`
	ErrorCode       string         `json:"error_code,omitempty"`
	Error           string         `json:"error,omitempty"`
}

// BlockStatus describes required execution for one group, excluding detached cleanup.
type BlockStatus struct {
	GroupID     string         `json:"group_id"`
	State       OperationState `json:"state"`
	CurrentStep int            `json:"current_step"`
	TotalSteps  int            `json:"total_steps"`
	Blocked     *BlockedReason `json:"blocked,omitempty"`
	ErrorCode   string         `json:"error_code,omitempty"`
	Error       string         `json:"error,omitempty"`
}

// BlockedReason identifies a scoped prerequisite or diagnostic cause of waiting.
// It is an observation, not authority to perform or skip an effect.
type BlockedReason struct {
	Code        string      `json:"code"`
	Message     string      `json:"message,omitempty"`
	OperationID OperationID `json:"operation_id,omitempty"`
	StepSeq     int         `json:"step_seq,omitempty"`
	NodeName    string      `json:"node_name,omitempty"`
	ReplicaID   uint64      `json:"replica_id,omitempty"`
}

// Clone returns owned block and diagnostic values without recomputing aggregate state.
func (s OperationStatus) Clone() OperationStatus {
	owned := s
	owned.Blocks = slices.Clone(s.Blocks)
	for i := range owned.Blocks {
		owned.Blocks[i].Blocked = cloneBlockedReason(s.Blocks[i].Blocked)
	}
	return owned
}

func cloneBlockedReason(reason *BlockedReason) *BlockedReason {
	if reason == nil {
		return nil
	}
	owned := *reason
	return &owned
}
