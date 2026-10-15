package topology

import "time"

// FullyConverged records historical full topology completion, not current quorum or cleanup.
type FullyConverged struct {
	Version uint64    `json:"version"`
	At      time.Time `json:"at"`
}

// ProgressQuery selects bounded metadata rows; zero Limit requests the default of 100.
type ProgressQuery struct {
	GroupID     string      `json:"group_id,omitempty"`
	NodeName    string      `json:"node_name,omitempty"`
	OperationID OperationID `json:"operation_id,omitempty"`
	Limit       int         `json:"limit,omitempty"`
	After       string      `json:"after,omitempty"`
}

// ProgressPage captures one metadata index with independently timestamped observations.
type ProgressPage struct {
	AppliedIndex   uint64            `json:"applied_index"`
	ReadAt         time.Time         `json:"read_at"`
	LatestVersion  uint64            `json:"latest_version"`
	TargetVersion  uint64            `json:"target_version"`
	FullyConverged FullyConverged    `json:"fully_converged"`
	Groups         []GroupProgress   `json:"groups"`
	Nodes          []NodeProgress    `json:"nodes"`
	Operations     []OperationStatus `json:"operations"`
	Cleanup        []CleanupProgress `json:"cleanup"`
	Next           string            `json:"next,omitempty"`
}

const (
	defaultProgressLimit = 100
	maxProgressLimit     = 1000
)

// Validate checks filters and limits; the read path binds and validates paging cursors.
func (q ProgressQuery) Validate() error {
	limit := q.Limit
	if limit == 0 {
		limit = defaultProgressLimit
	}
	if limit < 1 || limit > maxProgressLimit {
		return invalid("progress limit must be between 1 and 1000")
	}
	if q.GroupID != "" {
		if err := ValidateID(q.GroupID); err != nil {
			return err
		}
	}
	if q.NodeName != "" && !authorityText(q.NodeName) {
		return invalid("invalid progress node name")
	}
	if q.OperationID != "" && !authorityText(string(q.OperationID)) {
		return invalid("invalid progress operation ID")
	}
	return nil
}
