package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// A nil declaration with a nonzero source is a proven tombstone. Source zero
// means never converged; neither state is inferred from a native observation.
type groupConvergence struct {
	GroupID       string               `json:"group_id"`
	Declaration   *topology.Group      `json:"declaration,omitempty"`
	SourceVersion uint64               `json:"source_version"`
	Owner         topology.OperationID `json:"owner,omitempty"`
	Deleting      bool                 `json:"deleting"`
	CompletedAt   time.Time            `json:"completed_at"`
}

type nodeConvergence struct {
	NodeName      string         `json:"node_name"`
	Declaration   *topology.Node `json:"declaration,omitempty"`
	SourceVersion uint64         `json:"source_version"`
	// Exact source _meta membership; zero also permits a declared excluded node.
	MemberID    uint64               `json:"member_id"`
	Owner       topology.OperationID `json:"owner,omitempty"`
	CompletedAt time.Time            `json:"completed_at"`
}
