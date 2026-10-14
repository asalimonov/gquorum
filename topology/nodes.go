package topology

import (
	"time"
)

// NodeType is an application-defined classification used by placement rules.
type NodeType string

// NodeState describes a participant's declared placement and drain state.
type NodeState string

// Node is the replicated declaration of a named cluster participant. Labels
// describe host attributes and may include the reserved placement restrictions;
// Region is separate descriptive metadata, not a selector fallback.
type Node struct {
	// Name is distinct from the numeric _meta member ID.
	Name   string            `json:"name"`
	Type   NodeType          `json:"type"`
	State  NodeState         `json:"state"`
	Region string            `json:"region,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
	// Creation and last-declaration-change timestamps.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
