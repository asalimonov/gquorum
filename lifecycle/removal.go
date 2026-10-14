package lifecycle

import (
	"github.com/asalimonov/gquorum/topology"
)

// RemovalInput supplies applied self-removal evidence after mandatory fencing.
type RemovalInput struct {
	Identity topology.Identity
	Removal  topology.SelfRemovalStatus
	Fenced   bool
}

// RemovalAction selects the local response to authorized self-removal.
type RemovalAction string

// RemovalDecision selects an optional local response after participation has
// been fenced.
type RemovalDecision struct {
	Action RemovalAction
	Reason string
}
