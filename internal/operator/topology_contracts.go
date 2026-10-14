package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// Document aliases a replicated topology declaration for plan execution.
type Document = topology.Document

// Rules aliases the replicated schema and capacity limits.
type Rules = topology.Rules

// Limits aliases the topology capacity settings.
type Limits = topology.Limits
