package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// Group aliases a replicated group declaration.
type Group = topology.Group

// Replica aliases an exact desired group assignment.
type Replica = topology.Replica

// PlacementRule aliases voter bounds and host eligibility restrictions.
type PlacementRule = topology.PlacementRule
