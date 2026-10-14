package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// Plan aliases the immutable ordered topology execution plan.
type Plan = topology.Plan

// AuthorizedStep aliases the complete operator and action identity tuple.
type AuthorizedStep = topology.AuthorizedStep
