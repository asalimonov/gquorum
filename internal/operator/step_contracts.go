package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// Transition aliases a timestamped exact operator-step state change.
type Transition = topology.OperatorTransition

// Step aliases one ordered authorized plan operation.
type Step = topology.Step

// StepKind aliases the supported topology-plan operation kinds.
type StepKind = topology.StepKind
