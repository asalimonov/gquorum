package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// ID aliases the exact topology operator-attempt identifier.
type ID = topology.OperatorID

// State aliases the replicated operator execution state.
type State = topology.OperatorState

// Status aliases the observed operator progress and diagnostics.
type Status = topology.OperatorStatus
