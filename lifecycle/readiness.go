package lifecycle

import (
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// ReadinessInput supplies owned admission, recovery and founder observations.
type ReadinessInput struct {
	Now, StartedAt                   time.Time
	Identity                         topology.Identity
	FirstBootstrap                   bool
	Admitted, Recovered, SessionLive bool
	MissingFounders                  []topology.Identity
}

// ReadinessState describes the outcome of a local startup gate.
type ReadinessState string

// ReadinessDecision selects a startup-gate state and an optional diagnostic
// reason.
type ReadinessDecision struct {
	State  ReadinessState
	Reason string
}
