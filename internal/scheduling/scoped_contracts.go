package scheduling

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Observation ownership is shared by both static compositions until Task78
// removes the schema3 Host. Exactly one repair port is required at construction.
type observationHost interface {
	IsLeader() bool
	Health(context.Context) (map[string]topology.Health, error)
	RefreshObservations(context.Context) error
	ResetObservations()
	Observations() metrics.ClusterSnapshot
	VoterSwitches() []topology.VoterSwitchState
	WakeOperator()
}

// ScopedHost supplies coherent CAS captures and scoped native evidence. Empty
// group reads metadata only; a named group reads native state and recaptures it.
// PlanRepair must validate the supplied capture after any planning I/O.
type ScopedHost interface {
	observationHost
	ReadRepair(context.Context, string) (metaengine.PlanRead, error)
	PlanRepair(context.Context, metaengine.PlanRead, topology.Document) (topology.Plan, error)
	CommitRepair(context.Context, RepairCommit) (topology.OperatorID, error)
	CommitVoterChange(context.Context, topology.VoterChange, RepairCommit) (topology.OperatorID, error)
}
