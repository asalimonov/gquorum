package scheduling

import (
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ScheduleActionKind identifies a repair operation supported by the node.
type ScheduleActionKind string

// ScheduleAction identifies the requested repair and its exact affected
// identity.
type ScheduleAction struct {
	Kind        ScheduleActionKind
	VoterChange *topology.VoterChange
	Identity    storage.Identity // The old missing voter; the core allocates its successor.
}

// ScheduleDecision requests at most one repair with a diagnostic reason.
type ScheduleDecision struct {
	Action *ScheduleAction
	Reason string
}
