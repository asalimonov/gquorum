// Package policy defines replaceable decisions for cluster scheduling and local
// lifecycle. Policies return decisions; the node retains authorization, Raft
// safety and resource ownership. Implementations must honor cancellation and
// must not wait for an operation that joins their own callback.
package policy

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Scheduler selects at most one supported repair from owned observations.
// Returning an action requests validation and approval, never direct execution.
// One node serializes calls to its scheduler. Shared implementations must
// synchronize any state shared across nodes. Honor cancellation; status reads
// and RequestStop are allowed, but do not wait for Stop or a scheduling change
// that must join this callback.
type Scheduler interface {
	Schedule(context.Context, ScheduleInput) (ScheduleDecision, error)
}

type ScheduleInput struct {
	Now                    time.Time
	Latest, Target, Active topology.Document
	Status                 map[string]topology.GroupStatus
	Health                 map[string]topology.GroupHealthEvaluation
	Alive                  map[string]time.Duration
	Held                   []storage.Identity
}

// Clone creates an independent callback input, including nested observations.
func (in ScheduleInput) Clone() ScheduleInput {
	in.Latest, in.Target, in.Active = in.Latest.Clone(), in.Target.Clone(), in.Active.Clone()
	in.Status = maps.Clone(in.Status)
	for id, status := range in.Status {
		in.Status[id] = status.Clone()
	}
	in.Health = maps.Clone(in.Health)
	for id, health := range in.Health {
		in.Health[id] = health.Clone()
	}
	in.Alive = maps.Clone(in.Alive)
	in.Held = slices.Clone(in.Held)
	return in
}

type ScheduleActionKind string

const RestoreMissingReplica ScheduleActionKind = "restore_missing_replica"

type ScheduleAction struct {
	Kind     ScheduleActionKind
	Identity storage.Identity // The old missing voter; the core allocates its successor.
}

type ScheduleDecision struct {
	Action *ScheduleAction
	Reason string
}
