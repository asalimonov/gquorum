// Package scheduling defines replaceable repair decisions for Raft groups.
// Policies return decisions; the node retains authorization, Raft
// safety and resource ownership. Implementations must honor cancellation and
// must not wait for an operation that joins their own callback.
package scheduling

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// RepairPolicy selects at most one supported repair from owned observations.
// Returning an action requests validation and approval, never direct execution.
// One node serializes calls to its scheduler. Shared implementations must
// synchronize any state shared across nodes. Honor cancellation; status reads
// and RequestStop are allowed, but do not wait for Stop or a scheduling change
// that must join this callback.
type RepairPolicy interface {
	Schedule(context.Context, ScheduleInput) (ScheduleDecision, error)
}

// ScheduleInput provides owned topology, membership, health and local-hold
// observations.
type ScheduleInput struct {
	Now                    time.Time
	Latest, Target, Active topology.Document
	// Groups contains approved and proven declarations; Desired is omitted from callbacks.
	Groups map[string]topology.GroupProgress
	// Status contains group observations with leader names resolved through
	// this input's Active view, not a peer-supplied name or a newer document.
	Status   map[string]topology.GroupStatus
	Health   map[string]topology.GroupHealthEvaluation
	Alive    map[string]time.Duration
	Held     []storage.Identity
	Metrics  metrics.ClusterSnapshot
	Switches []topology.VoterSwitchState
}

// Clone creates an independent callback input, including nested observations.
func (in ScheduleInput) Clone() ScheduleInput {
	in.Latest, in.Target, in.Active = in.Latest.Clone(), in.Target.Clone(), in.Active.Clone()
	in.Groups = maps.Clone(in.Groups)
	for id, progress := range in.Groups {
		progress.Desired = nil
		in.Groups[id] = progress.Clone()
	}
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
	in.Metrics = copyMetrics(in.Metrics)
	in.Switches = slices.Clone(in.Switches)
	for i := range in.Switches {
		in.Switches[i] = in.Switches[i].Clone()
	}
	return in
}

// RestoreMissingReplica requests a successor for an absent legacy load voter.
const RestoreMissingReplica ScheduleActionKind = "restore_missing_replica"

// ReconfigureVoters requests a guarded change to replicated voter
// configuration.
const ReconfigureVoters ScheduleActionKind = "reconfigure_voters"

// copyMetrics preserves input values exactly, including nil/unknown spellings.
// The public metrics Clone methods additionally normalize their JSON read
// shape.
func copyMetrics(in metrics.ClusterSnapshot) metrics.ClusterSnapshot {
	copyReplica := func(r metrics.ReplicaObservation) metrics.ReplicaObservation {
		r.Sample.Voters = slices.Clone(r.Sample.Voters)
		r.Sample.Learners = slices.Clone(r.Sample.Learners)
		r.Sample.Progress = slices.Clone(r.Sample.Progress)
		r.Evidence.Reasons = slices.Clone(r.Evidence.Reasons)
		return r
	}
	in.Local = slices.Clone(in.Local)
	for i := range in.Local {
		in.Local[i] = copyReplica(in.Local[i])
	}
	in.Groups = slices.Clone(in.Groups)
	for i := range in.Groups {
		in.Groups[i].Replicas = slices.Clone(in.Groups[i].Replicas)
		for j := range in.Groups[i].Replicas {
			in.Groups[i].Replicas[j] = copyReplica(in.Groups[i].Replicas[j])
		}
	}
	return in
}
