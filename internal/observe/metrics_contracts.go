package observe

import (
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// LeaderEpoch identifies a metrics collector's process incarnation and Raft
// leadership term.
type LeaderEpoch struct {
	InstanceID string
	Term       uint64
	LeaderID   uint64
}

// Probe separates transport evidence from optional metrics. An adapter that
// omits metrics can still answer, but cannot prove standby readiness. Only an
// exact, dispatched, independently timed out/failed attempt is Unreachable.
type Probe struct {
	Identity     metrics.ReplicaIdentity
	Observation  metrics.ReplicaObservation
	Status       topology.GroupStatus
	Dispatched   bool
	Reachability string // alive, unreachable, unknown
	Held         bool
	Error        error
}

// GroupSamples combines native group status, replica observations and transport
// probes.
type GroupSamples struct {
	SourceVersion uint64
	Status        topology.GroupStatus
	Replicas      []metrics.ReplicaObservation
	Probes        []Probe
	Error         error
}
