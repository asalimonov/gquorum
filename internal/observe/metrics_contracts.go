package observe

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

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

type GroupSamples struct {
	Status   topology.GroupStatus
	Replicas []metrics.ReplicaObservation
	Probes   []Probe
	Error    error
}
type Round struct {
	ActiveVersion uint64
	Epoch         LeaderEpoch
	Groups        map[string]GroupSamples
}
type MetricsSource interface {
	Active(context.Context) (topology.Document, error)
	Epoch() (LeaderEpoch, bool)
	Collect(context.Context, topology.Document, *ReadPool) (Round, error)
}
type MetricsConfig struct {
	Source        MetricsSource
	Settings      MetricsSettings
	WakeScheduler func()
	Now           func() time.Time
	// Local and Switches are owned, in-memory reads; they must not perform I/O
	// or invoke application callbacks. Local includes only published owners.
	Local    func() []metrics.ReplicaObservation
	Identity func() topology.Identity
	Switches func() []topology.VoterSwitchState
	Enabled  func() bool
}
