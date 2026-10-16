package observe

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Round binds collected samples to per-group sources and one leadership epoch.
type Round struct {
	Epoch  LeaderEpoch
	Groups map[string]GroupSamples
}

// MetricsSource supplies owned group checkpoints, leadership and bounded probes.
// Topology returns only TargetVersion and Groups; it is a local observation.
type MetricsSource interface {
	Topology(context.Context) (topology.ProgressPage, error)
	Epoch() (LeaderEpoch, bool)
	Collect(context.Context, topology.ProgressPage, *ReadPool) (Round, error)
}

// MetricsConfig supplies collector settings, private sources and scheduler wake
// callbacks.
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
