package observe

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Round scopes collected group samples to an active topology version and
// leadership epoch.
type Round struct {
	ActiveVersion uint64
	Epoch         LeaderEpoch
	Groups        map[string]GroupSamples
}

// MetricsSource supplies active topology, collector leadership and bounded peer
// observations.
type MetricsSource interface {
	Active(context.Context) (topology.Document, error)
	Epoch() (LeaderEpoch, bool)
	Collect(context.Context, topology.Document, *ReadPool) (Round, error)
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
