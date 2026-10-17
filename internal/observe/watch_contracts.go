package observe

import (
	"context"
	"log/slog"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// Source supplies approved topology and owned node-health observations.
type Source interface {
	Target(context.Context) (topology.Document, error)
	Health(context.Context, topology.Document) (map[string]topology.Health, error)
}

// Settings bounds the health-sampling interval and each observation timeout.
type Settings struct{ Interval, Timeout time.Duration }

// Config supplies health observation sources, settings and synchronous
// transition hooks.
type Config struct {
	Source   Source
	Settings Settings
	Logger   *slog.Logger
	// Transition hooks run synchronously on the sampler; they must be bounded
	// and must not call Refresh or Stop on this watcher.
	OnGroupTransition func(topology.GroupHealth, topology.GroupHealthEvaluation)
	OnNodeTransition  func(string, topology.Health, topology.Health)
}
