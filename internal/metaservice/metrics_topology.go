package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Task78 replaces this named schema3 projection with local ProgressStatus rows.
// It copies actual proven rows; TargetVersion is only document context.
func (g *Group) metricsTopology() topology.ProgressPage {
	views := g.state().Views()
	return topology.ProgressPage{TargetVersion: views.Target.Version, Groups: metaengine.LegacyGroupProgress(views)}
}

func metricsConvergedGroup(groups []topology.GroupProgress, id string) *topology.Group {
	for _, row := range groups {
		if row.GroupID == id && row.ConvergedVersion != 0 {
			return row.Converged
		}
	}
	return nil
}

// LocalProgress copies applied group declarations without a quorum read or peer
// probes. Private host recovery and metrics use the same source capture.
func (g *Group) LocalProgress(ctx context.Context) (topology.ProgressPage, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return topology.ProgressPage{}, err
	}
	return g.metricsTopology(), nil
}
