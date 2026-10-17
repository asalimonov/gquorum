package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// Local metrics and host recovery consume the same applied per-group progress
// rows as status. TargetVersion is document context, never convergence proof.
func (g *Group) metricsTopology() topology.ProgressPage {
	page, err := g.state().Progress(topology.ProgressQuery{})
	if err != nil {
		return topology.ProgressPage{}
	}
	return page
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
