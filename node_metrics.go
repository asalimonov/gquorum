package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) localLoadMetrics() []metrics.ReplicaObservation {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.LocalMetrics()
}

func (n *node) loadMetricsReads(ctx context.Context, groups []topology.GroupProgress) []observe.ReplicaRead {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.MetricsReads(ctx, groups)
}
func (c *clusterView) Metrics() metrics.ClusterSnapshot {
	c.node.mu.RLock()
	m := c.node.meta
	c.node.mu.RUnlock()
	if m == nil {
		return (metrics.ClusterSnapshot{}).Clone()
	}
	return m.Metrics()
}

func (c *clusterView) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	c.node.mu.RLock()
	m := c.node.meta
	c.node.mu.RUnlock()
	if m == nil {
		return metrics.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	return m.MetricsHistory(q)
}
