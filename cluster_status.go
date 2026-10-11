package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/topology"
	"time"
)

func (c *clusterView) Rules() topology.Rules {
	return topology.Rules{SchemaVersion: 1, Limits: c.node.opts.Limits}
}
func (c *clusterView) IsLeader() bool {
	m, err := c.node.service(context.Background(), true)
	return err == nil && m.IsLeader()
}
func (c *clusterView) HealthObservation() topology.HealthObservation {
	m, err := c.node.service(context.Background(), false)
	if err != nil {
		return topology.HealthObservation{}
	}
	return m.HealthObservation()
}
func (c *clusterView) Status(parent context.Context) (topology.ClusterStatus, error) {
	m, err := c.node.service(parent, false)
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	n := c.node
	n.mu.RLock()
	cached := n.stopping || n.fenced
	n.mu.RUnlock()
	if cached {
		return m.CachedStatus(parent)
	}
	timeout := n.opts.Loads.EffectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return m.Status(ctx)
}
