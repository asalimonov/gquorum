package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/topology"
)

func (c *clusterView) Latest(ctx context.Context) (topology.Document, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Latest(ctx)
}
func (c *clusterView) Target(ctx context.Context) (topology.Document, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Target(ctx)
}
func (c *clusterView) Active(ctx context.Context) (topology.Document, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Active(ctx)
}
func (c *clusterView) Get(ctx context.Context, version uint64) (topology.Document, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Get(ctx, version)
}
func (c *clusterView) History(ctx context.Context, page topology.Page) (topology.HistoryResult, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.HistoryResult{}, err
	}
	return m.History(ctx, page)
}
func (c *clusterView) Watch(ctx context.Context, view topology.View) (<-chan topology.Update, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return nil, err
	}
	return m.Watch(ctx, view)
}
func (c *clusterView) Stage(ctx context.Context, doc topology.Document, expected uint64) (topology.Document, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Stage(ctx, doc, expected)
}
func (c *clusterView) Plan(ctx context.Context, version uint64) (topology.Plan, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return topology.Plan{}, err
	}
	return m.Plan(ctx, version)
}
func (c *clusterView) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return topology.Plan{}, err
	}
	return m.PlanFor(ctx, doc)
}
func (c *clusterView) Operator(ctx context.Context, id topology.OperatorID) (topology.OperatorStatus, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.OperatorStatus{}, err
	}
	return m.Operator(ctx, id)
}
func (c *clusterView) WaitOperator(ctx context.Context, id topology.OperatorID) (topology.OperatorStatus, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return topology.OperatorStatus{}, err
	}
	return m.WaitOperator(ctx, id)
}
func (c *clusterView) Approve(ctx context.Context, version uint64) (topology.OperatorID, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return "", err
	}
	return m.Apply(ctx, version)
}
func (c *clusterView) ParkedJoins(ctx context.Context) ([]topology.ParkedJoin, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return nil, err
	}
	return m.ParkedJoins(ctx)
}
