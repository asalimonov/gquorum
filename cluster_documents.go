package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

func (c *clusterView) Latest(ctx context.Context) (topology.Document, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Latest(ctx)
}
func (c *clusterView) Target(ctx context.Context) (topology.Document, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Target(ctx)
}
func (c *clusterView) Get(ctx context.Context, version uint64) (topology.Document, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Get(ctx, version)
}

func (c *clusterView) History(ctx context.Context, page topology.Page) (topology.HistoryResult, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return topology.HistoryResult{}, err
	}
	return m.History(ctx, page)
}

func (c *clusterView) Watch(ctx context.Context, view topology.View) (<-chan topology.Update, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return nil, err
	}
	return m.Watch(ctx, view)
}

func (c *clusterView) Stage(ctx context.Context, doc topology.Document, expected uint64) (topology.Document, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.Document{}, err
	}
	return m.Stage(ctx, doc, expected)
}

func (c *clusterView) Plan(ctx context.Context, version uint64) (topology.Plan, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.Plan{}, err
	}
	return m.Plan(ctx, version)
}

func (c *clusterView) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.Plan{}, err
	}
	return m.PlanFor(ctx, doc)
}

func (c *clusterView) Operation(ctx context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return topology.OperationStatus{}, err
	}
	return m.Operation(ctx, id)
}

func (c *clusterView) WaitOperation(ctx context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.OperationStatus{}, err
	}
	return m.WaitOperation(ctx, id)
}

func (c *clusterView) RetryOperation(ctx context.Context, previous topology.OperationID) (topology.OperationID, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return "", err
	}
	return m.RetryOperation(ctx, previous)
}

func (c *clusterView) Approve(ctx context.Context, version uint64) (topology.OperationID, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return "", err
	}
	return m.Approve(ctx, version)
}

func (c *clusterView) ParkedJoins(ctx context.Context) ([]topology.ParkedJoin, error) {
	m, err := c.node.service(ctx, readService)
	if err != nil {
		return nil, err
	}
	return m.ParkedJoins(ctx)
}
