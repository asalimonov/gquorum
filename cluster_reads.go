package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

func (c *clusterView) ReadBarrier(ctx context.Context) error {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return err
	}
	return m.ReadBarrier(ctx)
}

func (c *clusterView) ReadTopology(ctx context.Context) (topology.ReadView, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.ReadView{}, err
	}
	return m.ReadTopology(ctx)
}

func (c *clusterView) ReadLockHolder(ctx context.Context, key string) (topology.LockHolder, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.LockHolder{}, err
	}
	return m.ReadLockHolder(ctx, key)
}

func (c *clusterView) ReadAuthority(ctx context.Context, id topology.OperationID) (topology.AuthorityStatus, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.AuthorityStatus{}, err
	}
	return m.ReadAuthority(ctx, topology.OperatorID(id))
}

func (c *clusterView) WaitRevoked(ctx context.Context, id topology.OperationID) (topology.AuthorityStatus, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.AuthorityStatus{}, err
	}
	return m.WaitRevoked(ctx, topology.OperatorID(id))
}
