package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

func (c *operationCoordinator) ReadyFences(ctx context.Context, limit int) ([]operator.FenceWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	work, err := c.g.state().ReadyFences(c.fenceAfter, limit)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !c.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	if len(work) != 0 {
		c.fenceAfter = work[len(work)-1].Resource
	}
	return work, nil
}

func (c *operationCoordinator) ResolveFenceTarget(ctx context.Context,
	work operator.FenceWork) (topology.AuthorityTarget, error) {
	if !c.IsMetaLeader() {
		return topology.AuthorityTarget{}, fault.ErrNotLeader
	}
	peer, err := c.g.fenceReceiver(ctx, work.Grant, work.Resource)
	if err != nil {
		return topology.AuthorityTarget{}, err
	}
	return topology.AuthorityTarget{NodeName: peer.Node.NodeName,
		MemberID: peer.Node.MemberID, ReplicaID: peer.Replica.ReplicaID}, nil
}

func (c *operationCoordinator) DispatchFence(parent context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) error {
	if err := operator.ValidateFenceTarget(grant, resource, target); err != nil {
		return err
	}
	handle, err := c.g.acquireEffectContext(parent)
	if err != nil {
		return err
	}
	defer handle.Release()
	if !c.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	peer, err := c.g.ResolveAuthorityPeer(handle.Context, grant, target)
	if err != nil {
		return err
	}
	return c.g.installEffectFence(handle.Context, grant, resource, peer)
}
