package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (c *operationCoordinator) ReadyCleanup(ctx context.Context, limit int) ([]operator.CleanupWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	work, err := c.g.state().ReadyCleanup(c.cleanupAfter, limit)
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
		c.cleanupAfter = work[len(work)-1].Identity
	}
	return work, nil
}

func (c *operationCoordinator) BeginCleanupEffect(ctx context.Context, id storage.Identity,
	action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	if !c.IsMetaLeader() {
		return topology.AuthorityGrant{}, fault.ErrNotLeader
	}
	return c.g.BeginCleanupEffect(ctx, id, action)
}
