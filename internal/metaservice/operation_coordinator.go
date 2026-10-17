package metaservice

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// The controller adapter selects current scoped work with local fair cursors.
type operationCoordinator struct {
	g            *Group
	mu           sync.Mutex
	after        string
	cleanupAfter storage.Identity
	fenceAfter   topology.AuthorityResource
}

func (c *operationCoordinator) IsMetaLeader() bool { return c.g.IsLeader() }

func (c *operationCoordinator) ReadyBlocks(ctx context.Context, limit int) ([]operator.WorkItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	work, err := c.g.state().ReadyBlocks(c.after, limit)
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
		c.after = work[len(work)-1].GroupID
	}
	return work, nil
}

func (c *operationCoordinator) RecordOperationTransition(ctx context.Context, t topology.OperationTransition) error {
	if !c.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	_, err := c.g.propose(ctx, metaengine.TransitionOperationProposal{Transition: t.Clone(),
		ProposalID: newProposalID()})
	return err
}

var _ operator.Coordinator = (*operationCoordinator)(nil)
