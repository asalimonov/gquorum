package operator

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func validateCoordinatorConfig(cfg Config) error {
	if (cfg.Coordinator == nil) == (cfg.LegacyCoordinator == nil) {
		return fault.New(fault.Invalid, "controller requires exactly one execution coordinator", nil)
	}
	if cfg.LegacyCoordinator != nil {
		if cfg.Store == nil || cfg.PlanValidator == nil ||
			cfg.ScopedPlanValidator != nil || cfg.ScopedFinalValidator != nil {
			return fault.New(fault.Invalid, "legacy controller requires its store and plan validator", nil)
		}
		return nil
	}
	if cfg.ScopedPlanValidator == nil || cfg.ScopedFinalValidator == nil ||
		cfg.Store != nil || cfg.PlanValidator != nil || cfg.FinalValidator != nil {
		return fault.New(fault.Invalid, "scoped controller requires only scope validators", nil)
	}
	return nil
}

// Scoped execution is isolated from schema3 composition until Task78.
func (e *Controller) runScopedTick(ctx context.Context) controllerOutcome {
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	if run == nil || run.dispatch == nil {
		return controllerIdle
	}
	e.runDetachedTick(ctx, run)
	e.runRequiredTick(ctx, run)
	return controllerIdle
}

func (e *Controller) runRequiredTick(ctx context.Context, run *controllerRun) {
	limit := run.dispatch.available(e.rules.Limits.WithDefaults().MaxConcurrentGroups)
	if limit == 0 {
		return
	}
	work, err := e.readScopedWork(ctx, limit)
	if err != nil {
		return
	}
	for _, item := range work {
		e.dispatchWork(ctx, run.dispatch, item)
	}
}

func (e *Controller) readScopedWork(ctx context.Context, limit int) ([]WorkItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !e.scoped.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	work, err := e.scoped.ReadyBlocks(bounded, limit)
	if err != nil {
		return nil, err
	}
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	if !e.scoped.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	return work, nil
}

func (e *Controller) runScopedWork(ctx context.Context, work WorkItem) controllerOutcome {
	block, err := requiredWorkBlock(work)
	if err != nil || !e.ownsGroup(work.GroupID) {
		return controllerIdle
	}
	if err := e.scopedPlan(ctx, work.Operation.Clone(), work.GroupID); err != nil {
		return e.recordScopedError(ctx, work, err)
	}
	if ctx.Err() != nil || !e.scoped.IsMetaLeader() {
		return controllerIdle
	}
	if len(block.Steps) != 0 {
		return controllerWaiting // Exact effect execution is installed in Task65.
	}
	if err := e.scopedFinal(ctx, work.Operation.Clone(), work.GroupID); err != nil {
		return e.recordScopedError(ctx, work, err)
	}
	transition := topology.OperationTransition{State: topology.OperationSucceeded}
	if err := e.recordScopedTransition(ctx, work, transition); err != nil {
		return controllerIdle
	}
	return controllerProgressed
}

func requiredWorkBlock(work WorkItem) (topology.PlanBlock, error) {
	op := work.Operation
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool {
		return block.GroupID == work.GroupID
	})
	if position < 0 || position >= len(op.Status.Blocks) || op.Status.BlocksTruncated ||
		op.Status.Blocks[position].GroupID != work.GroupID || op.Status.Blocks[position].State.Terminal() {
		return topology.PlanBlock{}, fault.ErrOperationAuthorization
	}
	return op.Plan.Blocks[position], nil
}

func (e *Controller) recordScopedTransition(ctx context.Context, work WorkItem,
	transition topology.OperationTransition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !e.scoped.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	transition.OperationID, transition.Version = work.Operation.Status.ID, work.Operation.Status.Version
	transition.Attempt, transition.GroupID = work.Operation.Status.Attempt, work.GroupID
	transition.At = operatorTime(e.clock.Now().UTC(), work.Operation.UpdatedAt)
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	return e.scoped.RecordOperationTransition(bounded, transition)
}
