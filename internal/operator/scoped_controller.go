package operator

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func validateCoordinatorConfig(cfg Config) error {
	if cfg.Coordinator == nil || cfg.ScopedPlanValidator == nil || cfg.ScopedFinalValidator == nil {
		return fault.New(fault.Invalid, "controller requires coordinator and scope validators", nil)
	}
	return nil
}

// TODO(not planned): priorities, expiry and resource-pressure weighting.
// Current dispatch uses separate bounded budgets with rotation and backoff.
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
		return e.executeScopedStep(ctx, work)
	}
	return e.finishScopedWork(ctx, work)
}

func (e *Controller) finishScopedWork(ctx context.Context, work WorkItem) controllerOutcome {
	if err := e.verifyScopedGroup(ctx, work); err != nil {
		return e.recordScopedError(ctx, work, err)
	}
	if err := e.scopedFinal(ctx, work.Operation.Clone(), work.GroupID); err != nil {
		return e.recordScopedError(ctx, work, err)
	}
	transition := topology.OperationTransition{StepSeq: scopedWorkStep(work).Seq, State: topology.OperationSucceeded}
	if err := e.recordScopedTransition(ctx, work, transition); err != nil {
		return controllerIdle
	}
	return controllerProgressed
}

func requiredWorkBlock(work WorkItem) (topology.PlanBlock, error) {
	block, err := requiredPlanBlock(work.Operation, work.GroupID)
	if err != nil {
		return block, err
	}
	for _, status := range work.Operation.Status.Blocks {
		if status.GroupID == work.GroupID && status.State.Terminal() {
			return topology.PlanBlock{}, fault.ErrOperationAuthorization
		}
	}
	return block, nil
}

func requiredPlanBlock(op topology.Operation, group string) (topology.PlanBlock, error) {
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool {
		return block.GroupID == group
	})
	if position < 0 || position >= len(op.Status.Blocks) || op.Status.BlocksTruncated ||
		op.Status.Blocks[position].GroupID != group {
		return topology.PlanBlock{}, fault.ErrOperationAuthorization
	}
	definition := op.Plan.Blocks[position]
	for _, seq := range definition.Steps {
		if seq < 1 || seq > len(op.Plan.Steps) || op.Plan.Steps[seq-1].Seq != seq ||
			op.Plan.Steps[seq-1].GroupID != group {
			return topology.PlanBlock{}, fault.ErrOperationAuthorization
		}
	}
	if cursor := op.Status.Blocks[position].CurrentStep; cursor != 0 && !slices.Contains(definition.Steps, cursor) {
		return topology.PlanBlock{}, fault.ErrOperationAuthorization
	}
	return definition, nil
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
