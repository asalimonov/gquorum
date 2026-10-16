package operator

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) executeScopedStep(ctx context.Context, work WorkItem) controllerOutcome {
	step := scopedWorkStep(work)
	if step.Seq == 0 {
		return controllerIdle
	}
	running := topology.OperationTransition{StepSeq: step.Seq, State: topology.OperationRunning}
	if err := e.recordScopedTransition(ctx, work, running); err != nil {
		return controllerIdle
	}
	if !work.Consumed {
		if err := e.runScopedStep(ctx, work); err != nil {
			return e.recordScopedError(ctx, work, err)
		}
	}
	// Both RPCs and native observations may outlive the selected scope. Proof
	// stays under its original grant; only the still-current owner may advance.
	if err := e.checkScopedWork(ctx, work); err != nil {
		return e.recordScopedError(ctx, work, err)
	}
	block, _ := requiredWorkBlock(work)
	position := slices.Index(block.Steps, step.Seq)
	if position == len(block.Steps)-1 {
		return e.finishScopedWork(ctx, work)
	}
	next := topology.OperationTransition{StepSeq: block.Steps[position+1], State: topology.OperationRunning}
	if err := e.recordScopedTransition(ctx, work, next); err != nil {
		return controllerIdle
	}
	return controllerProgressed
}

func (e *Controller) checkScopedWork(ctx context.Context, work WorkItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !e.scoped.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	return e.scopedPlan(ctx, work.Operation.Clone(), work.GroupID)
}

func (e *Controller) runScopedStep(ctx context.Context, work WorkItem) error {
	step := scopedWorkStep(work)
	if step.Kind == topology.StepWaitCaughtUp {
		return e.verifyScopedStep(ctx, work)
	}
	if err := e.checkScopedPreparation(ctx, work); err != nil {
		return err
	}
	action := scopedAction(work, step)
	grant, err := e.authority.BeginEffect(ctx, action)
	if err != nil {
		return err
	}
	if grant.Action == nil || *grant.Action != action || grant.Validate() != nil {
		return fault.ErrOperationAuthorization
	}
	if err := e.checkScopedWork(ctx, work); err != nil {
		return err
	}
	if err := e.performScopedEffect(ctx, work, grant); err != nil {
		return err
	}
	return e.verifyScopedStep(ctx, work)
}

func scopedAction(work WorkItem, step topology.Step) topology.AuthorizedStep {
	op := work.Operation.Status
	return topology.AuthorizedStep{OperatorID: topology.OperatorID(op.ID), Version: op.Version, Attempt: op.Attempt,
		StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID,
		TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID,
		ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
}
