package operator

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) recordScopedError(ctx context.Context, work WorkItem, err error) controllerOutcome {
	step := scopedWorkStep(work)
	classification := classifyStepError(step, err)
	transition := topology.OperationTransition{StepSeq: step.Seq, State: topology.OperationState(classification.state)}
	if classification.retry {
		transition.Blocked = &topology.BlockedReason{Code: scopedWaitCode(transition.State, err),
			Message: err.Error(), OperationID: work.Operation.Status.ID,
			StepSeq: step.Seq, NodeName: step.NodeName, ReplicaID: step.ReplicaID}
	} else {
		transition.Error = err.Error()
		var classified *fault.Error
		if errors.As(err, &classified) {
			transition.ErrorCode = string(classified.Code)
		}
	}
	if e.recordScopedTransition(ctx, work, transition) != nil {
		return controllerIdle
	}
	if classification.retry {
		return controllerWaiting
	}
	return controllerProgressed
}

func scopedWorkStep(work WorkItem) topology.Step {
	definition, err := requiredWorkBlock(work)
	if err != nil || len(definition.Steps) == 0 {
		return topology.Step{}
	}
	seq := definition.Steps[0]
	for _, block := range work.Operation.Status.Blocks {
		if block.GroupID == work.GroupID && block.CurrentStep != 0 {
			seq = block.CurrentStep
		}
	}
	return work.Operation.Plan.Steps[seq-1]
}

func scopedWaitCode(state topology.OperationState, err error) string {
	if errors.Is(err, fault.ErrAuthorityPending) {
		return topology.BlockedRevocationPending
	}
	if state == topology.OperationWaitingForCatchUp {
		return topology.BlockedCatchUp
	}
	if state == topology.OperationWaitingForReplica {
		return topology.BlockedReplicaUnreachable
	}
	if errors.Is(err, fault.ErrUnavailable) {
		return topology.BlockedQuorumUnavailable
	}
	return topology.BlockedLeadership
}
