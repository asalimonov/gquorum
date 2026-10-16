package operator

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) verifyScopedStep(ctx context.Context, work WorkItem) error {
	step := scopedWorkStep(work)
	if step.Kind != topology.StepWaitCaughtUp && !requiresObservedVerification(step.Kind) {
		return nil // Exact host receipts attest joined lifecycle ownership.
	}
	status, err := e.groups.Status(ctx, work.GroupID)
	if err != nil {
		return err
	}
	if status.GroupID != work.GroupID {
		return fault.ErrOperationAuthorization
	}
	if status.LeaderID == 0 || !e.scopedEffectSatisfied(work, status) {
		return fault.ErrReplicaNotReady
	}
	return nil
}

func (e *Controller) checkScopedPreparation(ctx context.Context, work WorkItem) error {
	learner := scopedPreparatoryLearner(work)
	if learner == 0 {
		return nil
	}
	status, err := e.groups.Status(ctx, work.GroupID)
	if err != nil {
		return err
	}
	if status.GroupID != work.GroupID {
		return fault.ErrOperationAuthorization
	}
	if e.scopedEffectSatisfied(work, status) {
		return nil // The native receiver still persists fresh, fenced result proof.
	}
	if ready, _ := caughtUpStatus(status, learner); !ready || status.LeaderID == 0 {
		return fault.ErrReplicaNotReady
	}
	return nil
}

func scopedPreparatoryLearner(work WorkItem) uint64 {
	step := scopedWorkStep(work)
	if step.Kind != topology.StepRemoveReplica && step.Kind != topology.StepDemoteVoter {
		return 0
	}
	block, err := requiredWorkBlock(work)
	if err != nil {
		return 0
	}
	previous := topology.Step{}
	for _, seq := range block.Steps {
		if seq == step.Seq {
			break
		}
		candidate := work.Operation.Plan.Steps[seq-1]
		if candidate.Kind != topology.StepTransferLeader {
			previous = candidate
		}
	}
	if previous.Kind == topology.StepWaitCaughtUp && previous.ReplicaID != step.ReplicaID {
		return previous.ReplicaID
	}
	return 0
}

func (e *Controller) scopedEffectSatisfied(work WorkItem, status GroupStatus) bool {
	step := scopedWorkStep(work)
	return status.Found && e.nativeEffectSatisfied(step, work.Approved, status)
}

func (e *Controller) verifyScopedGroup(ctx context.Context, work WorkItem) error {
	if work.Approved == nil {
		// Deletion is proven by exact joined stops/readmission in the FSM.
		// Requiring a native leader after stopping its voters would deadlock it.
		return nil
	}
	status, err := e.groups.Status(ctx, work.GroupID)
	if err != nil {
		return err
	}
	if status.GroupID != work.GroupID || !groupMatchesTarget(work.Approved, status) {
		return fault.ErrReplicaNotReady
	}
	native, err := e.groups.ReadNative(ctx, work.GroupID)
	if err != nil {
		return err
	}
	if native.GroupID != work.GroupID || !groupMatchesTarget(work.Approved,
		GroupStatus{GroupID: work.GroupID, Found: true, ConfState: native.ConfState}) {
		return fault.ErrReplicaNotReady
	}
	return nil
}
