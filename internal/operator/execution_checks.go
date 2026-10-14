package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/topology"
)

const unknownStepMessage = "%w: unknown step kind %q"

func (e *Controller) recordPlanError(ctx context.Context, operator Record, err error) controllerOutcome {
	step := Step{}
	if len(operator.Steps) != 0 {
		seq := max(operator.CurrentStep, 1)
		if seq > len(operator.Steps) {
			return controllerIdle
		}
		step = operator.Steps[seq-1]
	}
	return e.recordError(ctx, operator, step, err)
}

func (e *Controller) stepStatus(ctx context.Context, active, target Document, step Step) (GroupStatus, error) {
	status, statusErr := e.groups.Status(ctx, step.GroupID)
	if statusErr != nil {
		if canResumeFounding(step, active, target, statusErr) {
			// Founding fans out the same deterministic voter set to every host.
			// If leadership or an RPC is interrupted after the first host
			// starts, that partial group cannot elect a leader and Status
			// remains unavailable. Re-run the authorized idempotent fan-out so
			// the remaining hosts can
			// install the group; established groups never take this path.
			status = GroupStatus{GroupID: step.GroupID}
		} else if (step.Kind == StepStopReplica || step.Kind == StepDeleteGroupState) &&
			errors.Is(statusErr, ErrGroupUnavailable) && active.Group(step.GroupID) != nil &&
			target.Group(step.GroupID) == nil {
			// Whole-group deletion deliberately stops every runtime without
			// first changing Raft membership. Once enough hosts have
			// acknowledged their stop, the group cannot have a leader; the
			// remaining exact host-local stops and shared-state deletion must
			// still proceed. Their scoped RPCs
			// remain the reachability and authorization boundary.
			status = GroupStatus{GroupID: step.GroupID, Found: step.Kind == StepStopReplica}
		} else {
			return status, statusErr
		}
	}
	return status, nil
}

func (e *Controller) executeStep(ctx context.Context, operator Record, input stepValidationInput) controllerOutcome {
	step, target, status := input.step, input.target, input.status
	if err := e.validateStep(ctx, operator, input); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		return e.recordError(ctx, operator, step, err)
	}
	if err := e.ensureRunning(ctx, operator, step); err != nil {
		return controllerIdle
	}
	if step.Kind == StepWaitCaughtUp && e.effectSatisfied(step, target, status) {
		return e.advance(ctx, operator, target, step)
	}
	if err := e.revalidateExact(ctx, operator, step); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		e.fail(ctx, operator, step, err)
		return controllerProgressed
	}
	if err := e.runStep(ctx, step, status); err != nil {
		if ctx.Err() == nil && e.coordinator.IsMetaLeader() && e.operatorIsCurrent(ctx, operator, step) {
			return e.recordError(ctx, operator, step, err)
		}
		return controllerIdle
	}
	return e.completeStep(ctx, operator, input)
}

func (e *Controller) completeStep(ctx context.Context, operator Record, input stepValidationInput) controllerOutcome {
	step, target := input.step, input.target
	// A submitted ConfChange is never rolled back or overlapped. Its call is
	// allowed to finish, then leadership and the exact replicated boundary are
	// observed again before this worker records progress.
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return controllerIdle
	}
	if err := e.revalidateExact(ctx, operator, step); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		return controllerIdle
	}
	// Membership/create/transfer acknowledgements are verified by a fresh
	// global observation. Host-local lifecycle callbacks attest their verified
	// exact identity result as part of the GroupEffects contract.
	if requiresObservedVerification(step.Kind) {
		observed, err := e.groups.Status(ctx, step.GroupID)
		if err != nil {
			return e.recordError(ctx, operator, step, err)
		}
		if observed.GroupID != step.GroupID {
			return e.recordError(ctx, operator, step, ErrOperatorAuthorization)
		}
		if !e.effectSatisfied(step, target, observed) {
			return e.recordError(ctx, operator, step,
				fmt.Errorf("%w: acknowledged effect is not observed for exact replica %d", ErrReplicaNotReady,
					step.ReplicaID))
		}
	}
	return e.advance(ctx, operator, target, step)
}

func (e *Controller) verifyFinalGroup(ctx context.Context, groupID string, target Document) error {
	status, err := e.groups.Status(ctx, groupID)
	if err != nil {
		if !errors.Is(err, ErrGroupUnavailable) || target.Group(groupID) != nil {
			return err
		}
		status = GroupStatus{GroupID: groupID}
	}
	if !groupMatchesTarget(target.Group(groupID), status) {
		return fmt.Errorf("%w: group %q does not match Target after final step", ErrReplicaNotReady, groupID)
	}
	if target.Group(groupID) != nil {
		native, err := e.groups.ReadNative(ctx, groupID)
		if err != nil {
			return err
		}
		if native.GroupID != groupID ||
			!groupMatchesTarget(target.Group(groupID), GroupStatus{GroupID: groupID, Found: true,
				ConfState: native.ConfState}) {
			return fmt.Errorf("%w: group %q applied membership does not match Target after final step",
				ErrReplicaNotReady, groupID)
		}
	}
	return nil
}

func (e *Controller) recordReceipts(ctx context.Context, grant topology.AuthorityGrant,
	receipts []topology.EffectReceipt) error {
	for _, receipt := range receipts {
		if receipt.Validate(grant) != nil {
			return ErrOperatorAuthorization
		}
		if err := e.authority.RecordEffect(ctx, receipt); err != nil {
			return err
		}
	}
	return nil
}

func (e *Controller) effectComplete(ctx context.Context, grant topology.AuthorityGrant) (bool, error) {
	recorded, err := e.authority.EffectReceipts(ctx, grant)
	if err != nil {
		return false, err
	}
	return receiptSetComplete(grant, recorded)
}

func validateStepPresence(input stepValidationInput) error {
	active, target, step, status := input.active, input.target, input.step, input.status
	deletingGroupStop := step.Kind == StepStopReplica && active.Group(step.GroupID) != nil &&
		target.Group(step.GroupID) == nil
	if step.Kind != StepCreateFoundingGroup && step.Kind != StepDeleteGroupState && !deletingGroupStop &&
		!status.Found {
		return fmt.Errorf("%w: group %q is not globally present", ErrGroupUnavailable, step.GroupID)
	}
	if (step.Kind == StepAddLearner || step.Kind == StepPromoteVoter || step.Kind == StepDemoteVoter ||
		step.Kind == StepRemoveReplica || step.Kind == StepTransferLeader) && status.LeaderID == 0 {
		return fmt.Errorf("%w: group %q has no leader", ErrGroupUnavailable, step.GroupID)
	}
	return nil
}

func validateDesiredStep(ex Record, input stepValidationInput) error {
	switch input.step.Kind {
	case StepCreateFoundingGroup, StepPrepareReplica:
		return validateStartingStep(ex, input)
	case StepAddLearner, StepWaitCaughtUp, StepPromoteVoter, StepDemoteVoter, StepTransferLeader:
		return validateReplicaStep(input)
	case StepRemoveReplica, StepStopReplica, StepCleanupReplicaState, StepDeleteGroupState:
		return validateRetiringStep(ex, input)
	default:
		return fmt.Errorf(unknownStepMessage, ErrOperatorAuthorization, input.step.Kind)
	}
}

func validateStartingStep(ex Record, input stepValidationInput) error {
	active, target, step, status := input.active, input.target, input.step, input.status
	targetReplica := documentReplicaByID(target, step.GroupID, step.ReplicaID)
	switch step.Kind {
	case StepCreateFoundingGroup:
		if active.Group(step.GroupID) != nil || target.Group(step.GroupID) == nil {
			return fmt.Errorf("%w: founding group is not fresh", ErrOperatorAuthorization)
		}
		if status.Found && !foundingMatchesTarget(target.Group(step.GroupID), status) {
			return fmt.Errorf("%w: founding group has conflicting membership", ErrOperatorAuthorization)
		}
	case StepPrepareReplica:
		if targetReplica == nil ||
			active.Group(step.GroupID) == nil && (targetReplica.Role != ReplicaLearner ||
				!freshFoundingPlan(ex, step)) {
			return fmt.Errorf("%w: prepare identity is not an established Target replica", ErrOperatorAuthorization)
		}
	default:
		return fmt.Errorf(unknownStepMessage, ErrOperatorAuthorization, step.Kind)
	}
	return nil
}

func validateReplicaStep(input stepValidationInput) error {
	target, step := input.target, input.step
	targetReplica := documentReplicaByID(target, step.GroupID, step.ReplicaID)
	switch step.Kind {
	case StepAddLearner:
		if targetReplica == nil {
			return fmt.Errorf("%w: learner is absent from Target", ErrOperatorAuthorization)
		}
	case StepWaitCaughtUp, StepPromoteVoter:
		if targetReplica == nil || targetReplica.Role != ReplicaVoter {
			return fmt.Errorf("%w: promotion target is not a desired voter", ErrOperatorAuthorization)
		}
	case StepDemoteVoter:
		if targetReplica == nil || targetReplica.Role != ReplicaLearner {
			return fmt.Errorf("%w: demotion target is not a desired learner", ErrOperatorAuthorization)
		}
	case StepTransferLeader:
		if targetReplica == nil || targetReplica.Role != ReplicaVoter {
			return fmt.Errorf("%w: leadership target is not a desired voter", ErrOperatorAuthorization)
		}
	default:
		return fmt.Errorf(unknownStepMessage, ErrOperatorAuthorization, step.Kind)
	}
	return nil
}

func validateRetiringStep(ex Record, input stepValidationInput) error {
	active, target, step := input.active, input.target, input.step
	targetReplica := documentReplicaByID(target, step.GroupID, step.ReplicaID)
	activeReplica := documentReplicaByID(active, step.GroupID, step.ReplicaID)
	switch step.Kind {
	case StepRemoveReplica, StepStopReplica:
		if targetReplica != nil || activeReplica == nil || activeReplica.NodeName != step.NodeName {
			return fmt.Errorf("%w: removal does not name an exact former replica", ErrOperatorAuthorization)
		}
	case StepCleanupReplicaState:
		if step.GroupID == MetaGroupID || targetReplica != nil || activeReplica == nil || step.Seq < cleanupStepSpan ||
			ex.Steps[step.Seq-2].Kind != StepStopReplica ||
			ex.Steps[step.Seq-cleanupStepSpan].Kind != StepRemoveReplica {
			return fmt.Errorf("%w: cleanup is not authorized after exact data replica removal and stop",
				ErrOperatorAuthorization)
		}
	case StepDeleteGroupState:
		if step.GroupID == MetaGroupID || target.Group(step.GroupID) != nil || active.Group(step.GroupID) == nil {
			return fmt.Errorf("%w: delete does not name an exact removed data group", ErrOperatorAuthorization)
		}
	default:
		return fmt.Errorf(unknownStepMessage, ErrOperatorAuthorization, step.Kind)
	}
	return nil
}
