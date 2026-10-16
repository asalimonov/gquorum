package metaengine

import (
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) rebindRetryStops(previous, retry *topology.Operation) error {
	debts := slices.Clone(s.Cleanup)
	for i := range debts {
		debt := &debts[i]
		if debt.Stopped != nil || debt.StopAction == nil ||
			debt.StopAction.OperatorID != topology.OperatorID(previous.Status.ID) ||
			!requiredCleanupStop(previous, *debt.StopAction) {
			continue
		}
		if retry.UpdatedAt.Before(debt.UpdatedAt) {
			return fault.ErrOperationAuthorization
		}
		action := scopedPlanAction(retry, retry.Plan.Steps[debt.StopAction.StepSeq-1])
		debt.StopAction, debt.UpdatedAt = &action, retry.UpdatedAt
	}
	s.Cleanup = debts
	return nil
}

// The immutable suffix keeps its original identities. Temporary admission and
// evacuation barriers may wait; a disproven identity/placement requires a new approval.
func (s *scopedState) validateRetrySuffix(retry *topology.Operation) error {
	for _, step := range s.successorSteps(retry) {
		action := scopedPlanAction(retry, step)
		if err := s.retryActionBarriers(retry, action); err != nil && !errors.Is(err, fault.ErrUnavailable) {
			return fault.New(fault.VersionConflict, "retry suffix no longer has approved prerequisites", err)
		}
		if step.Kind == topology.StepPrepareReplica && s.retryIdentityRetired(retry, step) {
			return fault.New(fault.VersionConflict, "retry cannot recreate a retired identity", nil)
		}
		if step.Kind != topology.StepWaitCaughtUp {
			if _, err := s.grantResources(action); err != nil {
				return fault.New(fault.VersionConflict, "retry suffix lacks its exact original targets", err)
			}
		}
	}
	return nil
}

func (s *scopedState) retryIdentityRetired(op *topology.Operation, step topology.Step) bool {
	approved := s.indexes.documents[op.Status.Version]
	member := approved.MemberID(step.NodeName)
	for _, debt := range s.Cleanup {
		if debt.MemberID == member && debt.Identity == (storage.Identity{ClusterID: s.Target.ClusterID,
			GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}) {
			return true // Removal already invalidated that exact application's preparation.
		}
	}
	return false
}

func (s *scopedState) successorSteps(op *topology.Operation) []topology.Step {
	if op.Previous == "" {
		return op.Plan.Steps
	}
	catalog := s.authorityCatalog()
	var steps []topology.Step
	for i, block := range op.Status.Blocks {
		if block.State.Terminal() {
			continue
		}
		for _, seq := range op.Plan.Blocks[i].Steps {
			step := op.Plan.Steps[seq-1]
			if seq >= block.CurrentStep && !retryStepConsumed(op, step, s.indexes, catalog) {
				steps = append(steps, step)
			}
		}
	}
	return steps
}

func (s *scopedState) retryActionBarriers(op *topology.Operation, action topology.AuthorizedStep) error {
	if action.GroupID == topology.MetaGroupID && action.Kind == topology.StepPrepareReplica {
		// A process may bind after retry approval. Exact current membership was
		// checked by validateRetryRequest; issuance still requires live admission.
		return nil
	}
	return s.validateActionBarriers(op, action)
}
