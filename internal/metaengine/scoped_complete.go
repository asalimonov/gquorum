package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// All mutations are on an owned draft. The enclosing Apply validates the full
// graph and installs it once, including final-step admission and role evidence.
func (s *scopedState) completeGroupLocked(index uint64,
	t topology.OperationTransition) (*scopedState, error) {
	next, err := s.prepareGroupCompletion(t)
	if err != nil {
		return nil, err
	}
	op, position := next.transitionBlock(t)
	if t.StepSeq != 0 {
		if err := next.recordScopedTransitionEffect(index, op, t.StepSeq, t.At); err != nil {
			return nil, err
		}
	}
	if err := next.detachCleanupLocked(op.Status.ID, t.GroupID, t.At); err != nil {
		return nil, err
	}
	if t.GroupID == topology.MetaGroupID &&
		!completeApprovalAdmissions(next, op, next.indexes, next.authorityCatalog()) {
		return nil, fault.ErrAuthorityPending
	}
	op.Status.Blocks[position] = scopedTransitionStatus(op.Status.Blocks[position].TotalSteps, t)
	op.Status.State, op.UpdatedAt = aggregateBlockStates(op.Status.Blocks), t.At
	next.releaseBlockOwner(op.Status.ID, t.GroupID)
	if err := next.advanceNodeProgressLocked(t.At); err != nil {
		return nil, err
	}
	next.updateFullyConvergedLocked(t.At)
	return next, nil
}

// Receipts normally created the debt already. Rechecking at completion keeps
// inherited retry proof tied to its original removal, never to the new attempt.
func (s *scopedState) detachCleanupLocked(id topology.OperationID, group string, at time.Time) error {
	op := s.indexes.operations[id]
	if op == nil {
		return fault.ErrOperationAuthorization
	}
	for _, step := range op.Plan.Steps {
		if step.GroupID != group || step.Kind != topology.StepRemoveReplica {
			continue
		}
		record := scopedConsumedAuthority(op, step, s.indexes, s.authorityCatalog())
		if record == nil {
			return fault.ErrAuthorityPending
		}
		for _, receipt := range record.Effects {
			if err := s.recordCleanupEffect(receipt, at); err != nil {
				return err
			}
		}
	}
	return nil
}
