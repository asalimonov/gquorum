package operator

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateRemainingScope reuses the native simulation for one required block.
// The plan and predecessor receipts retain their original global identities.
func ValidateRemainingScope(view RemainingPlanView, op topology.Operation, group string, rules Rules) error {
	block, err := requiredPlanBlock(op, group)
	if err != nil {
		return err
	}
	proofs, err := scopedRemainingProofs(view, op)
	if err != nil {
		return err
	}
	position := slices.IndexFunc(op.Status.Blocks, func(b topology.BlockStatus) bool { return b.GroupID == group })
	status := op.Status.Blocks[position]
	if status.State != topology.OperationFailed && status.State.Terminal() {
		return fault.ErrOperationAuthorization
	}
	first := status.CurrentStep
	consumed := make(map[int]bool, len(view.Consumed))
	for _, seq := range view.Consumed {
		consumed[seq] = true
	}
	if len(block.Steps) != 0 && (first == 0 || status.State == topology.OperationFailed) {
		first = remainingScopeCursor(block, consumed)
	}
	for _, seq := range block.Steps {
		if seq < first && !consumed[seq] {
			return fault.ErrOperationAuthorization
		}
	}
	ex := Record{ID: topology.OperationID(op.Status.ID), Version: op.Status.Version, Attempt: op.Status.Attempt,
		Steps: op.Plan.Steps, VoterBases: op.Plan.VoterBases, CurrentStep: first}
	view.Plan = op.Plan
	if err := validateRetainedScope(view, ex, group); err != nil {
		return err
	}
	approved := view.Target.Group(group)
	if approved == nil {
		return nil
	} // Deletion uses exact stopped-owner proof in metadata.
	required := make(map[int]bool, len(block.Steps))
	for _, seq := range block.Steps {
		required[seq] = true
	}
	validation := remainingPlanValidation{view: view, ex: ex, native: view.Input.Native, rules: rules,
		first: first, proofs: proofs, required: required, consumed: consumed}
	return validation.validateGroup(*approved)
}

func remainingScopeCursor(block topology.PlanBlock, consumed map[int]bool) int {
	last := 0
	for _, seq := range block.Steps {
		last = seq
		if !consumed[seq] {
			return seq
		}
	}
	return last
}

func scopedRemainingProofs(view RemainingPlanView, op topology.Operation) (map[int]bool, error) {
	attempts, err := remainingAncestors(op, view.Previous)
	if err != nil {
		return nil, err
	}
	grants := make(map[topology.AuthorityKey]topology.AuthorityGrant)
	for _, grant := range view.Grants {
		original, exists := attempts[topology.OperationID(grant.OperationID)]
		if !exists {
			return nil, fault.ErrOperationAuthorization
		}
		ex := Record{ID: grant.OperationID, Version: original.Status.Version, Attempt: original.Status.Attempt,
			Steps: original.Plan.Steps}
		if err := validateRemainingGrant(ex, grant); err != nil {
			return nil, err
		}
		if _, duplicate := grants[grant.Key]; duplicate {
			return nil, fault.ErrOperationAuthorization
		}
		grants[grant.Key] = grant
	}
	receipts, err := remainingReceipts(Record{Receipts: view.Receipts}, grants)
	if err != nil {
		return nil, err
	}
	proofs := make(map[int]bool)
	for key, grant := range grants {
		complete, err := remainingGrantComplete(grant, receipts[key], view.Input.Native)
		if err != nil {
			return nil, err
		}
		// This only chooses the simulation's before/result digest. It does not
		// mark the effect consumed or authorize skipping a new attempt's grant.
		proofs[grant.Action.StepSeq] = proofs[grant.Action.StepSeq] || complete
	}
	return proofs, nil
}

func remainingAncestors(op topology.Operation,
	previous []topology.Operation) (map[topology.OperationID]topology.Operation, error) {
	attempts := map[topology.OperationID]topology.Operation{op.Status.ID: op}
	current := op
	for _, parent := range previous {
		if parent.Status.Version != current.Status.Version || parent.Status.Attempt+1 != current.Status.Attempt ||
			current.Previous == "" || current.Previous != parent.Status.ID || parent.Status.Attempt == ^uint32(0) ||
			!parent.Plan.Equal(current.Plan) {
			return nil, fault.ErrOperationAuthorization
		}
		if _, duplicate := attempts[parent.Status.ID]; duplicate {
			return nil, fault.ErrOperationAuthorization
		}
		attempts[parent.Status.ID] = parent
		current = parent
	}
	if current.Previous != "" {
		return nil, fault.ErrHistoryUnavailable
	}
	return attempts, nil
}

func validateRetainedScope(view RemainingPlanView, ex Record, group string) error {
	for _, replica := range remainingOwners(view.Input, group) {
		if err := validateRetainedReplica(view, ex, group, replica); err != nil {
			return err
		}
	}
	return nil
}
