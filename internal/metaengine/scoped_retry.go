package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyRetryOperation(index uint64, p RetryOperationProposal) (proposalApplication, error) {
	if next := s.indexes.retries[p.Previous]; next != "" {
		return proposalApplication{result: scopedApprovalResult(p.ProposalID, next)}, nil
	}
	previous := s.indexes.operations[p.Previous]
	if previous == nil {
		return proposalApplication{}, fault.ErrHistoryUnavailable
	}
	if err := s.validateRetryRequest(previous, p); err != nil {
		return proposalApplication{}, err
	}
	next, err := s.prepareRetry(previous, p)
	if err != nil {
		return proposalApplication{}, err
	}
	retry := &next.Operations[len(next.Operations)-1]
	if err := next.recordRetryPrefix(index, previous, retry); err != nil {
		return proposalApplication{}, err
	}
	if err := next.revokeAuthorityLocked(index, failedBlockRefs(previous), retry.Status.ID); err != nil {
		return proposalApplication{}, err
	}
	if err := next.advanceNodeProgressLocked(p.At); err != nil {
		return proposalApplication{}, err
	}
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = *next
	return proposalApplication{changed: true, result: scopedApprovalResult(p.ProposalID, retry.Status.ID)}, nil
}

func (s *scopedState) validateRetryRequest(previous *topology.Operation, p RetryOperationProposal) error {
	if previous.Status.State != topology.OperationFailed || p.At.IsZero() || p.At.Before(previous.UpdatedAt) {
		return fault.ErrOperationAuthorization
	}
	if previous.Status.Attempt == ^uint32(0) {
		return fault.New(fault.Invalid, "operation attempt exhausted", nil)
	}
	groups := make([]string, 0, len(p.Scopes))
	for _, block := range failedBlockRefs(previous) {
		groups = append(groups, block.GroupID)
	}
	if len(groups) == 0 {
		return fault.ErrOperationAuthorization
	}
	if err := validateScopeGroups(p.Scopes, groups); err != nil {
		return err
	}
	approved, found := s.indexes.documents[previous.Status.Version]
	if !found {
		return fault.ErrHistoryUnavailable
	}
	changed := topology.AffectedGroups(approved, s.Target)
	for _, scope := range p.Scopes {
		row := s.indexes.groups[scope.GroupID]
		if row == nil || row.Owner != "" || !scope.matches(row) ||
			p.At.Before(row.CompletedAt) || slices.Contains(changed, scope.GroupID) ||
			s.newerGroupApproval(previous.Status.Version, scope.GroupID) {
			return fault.ErrVersionConflict
		}
	}
	return nil
}

func (s *scopedState) newerGroupApproval(version uint64, group string) bool {
	for _, op := range s.Operations {
		if op.Status.Version > version && slices.ContainsFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool {
			return block.GroupID == group
		}) {
			return true
		}
	}
	row := s.indexes.groups[group]
	return row != nil && row.SourceVersion > version
}

func failedBlockRefs(operation *topology.Operation) []blockRef {
	var refs []blockRef
	for _, block := range operation.Status.Blocks {
		if block.State == topology.OperationFailed {
			refs = append(refs, blockRef{operation.Status.ID, block.GroupID})
		}
	}
	return refs
}

func (s *scopedState) prepareRetry(previous *topology.Operation, p RetryOperationProposal) (*scopedState, error) {
	retry := previous.Clone()
	retry.Previous, retry.Status.Attempt = previous.Status.ID, previous.Status.Attempt+1
	retry.Status.ID = topology.MakeOperationID(retry.Status.Version, retry.Status.Attempt)
	retry.StartedAt, retry.UpdatedAt = p.At, p.At
	retry.Status.State, retry.Status.Error, retry.Status.ErrorCode = topology.OperationPending, "", ""
	catalog := s.authorityCatalog()
	for i, block := range previous.Status.Blocks {
		if block.State == topology.OperationFailed {
			retry.Status.Blocks[i] = topology.BlockStatus{GroupID: block.GroupID, State: topology.OperationPending,
				TotalSteps: block.TotalSteps, CurrentStep: firstRetryStep(previous, i, s.indexes, catalog)}
		}
	}
	next := s.cloneExecutionState()
	next.Operations = append(next.Operations, retry)
	failed := failedBlockRefs(previous)
	for i := range next.Groups {
		row := &next.Groups[i]
		if slices.ContainsFunc(failed, func(ref blockRef) bool { return ref.GroupID == row.GroupID }) {
			row.Owner = retry.Status.ID
		}
	}
	if err := prepareAdmissionHandovers(s, next); err != nil {
		return nil, err
	}
	if err := next.rebindRetryStops(previous, &retry); err != nil {
		return nil, err
	}
	if err := next.validateRetrySuffix(&retry); err != nil {
		return nil, err
	}
	return next, nil
}

func firstRetryStep(op *topology.Operation, position int, index *scopedIndexes, catalog authorityCatalog) int {
	last := 0
	for _, seq := range op.Plan.Blocks[position].Steps {
		last = seq
		if !retryStepConsumed(op, op.Plan.Steps[seq-1], index, catalog) {
			return seq
		}
	}
	return last // Consumed final step still requires final verification and completion.
}

func retryStepConsumed(op *topology.Operation, step topology.Step,
	index *scopedIndexes, catalog authorityCatalog) bool {
	if step.Kind == topology.StepWaitCaughtUp {
		for _, block := range op.Status.Blocks {
			if block.GroupID == step.GroupID {
				return scopedStepCompleted(block, step.Seq)
			}
		}
		return false
	}
	return scopedStepConsumed(op, step, index, catalog)
}

func (s *scopedState) recordRetryPrefix(index uint64, previous, retry *topology.Operation) error {
	for i, old := range previous.Status.Blocks {
		if old.State == topology.OperationFailed && old.CurrentStep != 0 &&
			retry.Status.Blocks[i].CurrentStep > old.CurrentStep {
			if err := s.recordScopedTransitionEffect(index, previous, old.CurrentStep, retry.UpdatedAt); err != nil {
				return err
			}
		}
	}
	return nil
}
