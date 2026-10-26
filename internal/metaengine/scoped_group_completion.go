package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Prepares owned group/debt rows only. The completion caller adds operation,
// admission, node and marker changes and validates the entire graph before
// installation; a partially completed group is never published.
func (s *scopedState) prepareGroupCompletion(t topology.OperationTransition) (*scopedState, error) {
	op, position := s.transitionBlock(t)
	if op == nil || t.State != topology.OperationSucceeded || t.At.IsZero() || t.At.Before(op.UpdatedAt) {
		return nil, fault.ErrOperationAuthorization
	}
	if err := s.validateScopedTransition(op, t); err != nil {
		return nil, err
	}
	if err := s.validateGroupCompletion(op, op.Plan.Blocks[position]); err != nil {
		return nil, err
	}
	approved := s.indexes.documents[op.Status.Version]
	next := s.cloneExecutionState()
	if approved.Group(t.GroupID) == nil {
		collector := s.planningCollector()
		input := s.planningDeclarations()
		if _, err := collector.collect(input, s.Target); err != nil {
			return nil, err
		}
		collector.markConvergedOwners(input.Groups)
		preparation := deletionPreparation{source: s, next: next, collector: collector}
		if err := preparation.complete(t.GroupID, t.At); err != nil {
			return nil, err
		}
	} else if len(op.Plan.Blocks[position].Steps) == 0 &&
		!zeroStepMembership(s, approved, t.GroupID, s.authorityCatalog()) {
		return nil, fault.New(fault.Unavailable, "required effects do not prove the approved membership", nil)
	}
	for i := range next.Groups {
		row := &next.Groups[i]
		if row.GroupID != t.GroupID {
			continue
		}
		if t.At.Before(row.CompletedAt) {
			return nil, fault.ErrOperationAuthorization
		}
		*row = groupConvergence{GroupID: t.GroupID, SourceVersion: op.Status.Version, CompletedAt: t.At,
			Declaration: (topology.GroupProgress{Converged: approved.Group(t.GroupID)}).Clone().Converged}
	}
	return next, nil
}

func (s *scopedState) validateGroupCompletion(op *topology.Operation, block topology.PlanBlock) error {
	catalog := s.authorityCatalog()
	for _, seq := range block.Steps {
		step := op.Plan.Steps[seq-1]
		if !scopedStepConsumed(op, step, s.indexes, catalog) {
			return fault.ErrAuthorityPending
		}
		if err := s.validateActionBarriers(op, scopedPlanAction(op, step)); err != nil {
			return err
		}
		if step.Kind == topology.StepWaitCaughtUp {
			continue
		}
		record := scopedConsumedAuthority(op, step, s.indexes, catalog)
		if err := s.completionResourcesAvailable(record.Grant.Resources); err != nil {
			return err
		}
	}
	return nil
}

func (s *scopedState) completionResourcesAvailable(resources []topology.AuthorityResource) error {
	for _, resource := range resources {
		for _, key := range s.indexes.obligations[resource] {
			record := s.indexes.grants[key]
			if s.stoppedGrantTargets(record) {
				continue
			}
			// Required host effects have already been proven consumed. A later
			// detached retirement on that host is not part of group convergence.
			if record != nil && record.Class == "cleanup" && record.Grant.Purpose == topology.AuthorityEffect &&
				slices.Contains([]topology.StepKind{topology.StepCleanupReplicaState,
					topology.StepDeleteGroupState}, record.Grant.Action.Kind) {
				continue
			}
			return fault.ErrAuthorityPending
		}
	}
	return nil
}

// A later Stop of the same retired identities cannot invalidate an earlier
// durable stop. Native/preparation/foreign targets still require reconciliation.
func (s *scopedState) stoppedGrantTargets(record *scopedAuthorityRecord) bool {
	if record == nil || record.Grant.Purpose != topology.AuthorityEffect ||
		record.Grant.Action.Kind != topology.StepStopReplica {
		return false
	}
	for _, target := range record.Grant.Targets {
		id := storage.Identity{ClusterID: record.Grant.Key.ClusterID, GroupID: record.Grant.Key.GroupID,
			NodeName: target.NodeName, ReplicaID: target.ReplicaID}
		debt := s.indexes.cleanup[id]
		if debt == nil || debt.MemberID != target.MemberID || debt.Stopped == nil ||
			debt.Stopped.Target != target || !cleanupRetainsAction(debt, *record.Grant.Action) {
			return false
		}
	}
	return len(record.Grant.Targets) != 0
}
