package metaengine

import (
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) authorityClass(grant topology.AuthorityGrant) string {
	if grant.Action != nil {
		a := grant.Action
		if op := s.indexes.operations[topology.OperationID(a.OperationID)]; op != nil &&
			slices.ContainsFunc(op.Plan.Blocks, func(b topology.PlanBlock) bool {
				return b.GroupID == a.GroupID && slices.Contains(b.Cleanup, a.StepSeq)
			}) {
			return "cleanup"
		}
	}
	return "execution"
}

func (s *scopedState) cleanupDebtForAction(id storage.Identity,
	action topology.AuthorizedStep) (*cleanupRecord, error) {
	debt := s.indexes.cleanup[id]
	if debt == nil || id.ClusterID != s.Target.ClusterID || action.GroupID != id.GroupID ||
		!exactCleanupBinding(debt, action) {
		return nil, fault.ErrOperationAuthorization
	}
	op := s.indexes.operations[topology.OperationID(action.OperationID)]
	if op == nil || action.StepSeq < 1 || action.StepSeq > len(op.Plan.Steps) ||
		!action.Matches(topology.OperationID(op.Status.ID), op.Status.Attempt,
			op.Status.Version, op.Plan.Steps[action.StepSeq-1]) ||
		s.authorityClass(topology.AuthorityGrant{Action: &action}) != "cleanup" {
		return nil, fault.ErrOperationAuthorization
	}
	if replica := documentReplica(s.Target, id.GroupID, id.ReplicaID); replica != nil &&
		replica.NodeName == id.NodeName && s.Target.MemberID(id.NodeName) == debt.MemberID {
		return nil, fault.ErrOperationAuthorization
	}
	if s.cleanupWaitsForReadmission(debt) {
		return nil, fault.ErrAuthorityPending
	}
	if action.Kind != topology.StepStopReplica && debt.Stopped == nil {
		return nil, fault.ErrAuthorityPending
	}
	return debt, nil
}

func (s *scopedState) cleanupWaitsForReadmission(debt *cleanupRecord) bool {
	return debt.Identity.GroupID != topology.MetaGroupID && debt.Retired == nil &&
		s.Target.Group(debt.Identity.GroupID) == nil &&
		s.Target.MemberID(debt.Identity.NodeName) > debt.MemberID &&
		slices.ContainsFunc(s.RetiredGroups, func(row retiredGroup) bool { return row.ID == debt.Identity.GroupID })
}

func exactCleanupBinding(debt *cleanupRecord, action topology.AuthorizedStep) bool {
	return debt.StopAction != nil && action == *debt.StopAction ||
		debt.RetireAction != nil && action == *debt.RetireAction
}

func (s *scopedState) authorizeCleanupAction(action topology.AuthorizedStep) error {
	pending := false
	for _, debt := range s.Cleanup {
		if exactCleanupBinding(&debt, action) {
			_, err := s.cleanupDebtForAction(debt.Identity, action)
			if err == nil {
				return nil // The exact target is checked by issuance and retained-grant validation.
			}
			pending = pending || errors.Is(err, fault.ErrAuthorityPending)
		}
	}
	if pending {
		return fault.ErrAuthorityPending
	}
	return fault.ErrOperationAuthorization
}

func cleanupActionConsumed(debt *cleanupRecord, action topology.AuthorizedStep) bool {
	if action.Kind == topology.StepStopReplica {
		return debt.Stopped != nil
	}
	return debt.Retired != nil
}
