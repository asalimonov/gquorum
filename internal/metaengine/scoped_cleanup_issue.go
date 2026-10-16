package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyBeginCleanupEffect(index uint64,
	p BeginCleanupEffectProposal) (proposalApplication, error) {
	debt, err := s.cleanupDebtForAction(p.Identity, p.Action)
	if err != nil {
		return proposalApplication{}, err
	}
	target := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	host := topology.AuthorityResource{Kind: topology.AuthorityHost,
		GroupID: debt.Identity.GroupID, NodeName: debt.Identity.NodeName}
	catalog := s.authorityCatalog()
	if record := catalog.cleanupGrant(p.Action, target); record != nil {
		if !record.Closed && (catalog.floors[host] == record.Grant.Key || resourceConsumed(record, host)) {
			grant := record.Grant.Clone()
			return proposalApplication{result: ApplyResult{
				Accepted: true, ProposalID: p.ProposalID, Grant: &grant}}, nil
		}
		if !catalog.resourceSettled(record, host) {
			return proposalApplication{}, fault.ErrAuthorityPending
		}
	}
	if cleanupActionConsumed(debt, p.Action) {
		return proposalApplication{}, fault.ErrOperationAuthorization // Retained proof already ends this obligation.
	}
	if err := s.resourcesAvailable([]topology.AuthorityResource{host}); err != nil {
		return proposalApplication{}, err
	}
	action := p.Action
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: debt.Identity.ClusterID,
		GroupID: debt.Identity.GroupID, Revision: index}, Purpose: topology.AuthorityEffect,
		OperatorID: action.OperatorID, Action: &action,
		Resources: []topology.AuthorityResource{host}, Targets: []topology.AuthorityTarget{target}}
	if err := s.installGrants(index, []topology.AuthorityGrant{grant}); err != nil {
		return proposalApplication{}, err
	}
	return proposalApplication{changed: true,
		result: ApplyResult{Accepted: true, ProposalID: p.ProposalID, Grant: &grant}}, nil
}

func (a authorityCatalog) cleanupGrant(action topology.AuthorizedStep,
	target topology.AuthorityTarget) *authorityRecord {
	var selected *authorityRecord
	for _, record := range a.records {
		if (selected == nil || record.Grant.Key.Revision > selected.Grant.Key.Revision) &&
			record.Grant.Purpose == topology.AuthorityEffect && record.Grant.Action != nil &&
			*record.Grant.Action == action && slices.Contains(record.Grant.Targets, target) {
			selected = record
		}
	}
	return selected
}
