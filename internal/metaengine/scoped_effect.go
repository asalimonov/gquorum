package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyBeginEffect(index uint64, p BeginEffectProposal) (proposalApplication, error) {
	if p.Action.StepSeq == 0 {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	if err := s.authorizeLocked(context.Background(), p.Action); err != nil {
		return proposalApplication{}, err
	}
	catalog := s.authorityCatalog()
	if record := catalog.records[catalog.actions[p.Action]]; record != nil {
		if err := catalog.currentGrant(record); err != nil {
			return proposalApplication{}, err
		}
		grant := record.Grant.Clone()
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &grant}}, nil
	}
	resources, err := s.grantResources(p.Action)
	if err != nil {
		return proposalApplication{}, err
	}
	if err := s.resourcesAvailable(resources.Resources); err != nil {
		return proposalApplication{}, err
	}
	action := p.Action
	grant := topology.AuthorityGrant{Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action,
		Key:       topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: action.GroupID, Revision: index},
		Resources: resources.Resources, Targets: resources.Targets}
	if err := s.installGrants(index, []topology.AuthorityGrant{grant}); err != nil {
		return proposalApplication{}, err
	}
	owned := grant.Clone()
	return proposalApplication{changed: true,
		result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &owned}}, nil
}

func (a authorityCatalog) currentGrant(record *authorityRecord) error {
	if record.Closed {
		return fault.ErrAuthorityFenced
	}
	for _, resource := range record.Grant.Resources {
		if a.floors[resource] != record.Grant.Key && !resourceConsumed(record, resource) {
			return fault.ErrAuthorityFenced
		}
	}
	return nil
}

func (s *scopedState) resourcesAvailable(resources []topology.AuthorityResource) error {
	for _, resource := range resources {
		if len(s.indexes.obligations[resource]) != 0 {
			return fault.ErrAuthorityPending
		}
	}
	return nil
}

func (s *scopedState) installGrants(index uint64, grants []topology.AuthorityGrant) error {
	if index <= s.AppliedIndex || len(grants) == 0 {
		return fault.ErrOperationAuthorization
	}
	catalog := s.authorityCatalog().clone()
	for _, grant := range grants {
		if grant.Key.Revision != index || catalog.records[grant.Key] != nil {
			return fault.ErrOperationAuthorization
		}
		if err := grant.Validate(); err != nil {
			return err
		}
		catalog.records[grant.Key] = &authorityRecord{Grant: grant.Clone()}
		for _, resource := range grant.Resources {
			catalog.floors[resource] = grant.Key
		}
	}
	next := *s
	update := scopedRevocation{state: &next, catalog: catalog,
		requested: make(map[blockRef]*scopedRevocationRecord)}
	update.copyRevocations()
	update.publish()
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return err
	}
	*s = next
	return nil
}
