package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyGrantTransfer(index uint64, p GrantTransferProposal) (proposalApplication, error) {
	if strings.TrimSpace(p.RequestID) == "" {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	// Rename the legacy payload field with its wire consumers at Task 78. In
	// scoped state it names this group's proven source, never global completion.
	action := topology.AuthorizedStep{Version: p.ExpectedActiveVersion, GroupID: p.GroupID,
		NodeName: p.NodeName, ReplicaID: p.ReplicaID, Kind: topology.StepTransferLeader}
	catalog := s.authorityCatalog()
	if prior := catalog.retainedTransferRequest(p, action); prior != nil {
		return proposalApplication{result: *prior}, nil
	}
	if err := s.authorizeDirectTransfer(action); err != nil {
		return proposalApplication{}, err
	}
	resources, err := s.grantResources(action)
	if err != nil {
		return proposalApplication{}, err
	}
	prior, blocked := catalog.unfinishedTransfer(resources.Resources, action)
	if blocked {
		return proposalApplication{}, fault.ErrAuthorityPending
	}
	if prior != nil {
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: prior}}, nil
	}
	if err := s.resourcesAvailable(resources.Resources); err != nil {
		return proposalApplication{}, err
	}
	grant := topology.AuthorityGrant{Purpose: topology.AuthorityTransfer, RequestID: p.RequestID, Action: &action,
		Key:       topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: p.GroupID, Revision: index},
		Resources: resources.Resources, Targets: resources.Targets}
	if err := s.installGrants(index, []topology.AuthorityGrant{grant}); err != nil {
		return proposalApplication{}, err
	}
	owned := grant.Clone()
	return proposalApplication{changed: true,
		result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &owned}}, nil
}
