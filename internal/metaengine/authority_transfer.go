package metaengine

import (
	"context"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type GrantTransferProposal struct {
	ExpectedActiveVersion uint64 `json:"expected_active_version"`
	GroupID               string `json:"group_id"`
	NodeName              string `json:"node_name"`
	ReplicaID             uint64 `json:"replica_id"`
	RequestID             string `json:"request_id"`
	ProposalID            string `json:"proposal_id"`
}

func (sm *topologySM) applyGrantTransfer(index uint64, p GrantTransferProposal) (ApplyResult, bool) {
	refuse := func(code proposalErrorCode, reason string) (ApplyResult, bool) {
		return rejectProposal(p.ProposalID, code, "%s", reason), false
	}
	if strings.TrimSpace(p.RequestID) == "" {
		return refuse(proposalAuthorization, "transfer requires a request identity")
	}
	a := topology.AuthorizedStep{Version: p.ExpectedActiveVersion, GroupID: p.GroupID, NodeName: p.NodeName, ReplicaID: p.ReplicaID, Kind: topology.StepTransferLeader}
	for _, r := range sm.authority.records {
		if r.Grant.Purpose != topology.AuthorityTransfer || r.Grant.RequestID != p.RequestID {
			continue
		}
		if r.Grant.Action == nil || *r.Grant.Action != a {
			return refuse(proposalAuthorization, "transfer retry changed exact action")
		}
		if r.Closed {
			return refuse(proposalAuthorityFenced, "transfer authority was revoked")
		}
		grant := r.Grant.Clone()
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &grant}, false
	}
	if err := sm.authorizeLocked(context.Background(), a); err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	if sm.hasNonterminalOperatorLocked() || sm.target.Version != sm.active.Version {
		return refuse(proposalAuthorityPending, "topology execution owns administrative authority")
	}
	for _, r := range sm.authority.revocations {
		if !revokedComplete(r) {
			return refuse(proposalAuthorityPending, "revocation is pending")
		}
	}
	resources, targets, err := sm.grantResourcesLocked(a)
	if err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	for _, resource := range resources {
		if old := sm.authority.records[sm.authority.floors[resource]]; old != nil && !resourceConsumed(old, resource) && !sm.authority.fenceSettled(old) {
			// Public callers do not retain the internal request ID when a wait
			// is canceled. Join the exact still-unconsumed transfer at the current
			// floor; retain its original request identity and durable receipt.
			if !old.Closed && old.Grant.Purpose == topology.AuthorityTransfer && old.Grant.Action != nil && *old.Grant.Action == a {
				grant := old.Grant.Clone()
				return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &grant}, false
			}
			return refuse(proposalAuthorityPending, "resource has an unfinished effect")
		}
	}
	if sm.authority.pendingCost()+3 > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return refuse(proposalOverloaded, fault.ErrOverloaded.Error())
	}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: sm.active.ClusterID, GroupID: p.GroupID, Revision: index}, Purpose: topology.AuthorityTransfer, Action: &a, RequestID: p.RequestID, Resources: resources, Targets: targets}
	if err := grant.Validate(); err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	sm.insertGrantLocked(grant)
	owned := grant.Clone()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &owned}, true
}
