package metaengine

import (
	"context"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// GrantTransferProposal requests authority for an exact direct
// leadership-transfer request.
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
	a := topology.AuthorizedStep{
		Version:   p.ExpectedActiveVersion,
		GroupID:   p.GroupID,
		NodeName:  p.NodeName,
		ReplicaID: p.ReplicaID,
		Kind:      topology.StepTransferLeader,
	}
	if prior := sm.authority.retainedTransferRequest(p, a); prior != nil {
		return *prior, false
	}
	if err := sm.authorizeLocked(context.Background(), a); err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	if sm.target.Version != sm.active.Version || sm.hasNonterminalOperatorLocked() {
		return refuse(proposalAuthorityPending, "topology execution owns administrative authority")
	}
	for _, r := range sm.authority.revocations {
		if !revokedComplete(r) {
			return refuse(proposalAuthorityPending, "revocation is pending")
		}
	}
	resources, err := sm.grantResourcesLocked(a)
	if err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	if prior, blocked := sm.authority.unfinishedTransfer(resources.Resources, a); blocked {
		return refuse(proposalAuthorityPending, "resource has an unfinished effect")
	} else if prior != nil {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: prior}, false
	}
	if sm.authority.pendingCost()+newAuthorityGrantCost > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return refuse(proposalOverloaded, fault.ErrOverloaded.Error())
	}
	grant := topology.AuthorityGrant{
		Key:       topology.AuthorityKey{ClusterID: sm.active.ClusterID, GroupID: p.GroupID, Revision: index},
		Purpose:   topology.AuthorityTransfer,
		Action:    &a,
		RequestID: p.RequestID,
		Resources: resources.Resources,
		Targets:   resources.Targets,
	}
	if err := grant.Validate(); err != nil {
		return refuse(proposalAuthorization, err.Error())
	}
	sm.insertGrantLocked(grant)
	owned := grant.Clone()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &owned}, true
}

func (a authorityCatalog) retainedTransferRequest(p GrantTransferProposal,
	action topology.AuthorizedStep) *ApplyResult {
	for _, r := range a.records {
		if r.Grant.Purpose != topology.AuthorityTransfer || r.Grant.RequestID != p.RequestID {
			continue
		}
		if r.Grant.Action == nil || *r.Grant.Action != action {
			result := rejectProposal(p.ProposalID, proposalAuthorization, "%s", "transfer retry changed exact action")
			return &result
		}
		if r.Closed {
			result := rejectProposal(p.ProposalID, proposalAuthorityFenced, "%s", "transfer authority was revoked")
			return &result
		}
		grant := r.Grant.Clone()
		result := ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &grant}
		return &result
	}
	return nil
}

func (a authorityCatalog) unfinishedTransfer(resources []topology.AuthorityResource,
	action topology.AuthorizedStep) (*topology.AuthorityGrant, bool) {
	for _, resource := range resources {
		if old := a.records[a.floors[resource]]; old != nil && !resourceConsumed(old, resource) &&
			!a.fenceSettled(old) {
			// Public callers do not retain the internal request ID when a wait
			// is canceled. Join the exact still-unconsumed transfer at the
			// current
			// floor; retain its original request identity and durable receipt.
			if !old.Closed && old.Grant.Purpose == topology.AuthorityTransfer && old.Grant.Action != nil &&
				*old.Grant.Action == action {
				grant := old.Grant.Clone()
				return &grant, false
			}
			return nil, true
		}
	}
	return nil, false
}
