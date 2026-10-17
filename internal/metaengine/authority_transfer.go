package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
)

// GrantTransferProposal requests authority for an exact direct
// leadership-transfer request.
type GrantTransferProposal struct {
	ExpectedSourceVersion uint64 `json:"expected_source_version"`
	GroupID               string `json:"group_id"`
	NodeName              string `json:"node_name"`
	ReplicaID             uint64 `json:"replica_id"`
	RequestID             string `json:"request_id"`
	ProposalID            string `json:"proposal_id"`
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
