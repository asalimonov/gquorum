package metaengine

import "github.com/asalimonov/gquorum/topology"

func (s *scopedState) applyBeginOperation(index uint64, p BeginOperationProposal) (proposalApplication, error) {
	if first := s.indexes.first[p.Version]; first != "" {
		return proposalApplication{result: scopedApprovalResult(p.ProposalID, first)}, nil
	}
	if p.Version != s.Latest.Version {
		result := rejectVersionConflict(p.ProposalID, p.Version, s.Latest.Version)
		result.RebasedTo = s.indexes.rebases[p.Version]
		return proposalApplication{result: result}, nil
	}
	next, err := prepareScopedApproval(s, s.Latest, p, explicitApproval)
	if err != nil {
		return proposalApplication{}, err
	}
	op := &next.Operations[len(next.Operations)-1]
	if err := next.revokeAuthorityLocked(index, next.supersededBlocks(op), op.Status.ID); err != nil {
		return proposalApplication{}, err
	}
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	result := scopedApprovalResult(p.ProposalID, op.Status.ID)
	*s = *next // Every check and fallible preparation precedes this installation.
	return proposalApplication{result: result, changed: true}, nil
}

func scopedApprovalResult(id string, operation topology.OperationID) ApplyResult {
	// The existing result codec is renamed with its consumers at Task 78.
	return ApplyResult{ProposalID: id, Accepted: true, OperatorID: topology.OperatorID(operation)}
}
