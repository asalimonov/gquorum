package metaengine

import "github.com/asalimonov/gquorum/topology"

func (s *scopedState) applyBeginOperation(index uint64, p BeginOperationProposal) (proposalApplication, error) {
	if first := s.indexes.first[p.Version]; first != "" {
		if !s.indexes.operations[first].Plan.Equal(p.Plan) {
			result := rejectVersionConflict(p.ProposalID, p.Version, s.Latest.Version)
			result.OperationID = first
			return proposalApplication{result: result}, nil
		}
		return proposalApplication{result: scopedApprovalResult(p.ProposalID, first)}, nil
	}
	view := topology.ReadView{Latest: s.Latest, Target: s.Target}
	if _, err := retainedApproval(p.Version, view, s.indexes); err != nil {
		return proposalApplication{}, err
	}
	next, err := prepareScopedApproval(s, s.Latest, p, explicitApproval)
	if err != nil {
		return proposalApplication{}, err
	}
	op := &next.Operations[len(next.Operations)-1]
	if err := next.revokeAuthorityLocked(index, s.supersededBlocks(op), op.Status.ID, p.DeletionSemantics); err != nil {
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
	return ApplyResult{ProposalID: id, Accepted: true, OperationID: operation}
}
