package metaengine

import (
	"cmp"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyCommitRepairOperation(index uint64,
	p CommitRepairOperationProposal) (proposalApplication, error) {
	if p.Intent != nil && p.ExpectedVersion != s.Latest.Version {
		result, err := retainedMutation(ProposeDocument{ExpectedVersion: p.ExpectedVersion,
			Intent: p.Intent, ProposalID: p.ProposalID}, s.Latest.Version, s.indexes)
		return proposalApplication{result: result}, err
	}
	if p.ExpectedVersion != s.Latest.Version || p.ExpectedTarget != s.Target.Version {
		return s.repairConflict(p), nil
	}
	pair, err := s.prepareRepairDocuments(p.Doc, p.At)
	if err != nil {
		return proposalApplication{}, err
	}
	if err := s.validateScopedReplicaRepair(pair.Repair); err != nil {
		return proposalApplication{}, err
	}
	if p.Intent != nil {
		if p.Intent.Kind != intentReseatReplica || pair.Repair.Origin != topology.OriginOperator {
			return proposalApplication{}, fault.ErrInvalid
		}
		if err := validateProposedIntent(*p.Intent, s.Latest, pair.Repair, s.Target.Version); err != nil {
			return proposalApplication{}, err
		}
	}
	return s.commitRepairDocuments(index, pair, p)
}

func (s *scopedState) repairConflict(p CommitRepairOperationProposal) proposalApplication {
	expected, current := p.ExpectedVersion, s.Latest.Version
	if expected == current {
		expected, current = p.ExpectedTarget, s.Target.Version
	}
	return proposalApplication{result: rejectVersionConflict(p.ProposalID, expected, current)}
}

// Reuse the narrow reseat guard with canonical order and the retained allocator.
// Only generated document metadata differs from the legacy whole-document path.
func (s *scopedState) validateScopedReplicaRepair(repair topology.Document) error {
	if repair.Origin != topology.OriginReconciler && repair.Origin != topology.OriginOperator {
		return fault.ErrOperationAuthorization
	}
	base, ordered := repairComparison(s.Target), repairComparison(repair)
	base.CreatedAt, base.RebasedFrom = repair.CreatedAt, 0
	// Independent staged allocation belongs to S, not R. Only the replaced
	// group's allocator participates in the narrow repair comparison.
	for _, floor := range s.Allocations {
		group := base.Group(floor.GroupID)
		if group != nil && !reflect.DeepEqual(group, ordered.Group(floor.GroupID)) {
			group.NextReplicaID = max(group.NextReplicaID, floor.NextReplicaID)
		}
	}
	if err := validateSingleReplicaRepair(base, ordered); err != nil {
		return fault.New(fault.OperationAuthorization, "repair must change only one exact approved load voter", err)
	}
	return nil
}

func repairComparison(doc topology.Document) topology.Document {
	owned := doc.Clone()
	canonicalizeDocument(&owned)
	for i := range owned.Groups {
		slices.SortFunc(owned.Groups[i].Replicas, func(a, b topology.Replica) int {
			return cmp.Compare(a.NodeName, b.NodeName)
		})
	}
	return owned
}

func (s *scopedState) commitRepairDocuments(index uint64, pair repairDocuments,
	p CommitRepairOperationProposal) (proposalApplication, error) {
	approval := BeginOperationProposal{Version: pair.Repair.Version, Plan: p.Plan, Scopes: p.Scopes,
		Actor: pair.Repair.ChangedBy, Origin: pair.Repair.Origin, At: p.At, ProposalID: p.ProposalID}
	// Convenience reseat and scheduling both refuse to seize an owned scope.
	for _, scope := range p.Scopes {
		if row := s.indexes.groups[scope.GroupID]; row != nil && row.Owner != "" && scope.matches(row) {
			result := rejectProposal(p.ProposalID, proposalAuthorityPending,
				"group %q is owned by operation %s", scope.GroupID, row.Owner)
			result.OperationID = topology.OperationID(row.Owner)
			return proposalApplication{result: result}, nil
		}
	}
	next, err := prepareScopedApproval(s, pair.Repair, approval, automaticApproval)
	if err != nil {
		return proposalApplication{}, err
	}
	next.installRepairDocuments(pair)
	if p.Intent != nil {
		next.Intents = append(slices.Clone(next.Intents), *p.Intent)
	}
	op := &next.Operations[len(next.Operations)-1]
	err = next.revokeAuthorityLocked(index, s.supersededBlocks(op), op.Status.ID, DeletionSemanticsLegacy)
	if err != nil {
		return proposalApplication{}, err
	}
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	result := scopedApprovalResult(p.ProposalID, op.Status.ID)
	result.Doc = pair.Repair.Clone()
	*s = *next
	return proposalApplication{result: result, changed: true}, nil
}
