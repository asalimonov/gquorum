package metaengine

import (
	"context"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// EffectReceipts returns owned immutable consumption evidence. A positive local
// result remains proof after leadership changes; progress still commits through
// the FSM's exact operator and receipt checks.
func (m *Machine) EffectReceipts(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	record := m.authority.records[grant.Key]
	if record == nil || !record.Grant.Equal(grant) {
		return nil, fault.ErrOperatorAuthorization
	}
	result := make([]topology.EffectReceipt, len(record.Effects))
	for i, receipt := range record.Effects {
		result[i] = receipt.Clone()
	}
	return result, nil
}

// RecordFenceProposal records exact resource fence installation evidence.
type RecordFenceProposal struct {
	Evidence   topology.FenceEvidence `json:"evidence"`
	ProposalID string                 `json:"proposal_id"`
}

// RecordEffectProposal records durable consumption of an exact granted effect.
type RecordEffectProposal struct {
	Receipt    topology.EffectReceipt `json:"receipt"`
	ProposalID string                 `json:"proposal_id"`
}

func sameFenceBoundary(a, b topology.FenceEvidence) bool {
	if a.Key != b.Key || a.Resource != b.Resource || a.AppliedIndex != b.AppliedIndex ||
		a.AppliedTerm != b.AppliedTerm || a.LocalSequence != b.LocalSequence {
		return false
	}
	return a.Resource.Kind == topology.AuthorityGroup || a.Receiver.MemberID == b.Receiver.MemberID
}

func (sm *topologySM) applyRecordFence(p RecordFenceProposal) (ApplyResult, bool) {
	r := sm.authority.records[p.Evidence.Key]
	if r == nil || p.Evidence.Validate(r.Grant) != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"fence evidence does not identify its retained grant"), false
	}
	for _, old := range r.Fences {
		if old.Resource == p.Evidence.Resource {
			if !sameFenceBoundary(old, p.Evidence) {
				return rejectProposal(p.ProposalID, proposalAuthorization, "contradictory fence boundary"), false
			}
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, false
		}
	}
	if reason := sm.authority.fenceEvidenceConflict(p.Evidence); reason != "" {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%s", reason), false
	}
	r.Fences = append(r.Fences, p.Evidence)
	slices.SortFunc(r.Fences, func(a, b topology.FenceEvidence) int { return a.Resource.Compare(b.Resource) })
	sm.authority.refreshRevocations()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, true
}

func (a authorityCatalog) fenceEvidenceConflict(evidence topology.FenceEvidence) string {
	for _, record := range a.records {
		for _, old := range record.Fences {
			if !orderedFenceBoundaries(old, evidence) {
				return "regressing fence boundary"
			}
		}
		for _, receipt := range record.Effects {
			if !receiptFitsFence(receipt, evidence) {
				return "fence contradicts recorded effect"
			}
		}
	}
	return ""
}

func (sm *topologySM) applyRecordEffect(p RecordEffectProposal) (ApplyResult, bool) {
	r := sm.authority.records[p.Receipt.Key]
	if r == nil || p.Receipt.Validate(r.Grant) != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"effect receipt does not identify its retained grant"), false
	}
	for _, old := range r.Effects {
		if old.Resource == p.Receipt.Resource && (old.Target == p.Receipt.Target ||
			old.Resource.Kind == topology.AuthorityGroup) {
			if !reflect.DeepEqual(old, p.Receipt) {
				return rejectProposal(p.ProposalID, proposalAuthorization, "contradictory effect receipt"), false
			}
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, false
		}
	}
	if !sm.authority.receiptFitsInstalledFences(p.Receipt) {
		return rejectProposal(p.ProposalID, proposalAuthorization, "effect contradicts installed authority"), false
	}
	r.Effects = append(r.Effects, p.Receipt.Clone())
	slices.SortFunc(r.Effects, func(a, b topology.EffectReceipt) int {
		if c := a.Resource.Compare(b.Resource); c != 0 {
			return c
		}
		return a.Target.Compare(b.Target)
	})
	sm.authority.refreshRevocations()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, true
}

func (a authorityCatalog) receiptFitsInstalledFences(receipt topology.EffectReceipt) bool {
	for _, record := range a.records {
		for _, fence := range record.Fences {
			if !receiptFitsFence(receipt, fence) {
				return false
			}
		}
	}
	return true
}

// PendingFences returns outstanding resource fence obligations from retained
// revocations.
func (m *Machine) PendingFences() []topology.FenceObligation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var pending []topology.FenceObligation
	for _, r := range m.authority.revocations {
		for _, ob := range r.Obligations {
			if ob.Evidence == nil {
				pending = appendObligation(pending, ob)
			}
		}
	}
	return pending
}

// AuthorityStatus reports retained revocation progress for an operator.
func (m *Machine) AuthorityStatus(id topology.OperatorID) (topology.AuthorityStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.authorityStatusLocked(id)
}

func (sm *topologySM) authorityStatusLocked(id topology.OperatorID) (topology.AuthorityStatus, error) {
	r := sm.authority.revocations[id]
	if r == nil {
		if sm.operators[id] == nil {
			return topology.AuthorityStatus{}, fault.ErrHistoryUnavailable
		}
		return topology.AuthorityStatus{OperatorID: id}, nil
	}
	return (topology.AuthorityStatus{OperatorID: id, RevocationRequested: r.Requested,
		RevocationComplete: r.Requested &&
			revokedComplete(r), Obligations: r.Obligations}).Clone(), nil
}
