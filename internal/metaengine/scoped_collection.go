package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// The caller owns the FSM read lock after the existing quorum/apply barrier.
// This is absence at one applied index, never an acknowledgement of collection.
func (s *scopedState) collectionCheckLocked(id storage.Identity) CollectionCheck {
	return CollectionCheck{AppliedIndex: s.AppliedIndex,
		Untracked: s.indexes.cleanup[id] == nil && !s.identityReferencedLocked(id)}
}

func queryScopedCollection(state *scopedState, query QueryRequest) (CollectionCheck, error) {
	if state == nil {
		return CollectionCheck{}, fault.ErrUnsupported
	}
	if err := query.validateCollectionFields(); err != nil {
		return CollectionCheck{}, err
	}
	if query.CollectionIdentity.ClusterID != state.Target.ClusterID {
		return CollectionCheck{}, fault.ErrIdentityMismatch
	}
	return state.collectionCheckLocked(*query.CollectionIdentity), nil
}

func (s *scopedState) applyRecordCollection(index uint64, p RecordCollectionProposal) (proposalApplication, error) {
	if err := p.Receipt.Validate(); err != nil {
		return proposalApplication{}, err
	}
	if p.Receipt.Identity.ClusterID != s.Target.ClusterID || p.At.IsZero() {
		return proposalApplication{}, fault.ErrInvalid
	}
	debt := s.indexes.cleanup[p.Receipt.Identity]
	if debt == nil {
		return proposalApplication{}, fault.ErrHistoryUnavailable // Recheck absence through the safe-read path.
	}
	if err := s.validateCollectionProof(*debt, p); err != nil {
		return proposalApplication{}, err
	}
	if debt.Collection != nil {
		if !debt.Collection.Equal(p.Receipt) {
			return proposalApplication{}, fault.ErrIdentityMismatch
		}
		return collectionAcceptance(index, p.ProposalID, *debt.Collection), nil
	}
	if index <= s.AppliedIndex {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	next := *s
	next.Cleanup = slices.Clone(s.Cleanup)
	for i := range next.Cleanup {
		row := &next.Cleanup[i]
		if row.Identity == p.Receipt.Identity {
			row.Collection = copyCleanupValue(&p.Receipt)
			row.UpdatedAt = latestTimestamp(p.At, row.UpdatedAt)
			row.NotBefore, row.Blocked = nil, nil
		}
	}
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	result := collectionAcceptance(index, p.ProposalID, p.Receipt)
	result.changed = true
	return result, nil
}

func collectionAcceptance(index uint64, proposal string, receipt authoritystore.CollectionReceipt) proposalApplication {
	return proposalApplication{result: ApplyResult{Accepted: true, ProposalID: proposal,
		Collection: &authoritystore.CollectionResult{Receipt: receipt, AppliedIndex: index}}}
}

func (s *scopedState) validateCollectionProof(debt cleanupRecord, p RecordCollectionProposal) error {
	if debt.Storage == topology.StorageMemory || debt.MemberID != p.Receipt.MemberID {
		return fault.ErrOperationAuthorization
	}
	check := scopedCleanupValidation{s, s.indexes, s.authorityCatalog()}
	if !check.declaration(debt) || !check.actions(debt) || !check.milestones(debt) {
		return fault.ErrOperationAuthorization
	}
	if err := check.collectionRetirement(debt, p.Receipt.RetirementKey); err != nil {
		return err
	}
	return s.validateCollectionCourier(debt, p)
}
