package metaengine

import (
	"reflect"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyRecordCleanupStatus(index uint64,
	p RecordCleanupStatusProposal) (proposalApplication, error) {
	debt := s.indexes.cleanup[p.Identity]
	if debt == nil || debt.MemberID != p.MemberID || p.At.IsZero() || p.NotBefore != nil && p.NotBefore.IsZero() ||
		p.Blocked != nil && strings.TrimSpace(p.Blocked.Code) == "" {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	check := scopedCleanupValidation{s, s.indexes, s.authorityCatalog()}
	if err := check.collectionRetirement(*debt, p.RetirementKey); err != nil {
		return proposalApplication{}, err
	}
	result := proposalApplication{result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}}
	if debt.Collection != nil || debt.Storage == topology.StorageMemory ||
		reflect.DeepEqual(debt.NotBefore, p.NotBefore) && reflect.DeepEqual(debt.Blocked, p.Blocked) {
		return result, nil
	}
	if index <= s.AppliedIndex || p.At.Before(debt.UpdatedAt) {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	next := *s
	next.Cleanup = slices.Clone(s.Cleanup)
	for i := range next.Cleanup {
		row := &next.Cleanup[i]
		if row.Identity == p.Identity {
			row.NotBefore, row.Blocked = copyCleanupValue(p.NotBefore), copyCleanupValue(p.Blocked)
			row.UpdatedAt = p.At
		}
	}
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s, result.changed = next, true
	return result, nil
}

func (s *scopedState) applyRecordFence(index uint64, p RecordFenceProposal) (proposalApplication, error) {
	catalog := s.authorityCatalog().clone()
	result, changed := catalog.recordFence(p)
	if !changed {
		return proposalApplication{result: result}, nil
	}
	if err := s.installAuthorityCatalog(index, catalog); err != nil {
		return proposalApplication{}, err
	}
	return proposalApplication{result: result, changed: true}, nil
}
