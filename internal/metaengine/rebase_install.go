package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Only call on the owned approval candidate, after preparing both documents.
// Task46 installs owners/authority and validates the entire graph in one Apply.
// Ordinary commitLatestLocked intentionally clears RebasedFrom and cannot be used here.
func (s *scopedState) installRepairDocuments(pair repairDocuments) {
	history := slices.Clone(s.History)
	for _, doc := range []topology.Document{s.Target, s.Latest, pair.Repair, pair.Latest} {
		if !slices.ContainsFunc(history, func(old topology.Document) bool { return old.Version == doc.Version }) {
			history = append(history, doc.Clone())
		}
	}
	slices.SortFunc(history, func(a, b topology.Document) int { return cmp.Compare(a.Version, b.Version) })
	s.History, s.Target, s.Latest = history, pair.Repair.Clone(), pair.Latest.Clone()
	s.Allocations = slices.Clone(s.Allocations)
	for _, doc := range []topology.Document{pair.Repair, pair.Latest} {
		for _, group := range doc.Groups {
			position := slices.IndexFunc(s.Allocations, func(row allocationRecord) bool {
				return row.GroupID == group.ID
			})
			if position < 0 {
				s.Allocations = append(s.Allocations, allocationRecord{group.ID, group.NextReplicaID})
			} else {
				s.Allocations[position].NextReplicaID = max(s.Allocations[position].NextReplicaID, group.NextReplicaID)
			}
		}
	}
}

// Task46 replaces this isolated preparation-only handler with scoped approval
// and one validated install. Even the test dispatcher must fail closed meanwhile.
func (s *scopedState) applyCommitRepairOperation(_ uint64,
	p CommitRepairOperationProposal) (proposalApplication, error) {
	if p.ExpectedVersion != s.Latest.Version || p.ExpectedTarget != s.Target.Version {
		return proposalApplication{}, fault.ErrVersionConflict
	}
	pair, err := s.prepareRepairDocuments(p.Doc, p.At)
	if err != nil {
		return proposalApplication{}, err
	}
	next := *s
	next.installRepairDocuments(pair)
	return proposalApplication{}, fault.New(fault.Unsupported, "repair approval installation is not connected", nil)
}
