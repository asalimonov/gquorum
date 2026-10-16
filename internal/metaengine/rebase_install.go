package metaengine

import (
	"cmp"
	"slices"

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
	for _, doc := range []topology.Document{pair.Repair, pair.Latest} {
		s.rememberScopedAllocations(doc)
	}
}

func (s *scopedState) rememberScopedAllocations(doc topology.Document) {
	s.Allocations = slices.Clone(s.Allocations)
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
