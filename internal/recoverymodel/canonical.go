package recoverymodel

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// CanonicalAccepted owns and orders identity-keyed evidence before approval.
func CanonicalAccepted(input AcceptedPlan) AcceptedPlan {
	p := CloneAcceptedPlan(input)
	slices.Sort(p.Request.RequiredGroups)
	slices.Sort(p.Retired)
	slices.SortFunc(p.Floors, func(a, b AllocationFloor) int { return cmp.Compare(a.GroupID, b.GroupID) })
	slices.SortFunc(p.Sources, func(a, b SeedSource) int { return cmp.Compare(a.GroupID, b.GroupID) })
	slices.SortFunc(p.Groups, func(a, b GroupRecovery) int { return cmp.Compare(a.GroupID, b.GroupID) })
	slices.SortFunc(p.Issues, func(a, b Issue) int {
		return cmp.Or(cmp.Compare(a.GroupID, b.GroupID), cmp.Compare(a.Code, b.Code), cmp.Compare(a.Message, b.Message))
	})
	slices.SortFunc(p.Archive, func(a, b ArchiveEntry) int {
		return cmp.Or(cmp.Compare(a.Root, b.Root),
			cmp.Compare(a.Name, b.Name))
	})
	slices.SortFunc(p.Approved.Groups, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	return p
}
