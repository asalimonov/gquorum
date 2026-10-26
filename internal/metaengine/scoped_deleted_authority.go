package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// Native settlement requires the permanent logical tombstone and actual stops
// for every retained owner, including never-converged founding targets. Logical
// deletion alone cannot manufacture a Raft fence receipt.
// Host floors must remain independent: they reject delayed old start requests.
func (s *scopedState) completedDeletions() map[string]bool {
	retired := make(map[string]uint64, len(s.RetiredGroups))
	for _, row := range s.RetiredGroups {
		retired[row.ID] = row.Version
	}
	var completed map[string]bool
	for _, row := range s.Groups {
		version := retired[row.GroupID]
		if version == 0 || row.SourceVersion < version || row.Declaration != nil || row.Deleting ||
			row.Owner != "" || row.GroupID == topology.MetaGroupID || row.CompletedAt.IsZero() {
			continue
		}
		if completed == nil {
			completed = make(map[string]bool)
		}
		completed[row.GroupID] = true
	}
	for _, debt := range s.Cleanup {
		if debt.Stopped == nil {
			delete(completed, debt.Identity.GroupID)
		}
	}
	return completed
}

func (a authorityCatalog) deletedResource(resource topology.AuthorityResource) bool {
	return resource.Kind == topology.AuthorityGroup && a.deletedGroups[resource.GroupID]
}

func (a authorityCatalog) liveObligations(obligations []topology.FenceObligation) []topology.FenceObligation {
	return slices.DeleteFunc(slices.Clone(obligations), func(ob topology.FenceObligation) bool {
		return a.deletedResource(ob.Resource)
	})
}
