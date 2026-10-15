package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// Execution and cleanup use the issuer's current-floor and revocation rules.
// Old non-floor grants remain evidence, not independent issuance gates.
func scopedResourceObligations(state *scopedState) map[topology.AuthorityResource][]topology.AuthorityKey {
	catalog := newAuthorityCatalog()
	for i := range state.AuthorityRecords {
		r := &state.AuthorityRecords[i].authorityRecord
		catalog.records[r.Grant.Key] = r
	}
	sets := map[topology.AuthorityResource]map[topology.AuthorityKey]struct{}{}
	add := func(resource topology.AuthorityResource, key topology.AuthorityKey) {
		if sets[resource] == nil {
			sets[resource] = map[topology.AuthorityKey]struct{}{}
		}
		sets[resource][key] = struct{}{}
	}
	for _, floor := range state.AuthorityFloors {
		r := catalog.records[floor.Key]
		if r != nil && !resourceConsumed(r, floor.Resource) && !catalog.fenceSettled(r) {
			add(floor.Resource, floor.Key)
		}
	}
	for _, revocation := range state.Revocations {
		for _, ob := range revocation.Obligations {
			ob = catalog.obligationWithEvidence(ob)
			if ob.Evidence == nil && !catalog.fenceConsumed(ob.Key, ob.Resource) {
				add(ob.Resource, ob.Key)
			}
		}
	}
	return sortedScopedObligations(sets)
}

func sortedScopedObligations(
	sets map[topology.AuthorityResource]map[topology.AuthorityKey]struct{},
) map[topology.AuthorityResource][]topology.AuthorityKey {
	indexed := make(map[topology.AuthorityResource][]topology.AuthorityKey, len(sets))
	for resource, set := range sets {
		keys := make([]topology.AuthorityKey, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, compareAuthorityKey)
		indexed[resource] = keys
	}
	return indexed
}
