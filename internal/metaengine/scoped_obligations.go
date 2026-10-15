package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// Execution and cleanup share resource floors and therefore one prerequisite
// index. Closed/consumed grants do not block; missing revocation evidence does.
func scopedResourceObligations(state *scopedState) map[topology.AuthorityResource][]topology.AuthorityKey {
	sets := map[topology.AuthorityResource]map[topology.AuthorityKey]struct{}{}
	add := func(resource topology.AuthorityResource, key topology.AuthorityKey) {
		if sets[resource] == nil {
			sets[resource] = map[topology.AuthorityKey]struct{}{}
		}
		sets[resource][key] = struct{}{}
	}
	for i := range state.AuthorityRecords {
		addUnconsumedResources(&state.AuthorityRecords[i], add)
	}
	for _, revocation := range state.Revocations {
		for _, obligation := range revocation.Obligations {
			if obligation.Evidence == nil {
				add(obligation.Resource, obligation.Key)
			}
		}
	}
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

func addUnconsumedResources(record *scopedAuthorityRecord,
	add func(topology.AuthorityResource, topology.AuthorityKey)) {
	if record.Closed {
		return
	}
	for _, resource := range record.Grant.Resources {
		if !resourceConsumed(&record.authorityRecord, resource) {
			add(resource, record.Grant.Key)
		}
	}
}
