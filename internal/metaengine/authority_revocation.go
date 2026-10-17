package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func (a authorityCatalog) clone() authorityCatalog {
	catalogCopy := newAuthorityCatalog()
	catalogCopy.deletedGroups = a.deletedGroups // Immutable derived state for this preparation.
	for key, r := range a.records {
		v := *r
		v.Grant = r.Grant.Clone()
		v.Fences = slices.Clone(r.Fences)
		v.Effects = slices.Clone(r.Effects)
		for i, e := range r.Effects {
			v.Effects[i] = e.Clone()
		}
		catalogCopy.records[key] = &v
	}
	for r, k := range a.floors {
		catalogCopy.floors[r] = k
	}
	for action, k := range a.actions {
		catalogCopy.actions[action] = k
	}
	return catalogCopy
}

func appendObligation(existing []topology.FenceObligation, ob topology.FenceObligation) []topology.FenceObligation {
	list := existing
	if !slices.ContainsFunc(list, func(old topology.FenceObligation) bool {
		return old.Key == ob.Key && old.Resource == ob.Resource
	}) {
		list = append(list, ob)
	}
	slices.SortFunc(list, compareFenceObligations)
	return list
}

// A fence is unnecessary once every older one-shot grant for this resource is
// durably consumed. This covers destroyed groups without inventing fence proof.
func (a authorityCatalog) fenceConsumed(key topology.AuthorityKey, resource topology.AuthorityResource) bool {
	if a.deletedResource(resource) {
		return true
	}
	for _, r := range a.records {
		if r.Grant.Key.GroupID == key.GroupID && r.Grant.Key.Revision < key.Revision &&
			r.Grant.Purpose != topology.AuthorityFence && slices.Contains(r.Grant.Resources, resource) &&
			!a.resourceSettled(r, resource) {
			return false
		}
	}
	return true
}

func (a authorityCatalog) obligationWithEvidence(ob topology.FenceObligation) topology.FenceObligation {
	for _, grant := range a.records {
		if grant.Grant.Key.GroupID != ob.Key.GroupID || grant.Grant.Key.Revision < ob.Key.Revision {
			continue
		}
		for _, e := range grant.Fences {
			if e.Resource == ob.Resource && (ob.Evidence == nil ||
				e.Key.Revision > ob.Evidence.Key.Revision) {
				proof := e
				ob.Evidence = &proof
			}
		}
	}
	return ob
}

func (a authorityCatalog) fenceSettled(r *authorityRecord) bool {
	if r.Grant.Purpose != topology.AuthorityFence {
		return false
	}
	for _, resource := range r.Grant.Resources {
		if !resourceConsumed(r, resource) && !a.fenceConsumed(r.Grant.Key, resource) {
			return false
		}
	}
	return true
}

// A transfer receipt consumes the command but leaves resumable initiation
// intent.
// Only a newer durable resource floor proves that intent can no longer resume.
func (a authorityCatalog) resourceSettled(r *authorityRecord, resource topology.AuthorityResource) bool {
	if a.deletedResource(resource) {
		return true
	}
	transfer := r.Grant.Action != nil && r.Grant.Action.Kind == topology.StepTransferLeader
	if !transfer && resourceConsumed(r, resource) {
		return true
	}
	for _, newer := range a.records {
		if newer.Grant.Key.GroupID != r.Grant.Key.GroupID || newer.Grant.Key.Revision <= r.Grant.Key.Revision {
			continue
		}
		for _, proof := range newer.Fences {
			if proof.Resource == resource {
				return true
			}
		}
	}
	return false
}
func (a authorityCatalog) recordSettled(r *authorityRecord) bool {
	if a.fenceSettled(r) {
		return true
	}
	for _, resource := range r.Grant.Resources {
		if !a.resourceSettled(r, resource) {
			return false
		}
	}
	return true
}

func compareFenceObligations(a, b topology.FenceObligation) int {
	if order := a.Resource.Compare(b.Resource); order != 0 {
		return order
	}
	return compareAuthorityKey(a.Key, b.Key)
}
