package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (a authorityCatalog) clone() authorityCatalog {
	copy := newAuthorityCatalog()
	for key, r := range a.records {
		v := *r
		v.Grant = r.Grant.Clone()
		v.Fences = slices.Clone(r.Fences)
		v.Effects = make([]topology.EffectReceipt, len(r.Effects))
		for i, e := range r.Effects {
			v.Effects[i] = e.Clone()
		}
		copy.records[key] = &v
	}
	for r, k := range a.floors {
		copy.floors[r] = k
	}
	for action, k := range a.actions {
		copy.actions[action] = k
	}
	for id, r := range a.revocations {
		owned := topology.AuthorityStatus{Obligations: r.Obligations}.Clone()
		copy.revocations[id] = &revocationRecord{OperatorID: id, Requested: r.Requested, Obligations: owned.Obligations}
	}
	return copy
}
func appendObligation(list []topology.FenceObligation, ob topology.FenceObligation) []topology.FenceObligation {
	if !slices.ContainsFunc(list, func(old topology.FenceObligation) bool { return old.Key == ob.Key && old.Resource == ob.Resource }) {
		list = append(list, ob)
	}
	slices.SortFunc(list, func(a, b topology.FenceObligation) int {
		if c := a.Resource.Compare(b.Resource); c != 0 {
			return c
		}
		return compareAuthorityKey(a.Key, b.Key)
	})
	return list
}

// revokeAuthorityLocked builds the entire transition before installing it. A
// rejected approval cannot partially close old grants or advance any floor.
func (sm *topologySM) revokeAuthorityLocked(index uint64, ids []topology.OperatorID, successor topology.OperatorID) error {
	next := sm.authority.clone()
	requested := map[topology.OperatorID]bool{}
	for _, id := range ids {
		requested[id] = true
	}
	var inherited []topology.FenceObligation
	for _, r := range next.revocations {
		for _, ob := range r.Obligations {
			if ob.Evidence == nil {
				inherited = appendObligation(inherited, ob)
			}
		}
	}
	type scope struct {
		resources map[topology.AuthorityResource]bool
		targets   map[topology.AuthorityTarget]bool
		owners    map[topology.OperatorID]bool
	}
	scopes := map[string]*scope{}
	for _, r := range next.records {
		g := r.Grant
		if r.Closed || g.Purpose == topology.AuthorityFence || successor == "" && !requested[g.OperatorID] {
			continue
		}
		r.Closed = true
		if g.OperatorID != "" {
			requested[g.OperatorID] = true
		}
		for _, resource := range g.Resources {
			if next.resourceSettled(r, resource) {
				continue
			}
			s := scopes[g.Key.GroupID]
			if s == nil {
				s = &scope{map[topology.AuthorityResource]bool{}, map[topology.AuthorityTarget]bool{}, map[topology.OperatorID]bool{}}
				scopes[g.Key.GroupID] = s
			}
			s.resources[resource] = true
			for _, t := range g.Targets {
				s.targets[t] = true
			}
			owner := g.OperatorID
			if owner == "" && g.Purpose == topology.AuthorityReadmit {
				for _, admission := range sm.admissions {
					if ReadmissionID(*admission) == g.AdmissionID {
						owner = admission.OperatorID
						requested[owner] = true
						break
					}
				}
			}
			if owner == "" {
				owner = successor
			}
			if owner == "" {
				return fault.ErrInvalid
			}
			s.owners[owner] = true
		}
	}
	ensure := func(id topology.OperatorID) *revocationRecord {
		r := next.revocations[id]
		if r == nil {
			r = &revocationRecord{OperatorID: id}
			next.revocations[id] = r
		}
		return r
	}
	for id := range requested {
		r := ensure(id)
		r.Requested = true
		for _, ob := range inherited {
			r.Obligations = appendObligation(r.Obligations, ob)
		}
	}
	for group, s := range scopes {
		g := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: group, Revision: index}, Purpose: topology.AuthorityFence}
		for r := range s.resources {
			g.Resources = append(g.Resources, r)
		}
		for t := range s.targets {
			g.Targets = append(g.Targets, t)
		}
		slices.SortFunc(g.Resources, topology.AuthorityResource.Compare)
		slices.SortFunc(g.Targets, topology.AuthorityTarget.Compare)
		if err := g.Validate(); err != nil {
			return err
		}
		if next.records[g.Key] != nil {
			return fault.ErrIdentityMismatch
		}
		next.records[g.Key] = &authorityRecord{Grant: g}
		for _, resource := range g.Resources {
			next.floors[resource] = g.Key
			ob := topology.FenceObligation{Key: g.Key, Resource: resource}
			for owner := range s.owners {
				r := ensure(owner)
				r.Obligations = appendObligation(r.Obligations, ob)
			}
		}
	}
	if next.pendingCost() > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return fault.ErrOverloaded
	}
	sm.authority = next
	return nil
}

// A fence is unnecessary once every older one-shot grant for this resource is
// durably consumed. This covers destroyed groups without inventing fence proof.
func (a authorityCatalog) fenceConsumed(key topology.AuthorityKey, resource topology.AuthorityResource) bool {
	for _, r := range a.records {
		if r.Grant.Key.GroupID == key.GroupID && r.Grant.Key.Revision < key.Revision && r.Grant.Purpose != topology.AuthorityFence &&
			slices.Contains(r.Grant.Resources, resource) && !a.resourceSettled(r, resource) {
			return false
		}
	}
	return true
}
func (a authorityCatalog) refreshRevocations() {
	for _, r := range a.revocations {
		var obligations []topology.FenceObligation
		for _, ob := range r.Obligations {
			if ob.Evidence == nil {
				for _, grant := range a.records {
					if grant.Grant.Key.GroupID != ob.Key.GroupID || grant.Grant.Key.Revision < ob.Key.Revision {
						continue
					}
					for _, e := range grant.Fences {
						if e.Resource == ob.Resource && (ob.Evidence == nil || e.Key.Revision > ob.Evidence.Key.Revision) {
							proof := e
							ob.Evidence = &proof
						}
					}
				}
			}
			if ob.Evidence == nil && a.fenceConsumed(ob.Key, ob.Resource) {
				continue
			}
			obligations = append(obligations, ob)
		}
		r.Obligations = obligations
	}
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

// A transfer receipt consumes the command but leaves resumable initiation intent.
// Only a newer durable resource floor proves that intent can no longer resume.
func (a authorityCatalog) resourceSettled(r *authorityRecord, resource topology.AuthorityResource) bool {
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
