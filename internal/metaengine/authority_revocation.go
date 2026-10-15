package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (a authorityCatalog) clone() authorityCatalog {
	catalogCopy := newAuthorityCatalog()
	for key, r := range a.records {
		v := *r
		v.Grant = r.Grant.Clone()
		v.Fences = slices.Clone(r.Fences)
		v.Effects = make([]topology.EffectReceipt, len(r.Effects))
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
	for id, r := range a.revocations {
		owned := topology.AuthorityStatus{Obligations: r.Obligations}.Clone()
		catalogCopy.revocations[id] = &revocationRecord{OperatorID: id, Requested: r.Requested,
			Obligations: owned.Obligations}
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

// revokeAuthorityLocked builds the entire transition before installing it. A
// rejected approval cannot partially close old grants or advance any floor.
func (sm *topologySM) revokeAuthorityLocked(index uint64, ids []topology.OperatorID,
	successor topology.OperatorID) error {
	transition := authorityRevocation{sm: sm, catalog: sm.authority.clone(), successor: successor, index: index,
		requested: map[topology.OperatorID]bool{}, scopes: map[string]*authorityRevocationScope{}}
	for _, id := range ids {
		transition.requested[id] = true
	}
	transition.inherited = transition.catalog.inheritedObligations()
	if err := transition.closeRecords(); err != nil {
		return err
	}
	transition.markRequested()
	for group, scope := range transition.scopes {
		if err := transition.installFence(group, scope); err != nil {
			return err
		}
	}
	if transition.catalog.pendingCost() > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return fault.ErrOverloaded
	}
	sm.authority = transition.catalog
	return nil
}

type authorityRevocationScope struct {
	resources map[topology.AuthorityResource]bool
	targets   map[topology.AuthorityTarget]bool
	owners    map[topology.OperatorID]bool
}

type authorityRevocation struct {
	sm        *topologySM
	catalog   authorityCatalog
	successor topology.OperatorID
	index     uint64
	requested map[topology.OperatorID]bool
	scopes    map[string]*authorityRevocationScope
	inherited []topology.FenceObligation
}

func (a authorityCatalog) inheritedObligations() []topology.FenceObligation {
	var inherited []topology.FenceObligation
	for _, r := range a.revocations {
		for _, ob := range r.Obligations {
			if ob.Evidence == nil {
				inherited = appendObligation(inherited, ob)
			}
		}
	}
	return inherited
}

func (transition *authorityRevocation) closeRecords() error {
	for _, r := range transition.catalog.records {
		g := r.Grant
		if r.Closed || g.Purpose == topology.AuthorityFence ||
			transition.successor == "" && !transition.requested[g.OperatorID] {
			continue
		}
		r.Closed = true
		if g.OperatorID != "" {
			transition.requested[g.OperatorID] = true
		}
		if err := transition.addRecordObligations(r); err != nil {
			return err
		}
	}
	return nil
}

func (transition *authorityRevocation) addRecordObligations(r *authorityRecord) error {
	g := r.Grant
	for _, resource := range g.Resources {
		if transition.catalog.resourceSettled(r, resource) {
			continue
		}
		s := transition.scopes[g.Key.GroupID]
		if s == nil {
			s = &authorityRevocationScope{resources: map[topology.AuthorityResource]bool{},
				targets: map[topology.AuthorityTarget]bool{}, owners: map[topology.OperatorID]bool{}}
			transition.scopes[g.Key.GroupID] = s
		}
		s.resources[resource] = true
		for _, t := range g.Targets {
			s.targets[t] = true
		}
		owner, err := transition.resourceOwner(g)
		if err != nil {
			return err
		}
		s.owners[owner] = true
	}
	return nil
}

func (transition *authorityRevocation) resourceOwner(g topology.AuthorityGrant) (topology.OperatorID, error) {
	owner := g.OperatorID
	if owner == "" && g.Purpose == topology.AuthorityReadmit {
		for _, admission := range transition.sm.admissions {
			if ReadmissionID(*admission) == g.AdmissionID {
				owner = admission.OperatorID
				transition.requested[owner] = true
				break
			}
		}
	}
	if owner == "" {
		owner = transition.successor
	}
	if owner == "" {
		return "", fault.ErrInvalid
	}
	return owner, nil
}

func (a authorityCatalog) ensureRevocation(id topology.OperatorID) *revocationRecord {
	r := a.revocations[id]
	if r == nil {
		r = &revocationRecord{OperatorID: id}
		a.revocations[id] = r
	}
	return r
}

func (transition *authorityRevocation) markRequested() {
	for id := range transition.requested {
		r := transition.catalog.ensureRevocation(id)
		r.Requested = true
		for _, ob := range transition.inherited {
			r.Obligations = appendObligation(r.Obligations, ob)
		}
	}
}

func (transition *authorityRevocation) installFence(group string, scope *authorityRevocationScope) error {
	g := topology.AuthorityGrant{
		Key: topology.AuthorityKey{ClusterID: transition.sm.target.ClusterID,
			GroupID: group, Revision: transition.index},
		Purpose: topology.AuthorityFence,
	}
	for r := range scope.resources {
		g.Resources = append(g.Resources, r)
	}
	for t := range scope.targets {
		g.Targets = append(g.Targets, t)
	}
	slices.SortFunc(g.Resources, topology.AuthorityResource.Compare)
	slices.SortFunc(g.Targets, topology.AuthorityTarget.Compare)
	if err := g.Validate(); err != nil {
		return err
	}
	if transition.catalog.records[g.Key] != nil {
		return fault.ErrIdentityMismatch
	}
	transition.catalog.records[g.Key] = &authorityRecord{Grant: g}
	for _, resource := range g.Resources {
		transition.catalog.floors[resource] = g.Key
		ob := topology.FenceObligation{Key: g.Key, Resource: resource}
		for owner := range scope.owners {
			r := transition.catalog.ensureRevocation(owner)
			r.Obligations = appendObligation(r.Obligations, ob)
		}
	}
	return nil
}

// A fence is unnecessary once every older one-shot grant for this resource is
// durably consumed. This covers destroyed groups without inventing fence proof.
func (a authorityCatalog) fenceConsumed(key topology.AuthorityKey, resource topology.AuthorityResource) bool {
	for _, r := range a.records {
		if r.Grant.Key.GroupID == key.GroupID && r.Grant.Key.Revision < key.Revision &&
			r.Grant.Purpose != topology.AuthorityFence && slices.Contains(r.Grant.Resources, resource) &&
			!a.resourceSettled(r, resource) {
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
				ob = a.obligationWithEvidence(ob)
			}
			if ob.Evidence == nil && a.fenceConsumed(ob.Key, ob.Resource) {
				continue
			}
			obligations = append(obligations, ob)
		}
		r.Obligations = obligations
	}
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
