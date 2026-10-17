package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/topology"
)

type authorityRecord struct {
	Grant   topology.AuthorityGrant  `json:"grant"`
	Fences  []topology.FenceEvidence `json:"fences"`
	Effects []topology.EffectReceipt `json:"effects"`
	Closed  bool                     `json:"closed"`
}
type resourceFloor struct {
	Resource topology.AuthorityResource `json:"resource"`
	Key      topology.AuthorityKey      `json:"key"`
}
type authorityCatalog struct {
	deletedGroups map[string]bool // Scoped completed tombstones only; never serialized.
	records       map[topology.AuthorityKey]*authorityRecord
	floors        map[topology.AuthorityResource]topology.AuthorityKey
	actions       map[topology.AuthorizedStep]topology.AuthorityKey
}

func newAuthorityCatalog() authorityCatalog {
	return authorityCatalog{
		records: make(map[topology.AuthorityKey]*authorityRecord),
		floors:  make(map[topology.AuthorityResource]topology.AuthorityKey),
		actions: make(map[topology.AuthorizedStep]topology.AuthorityKey),
	}
}
func compareAuthorityKey(a, b topology.AuthorityKey) int {
	return cmp.Or(cmp.Compare(a.ClusterID, b.ClusterID), cmp.Compare(a.GroupID, b.GroupID),
		cmp.Compare(a.Revision, b.Revision))
}

func (a authorityCatalog) snapshotRecords() []authorityRecord {
	records := collection.Slice[authorityRecord](len(a.records))
	for _, record := range a.records {
		records = append(records, *record)
	}
	slices.SortFunc(records, func(a, b authorityRecord) int { return compareAuthorityKey(a.Grant.Key, b.Grant.Key) })
	return records
}

func (a authorityCatalog) snapshotFloors() []resourceFloor {
	floors := collection.Slice[resourceFloor](len(a.floors))
	for resource, key := range a.floors {
		floors = append(floors, resourceFloor{resource, key})
	}
	slices.SortFunc(floors, func(a, b resourceFloor) int { return a.Resource.Compare(b.Resource) })
	return floors
}

func resourceConsumed(record *authorityRecord, resource topology.AuthorityResource) bool {
	if record.Grant.Purpose == topology.AuthorityFence {
		return slices.ContainsFunc(record.Fences, func(e topology.FenceEvidence) bool { return e.Resource == resource })
	}
	if resource.Kind == topology.AuthorityGroup {
		return slices.ContainsFunc(record.Effects, func(e topology.EffectReceipt) bool {
			return e.Resource == resource
		})
	}
	for _, target := range record.Grant.Targets {
		if target.NodeName == resource.NodeName &&
			!slices.ContainsFunc(record.Effects,
				func(e topology.EffectReceipt) bool { return e.Resource == resource && e.Target == target }) {
			return false
		}
	}
	return true
}
func revokedComplete(obligations []topology.FenceObligation) bool {
	for _, ob := range obligations {
		if ob.Evidence == nil {
			return false
		}
	}
	return true
}

// AuthorityGrant returns an owned retained grant for an exact authority key.
func (m *Machine) AuthorityGrant(key topology.AuthorityKey) (topology.AuthorityGrant, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.scoped.indexes.grants[key]
	if !ok {
		return topology.AuthorityGrant{}, false
	}
	return r.Grant.Clone(), true
}

// ValidateGrant checks committed immutable identity, not current approval. A
// closed grant remains verifiable: resource fencing decides its execution
// limit.
func (m *Machine) ValidateGrant(ctx context.Context, grant topology.AuthorityGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.scoped.indexes.grants[grant.Key]
	if !ok {
		return fault.ErrOperationAuthorization
	}
	if !r.Grant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

type authorityRestore struct {
	cluster                                  string
	appliedIndex                             uint64
	admissions                               []AdmissionRecord
	floors                                   []resourceFloor
	catalog                                  authorityCatalog
	requests                                 map[string]bool
	readmissionRevisions, readmissionMembers map[string]uint64
	readmissionMeta                          map[string]bool
	cleanupActions                           map[topology.AuthorizedStep]bool
}

func invalidAuthorityCatalog() error {
	return fault.New(fault.Unreadable, "invalid authority catalog", nil)
}

func (restore *authorityRestore) restoreRecord(r authorityRecord) error {
	a, requests := restore.catalog, restore.requests
	if r.Grant.Key.ClusterID != restore.cluster || r.Grant.Key.Revision > restore.appliedIndex ||
		a.records[r.Grant.Key] != nil || r.Grant.Validate() != nil {
		return invalidAuthorityCatalog()
	}
	if r.Grant.Purpose == topology.AuthorityReadmit {
		if err := restore.restoreReadmissionGrant(r.Grant); err != nil {
			return err
		}
	}
	if r.Grant.Purpose == topology.AuthorityTransfer {
		if requests[r.Grant.RequestID] {
			return invalidAuthorityCatalog()
		}
		requests[r.Grant.RequestID] = true
	}
	if r.Grant.Purpose == topology.AuthorityEffect {
		if err := restore.restoreEffectAction(r.Grant); err != nil {
			return err
		}
	}
	if err := validateRestoredReceipts(r); err != nil {
		return err
	}
	a.records[r.Grant.Key] = &r
	return nil
}

func (restore *authorityRestore) restoreEffectAction(grant topology.AuthorityGrant) error {
	a := restore.catalog
	action := *grant.Action
	if _, duplicate := a.actions[action]; duplicate && !restore.cleanupActions[action] {
		return invalidAuthorityCatalog()
	}
	a.rememberAction(grant)
	return nil
}

func (a authorityCatalog) rememberAction(grant topology.AuthorityGrant) {
	if grant.Action != nil && compareAuthorityKey(grant.Key, a.actions[*grant.Action]) > 0 {
		a.actions[*grant.Action] = grant.Key
	}
}

func (restore *authorityRestore) readmissionAdmission(id string) (*AdmissionRecord, error) {
	var admission *AdmissionRecord
	for _, candidate := range restore.admissions {
		if candidate.ReadmissionInstanceID == "" && ReadmissionID(candidate) == id {
			if admission != nil {
				return nil, invalidAuthorityCatalog()
			}
			admissionCopy := candidate
			admission = &admissionCopy
		}
	}
	return admission, nil
}

func (restore *authorityRestore) restoreReadmissionGrant(grant topology.AuthorityGrant) error {
	readmissionMembers, readmissionRevisions, readmissionMeta :=
		restore.readmissionMembers, restore.readmissionRevisions, restore.readmissionMeta
	admission, err := restore.readmissionAdmission(grant.AdmissionID)
	if err != nil {
		return err
	}
	if admission == nil || len(grant.Resources) != 1 || grant.Resources[0].NodeName != admission.NodeName {
		return invalidAuthorityCatalog()
	}
	for _, target := range grant.Targets {
		if target.NodeName != admission.NodeName || target.MemberID >= admission.MemberID {
			return invalidAuthorityCatalog()
		}
		if previous, ok := readmissionMembers[grant.AdmissionID]; ok && previous != target.MemberID {
			return invalidAuthorityCatalog()
		}
		readmissionMembers[grant.AdmissionID] = target.MemberID
	}
	if grant.Key.GroupID == topology.MetaGroupID {
		if len(grant.Targets) != 1 || grant.Targets[0].ReplicaID != grant.Targets[0].MemberID {
			return invalidAuthorityCatalog()
		}
		readmissionMeta[grant.AdmissionID] = true
	}
	if revision, ok := readmissionRevisions[grant.AdmissionID]; ok && revision != grant.Key.Revision {
		return invalidAuthorityCatalog()
	}
	readmissionRevisions[grant.AdmissionID] = grant.Key.Revision
	return nil
}

func validateRestoredReceipts(r authorityRecord) error {
	seenFences := map[topology.AuthorityResource]bool{}
	for _, e := range r.Fences {
		if seenFences[e.Resource] || e.Validate(r.Grant) != nil {
			return invalidAuthorityCatalog()
		}
		seenFences[e.Resource] = true
	}
	seenEffects := map[struct {
		resource topology.AuthorityResource
		target   topology.AuthorityTarget
	}]bool{}
	for _, e := range r.Effects {
		key := struct {
			resource topology.AuthorityResource
			target   topology.AuthorityTarget
		}{e.Resource, e.Target}
		if seenEffects[key] || e.Validate(r.Grant) != nil {
			return invalidAuthorityCatalog()
		}
		seenEffects[key] = true
	}
	return nil
}

func (restore *authorityRestore) restoreResourceFloors() error {
	a := restore.catalog
	for _, f := range restore.floors {
		r := a.records[f.Key]
		if _, duplicate := a.floors[f.Resource]; duplicate ||
			!validResourceFloor(f, restore.cluster, restore.appliedIndex) ||
			r != nil && !slices.Contains(r.Grant.Resources, f.Resource) {
			return invalidAuthorityCatalog()
		}
		a.floors[f.Resource] = f.Key
	}
	for _, r := range a.records {
		for _, resource := range r.Grant.Resources {
			if floor, ok := a.floors[resource]; !ok || floor.Revision < r.Grant.Key.Revision {
				return invalidAuthorityCatalog()
			}
		}
	}
	return nil
}

func (a authorityCatalog) validateFencePredecessors() error {
	for _, record := range a.records {
		if record.Grant.Purpose != topology.AuthorityFence {
			continue
		}
		for _, resource := range record.Grant.Resources {
			if a.deletedResource(resource) || resourceConsumed(record, resource) {
				continue
			}
			if !a.hasFencePredecessor(record, resource) {
				return invalidAuthorityCatalog()
			}
		}
	}
	return nil
}

func (a authorityCatalog) hasFencePredecessor(record *authorityRecord, resource topology.AuthorityResource) bool {
	predecessor := false
	for _, older := range a.records {
		if older.Grant.Key.GroupID == record.Grant.Key.GroupID &&
			older.Grant.Key.Revision < record.Grant.Key.Revision &&
			older.Grant.Purpose != topology.AuthorityFence && slices.Contains(older.Grant.Resources, resource) {
			predecessor = true
		}
	}
	return predecessor
}

func (a authorityCatalog) validateRestoredObligations(obligations []topology.FenceObligation) error {
	seen := map[resourceFloor]bool{}
	for _, ob := range obligations {
		key := resourceFloor{ob.Resource, ob.Key}
		g := a.records[ob.Key]
		if g == nil || g.Grant.Purpose != topology.AuthorityFence || seen[key] ||
			!slices.Contains(g.Grant.Resources, ob.Resource) {
			return invalidAuthorityCatalog()
		}
		seen[key] = true
		if ob.Evidence == nil && !a.deletedResource(ob.Resource) && a.fenceConsumed(ob.Key, ob.Resource) {
			return invalidAuthorityCatalog()
		}
		if ob.Evidence != nil {
			e := ob.Evidence
			proof := a.records[e.Key]
			if proof == nil || e.Resource != ob.Resource || e.Key.Revision < ob.Key.Revision ||
				e.Validate(proof.Grant) != nil ||
				!slices.Contains(proof.Fences, *e) {
				return invalidAuthorityCatalog()
			}
		}
	}
	return nil
}
