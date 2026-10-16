package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/topology"
)

// newAuthorityGrantCost matches the open, non-fence record cost in pendingCost.
const newAuthorityGrantCost = 3

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
type revocationRecord struct {
	Requested   bool                       `json:"requested"`
	OperatorID  topology.OperatorID        `json:"operator_id"`
	Obligations []topology.FenceObligation `json:"obligations"`
}
type authorityCatalog struct {
	deletedGroups map[string]bool // Scoped completed tombstones only; never serialized.
	records       map[topology.AuthorityKey]*authorityRecord
	floors        map[topology.AuthorityResource]topology.AuthorityKey
	actions       map[topology.AuthorizedStep]topology.AuthorityKey
	revocations   map[topology.OperatorID]*revocationRecord
}

func newAuthorityCatalog() authorityCatalog {
	return authorityCatalog{
		records:     make(map[topology.AuthorityKey]*authorityRecord),
		floors:      make(map[topology.AuthorityResource]topology.AuthorityKey),
		actions:     make(map[topology.AuthorizedStep]topology.AuthorityKey),
		revocations: make(map[topology.OperatorID]*revocationRecord),
	}
}
func compareAuthorityKey(a, b topology.AuthorityKey) int {
	return cmp.Or(cmp.Compare(a.ClusterID, b.ClusterID), cmp.Compare(a.GroupID, b.GroupID),
		cmp.Compare(a.Revision, b.Revision))
}

type authoritySnapshot struct {
	records     []authorityRecord
	floors      []resourceFloor
	revocations []revocationRecord
}

func (a authorityCatalog) snapshot() authoritySnapshot {
	return authoritySnapshot{records: a.snapshotRecords(), floors: a.snapshotFloors(),
		revocations: a.snapshotRevocations()}
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

func (a authorityCatalog) snapshotRevocations() []revocationRecord {
	revoked := collection.Slice[revocationRecord](len(a.revocations))
	for _, r := range a.revocations {
		revoked = append(revoked, *r)
	}
	slices.SortFunc(revoked, func(a, b revocationRecord) int { return cmp.Compare(a.OperatorID, b.OperatorID) })
	return revoked
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
func revokedComplete(r *revocationRecord) bool {
	for _, ob := range r.Obligations {
		if ob.Evidence == nil {
			return false
		}
	}
	return true
}

// Every open unfinished effect reserves its own slot plus a future fence and
// revocation record. Supersession can consume these reservations at capacity.
func (a authorityCatalog) pendingCost() int {
	cost := 0
	for _, r := range a.records {
		if a.recordSettled(r) {
			continue
		}
		cost++
		if !r.Closed && r.Grant.Purpose != topology.AuthorityFence {
			cost += 2
		}
	}
	for _, r := range a.revocations {
		if !revokedComplete(r) {
			cost++
		}
	}
	return cost
}

// AuthorityGrant returns an owned retained grant for an exact authority key.
func (m *Machine) AuthorityGrant(key topology.AuthorityKey) (topology.AuthorityGrant, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.authority.records[key]
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
	r, ok := m.authority.records[grant.Key]
	if !ok {
		return fault.ErrOperatorAuthorization
	}
	if !r.Grant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	return nil
}
func restoreAuthority(st smState) (authorityCatalog, error) {
	restore := authorityRestore{state: st, catalog: newAuthorityCatalog(),
		operators: make(map[topology.OperatorID]topology.Operator, len(st.Operators)),
		requests:  map[string]bool{}, readmissionRevisions: map[string]uint64{},
		readmissionMembers: map[string]uint64{}, readmissionMeta: map[string]bool{}}
	for _, op := range st.Operators {
		restore.operators[op.Status.ID] = op
	}
	for _, record := range st.AuthorityRecords {
		if err := restore.restoreRecord(record); err != nil {
			return authorityCatalog{}, err
		}
	}
	if err := restore.validateRetainedOwners(); err != nil {
		return authorityCatalog{}, err
	}
	if err := restore.restoreFloors(); err != nil {
		return authorityCatalog{}, err
	}
	if err := restore.catalog.validateFencePredecessors(); err != nil {
		return authorityCatalog{}, err
	}
	for _, record := range st.Revocations {
		if err := restore.catalog.restoreRevocation(record); err != nil {
			return authorityCatalog{}, err
		}
	}
	if err := restore.validateOperatorCompletion(); err != nil {
		return authorityCatalog{}, err
	}
	if !validAuthorityBoundaries(restore.catalog) ||
		restore.catalog.pendingCost() > st.Rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return authorityCatalog{}, invalidAuthorityCatalog()
	}
	return restore.catalog, nil
}

type authorityRestore struct {
	state                                    smState
	catalog                                  authorityCatalog
	operators                                map[topology.OperatorID]topology.Operator
	requests                                 map[string]bool
	readmissionRevisions, readmissionMembers map[string]uint64
	readmissionMeta                          map[string]bool
	cleanupActions                           map[topology.AuthorizedStep]bool
}

func invalidAuthorityCatalog() error {
	return fault.New(fault.Unreadable, "invalid authority catalog", nil)
}

func (restore *authorityRestore) restoreRecord(r authorityRecord) error {
	a, st, requests := restore.catalog, restore.state, restore.requests
	if r.Grant.Key.ClusterID != st.Latest.ClusterID || r.Grant.Key.Revision > st.AppliedIndex ||
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
	a, operators := restore.catalog, restore.operators
	action := *grant.Action
	if op, exists := operators[action.OperatorID]; exists {
		if action.StepSeq > len(op.Plan.Steps) ||
			!action.Matches(op.Status.ID, op.Status.Attempt, op.Status.Version,
				op.Plan.Steps[action.StepSeq-1]) {
			return invalidAuthorityCatalog()
		}
	}
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
	st := restore.state
	var admission *AdmissionRecord
	for _, candidate := range st.Admissions {
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

func (restore *authorityRestore) validateRetainedOwners() error {
	a, operators := restore.catalog, restore.operators
	readmissionMembers, readmissionMeta := restore.readmissionMembers, restore.readmissionMeta
	for id := range readmissionMembers {
		if !readmissionMeta[id] {
			return invalidAuthorityCatalog()
		}
	}
	for _, r := range a.records {
		if r.Grant.Purpose == topology.AuthorityEffect {
			if _, exists := operators[r.Grant.OperatorID]; !exists && !a.recordSettled(r) {
				return invalidAuthorityCatalog()
			}
		}
	}
	return nil
}

func (restore *authorityRestore) restoreFloors() error {
	for _, floor := range restore.state.AuthorityFloors {
		if restore.catalog.records[floor.Key] == nil {
			return invalidAuthorityCatalog()
		}
	}
	return restore.restoreResourceFloors()
}

func (restore *authorityRestore) restoreResourceFloors() error {
	a, st := restore.catalog, restore.state
	for _, f := range st.AuthorityFloors {
		r := a.records[f.Key]
		if _, duplicate := a.floors[f.Resource]; duplicate || !validResourceFloor(f, st) ||
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

func (a authorityCatalog) restoreRevocation(r revocationRecord) error {
	if r.OperatorID == "" || a.revocations[r.OperatorID] != nil {
		return invalidAuthorityCatalog()
	}
	if err := a.validateRestoredObligations(r.Obligations); err != nil {
		return err
	}
	a.revocations[r.OperatorID] = &r
	return nil
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

func (restore *authorityRestore) validateOperatorCompletion() error {
	a, st := restore.catalog, restore.state
	for _, view := range st.Operators {
		ex := operatorFromView(view)
		consumed := ex.CurrentStep - 1
		if ex.State == topology.OperatorSucceeded {
			consumed = len(ex.Steps)
		}
		for i := range consumed {
			if ex.Steps[i].Kind != topology.StepWaitCaughtUp && !a.stepConsumed(&ex, ex.Steps[i], retainedCompletion) {
				return invalidAuthorityCatalog()
			}
		}
	}
	return nil
}

func (a authorityCatalog) hasPendingRevocations() bool {
	for _, r := range a.revocations {
		if !revokedComplete(r) {
			return true
		}
	}
	return false
}
