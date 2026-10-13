package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
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
type revocationRecord struct {
	Requested   bool                       `json:"requested"`
	OperatorID  topology.OperatorID        `json:"operator_id"`
	Obligations []topology.FenceObligation `json:"obligations"`
}
type authorityCatalog struct {
	records     map[topology.AuthorityKey]*authorityRecord
	floors      map[topology.AuthorityResource]topology.AuthorityKey
	actions     map[topology.AuthorizedStep]topology.AuthorityKey
	revocations map[topology.OperatorID]*revocationRecord
}

func newAuthorityCatalog() authorityCatalog {
	return authorityCatalog{records: make(map[topology.AuthorityKey]*authorityRecord), floors: make(map[topology.AuthorityResource]topology.AuthorityKey), actions: make(map[topology.AuthorizedStep]topology.AuthorityKey), revocations: make(map[topology.OperatorID]*revocationRecord)}
}
func compareAuthorityKey(a, b topology.AuthorityKey) int {
	return cmp.Or(cmp.Compare(a.ClusterID, b.ClusterID), cmp.Compare(a.GroupID, b.GroupID), cmp.Compare(a.Revision, b.Revision))
}
func (a authorityCatalog) snapshot() ([]authorityRecord, []resourceFloor, []revocationRecord) {
	var records []authorityRecord
	for _, record := range a.records {
		records = append(records, *record)
	}
	slices.SortFunc(records, func(a, b authorityRecord) int { return compareAuthorityKey(a.Grant.Key, b.Grant.Key) })
	var floors []resourceFloor
	for resource, key := range a.floors {
		floors = append(floors, resourceFloor{resource, key})
	}
	slices.SortFunc(floors, func(a, b resourceFloor) int { return a.Resource.Compare(b.Resource) })
	var revoked []revocationRecord
	for _, r := range a.revocations {
		revoked = append(revoked, *r)
	}
	slices.SortFunc(revoked, func(a, b revocationRecord) int { return cmp.Compare(a.OperatorID, b.OperatorID) })
	return records, floors, revoked
}
func resourceConsumed(record *authorityRecord, resource topology.AuthorityResource) bool {
	if record.Grant.Purpose == topology.AuthorityFence {
		return slices.ContainsFunc(record.Fences, func(e topology.FenceEvidence) bool { return e.Resource == resource })
	}
	if resource.Kind == topology.AuthorityGroup {
		return slices.ContainsFunc(record.Effects, func(e topology.EffectReceipt) bool { return e.Resource == resource })
	}
	for _, target := range record.Grant.Targets {
		if target.NodeName == resource.NodeName && !slices.ContainsFunc(record.Effects, func(e topology.EffectReceipt) bool { return e.Resource == resource && e.Target == target }) {
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
// closed grant remains verifiable: resource fencing decides its execution limit.
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
	a := newAuthorityCatalog()
	invalid := func() (authorityCatalog, error) {
		return authorityCatalog{}, fault.New(fault.Unreadable, "invalid authority catalog", nil)
	}
	operators := make(map[topology.OperatorID]topology.Operator, len(st.Operators))
	for _, op := range st.Operators {
		operators[op.Status.ID] = op
	}
	requests := map[string]bool{}
	readmissionRevisions := map[string]uint64{}
	readmissionMembers := map[string]uint64{}
	readmissionMeta := map[string]bool{}
	for _, r := range st.AuthorityRecords {
		if r.Grant.Validate() != nil || r.Grant.Key.ClusterID != st.Latest.ClusterID || r.Grant.Key.Revision > st.AppliedIndex || a.records[r.Grant.Key] != nil {
			return invalid()
		}
		if r.Grant.Purpose == topology.AuthorityReadmit {
			var admission *AdmissionRecord
			for _, candidate := range st.Admissions {
				if candidate.ReadmissionInstanceID == "" && ReadmissionID(candidate) == r.Grant.AdmissionID {
					if admission != nil {
						return invalid()
					}
					copy := candidate
					admission = &copy
				}
			}
			if admission == nil || len(r.Grant.Resources) != 1 || r.Grant.Resources[0].NodeName != admission.NodeName {
				return invalid()
			}
			for _, target := range r.Grant.Targets {
				if target.NodeName != admission.NodeName || target.MemberID >= admission.MemberID {
					return invalid()
				}
				if previous, ok := readmissionMembers[r.Grant.AdmissionID]; ok && previous != target.MemberID {
					return invalid()
				}
				readmissionMembers[r.Grant.AdmissionID] = target.MemberID
			}
			if r.Grant.Key.GroupID == topology.MetaGroupID {
				if len(r.Grant.Targets) != 1 || r.Grant.Targets[0].ReplicaID != r.Grant.Targets[0].MemberID {
					return invalid()
				}
				readmissionMeta[r.Grant.AdmissionID] = true
			}
			if revision, ok := readmissionRevisions[r.Grant.AdmissionID]; ok && revision != r.Grant.Key.Revision {
				return invalid()
			}
			readmissionRevisions[r.Grant.AdmissionID] = r.Grant.Key.Revision
		}
		if r.Grant.Purpose == topology.AuthorityTransfer {
			if requests[r.Grant.RequestID] {
				return invalid()
			}
			requests[r.Grant.RequestID] = true
		}
		if r.Grant.Purpose == topology.AuthorityEffect {
			action := *r.Grant.Action
			if op, exists := operators[action.OperatorID]; exists {
				if action.StepSeq > len(op.Plan.Steps) || !action.Matches(op.Status.ID, op.Status.Attempt, op.Status.Version, op.Plan.Steps[action.StepSeq-1]) {
					return invalid()
				}
			}
		}
		if r.Grant.Purpose == topology.AuthorityEffect {
			if _, duplicate := a.actions[*r.Grant.Action]; duplicate {
				return invalid()
			}
			a.actions[*r.Grant.Action] = r.Grant.Key
		}
		seenFences := map[topology.AuthorityResource]bool{}
		for _, e := range r.Fences {
			if e.Validate(r.Grant) != nil || seenFences[e.Resource] {
				return invalid()
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
			if e.Validate(r.Grant) != nil || seenEffects[key] {
				return invalid()
			}
			seenEffects[key] = true
		}
		a.records[r.Grant.Key] = &r
	}
	for id := range readmissionMembers {
		if !readmissionMeta[id] {
			return invalid()
		}
	}
	for _, r := range a.records {
		if r.Grant.Purpose == topology.AuthorityEffect {
			if _, exists := operators[r.Grant.OperatorID]; !exists && !a.recordSettled(r) {
				return invalid()
			}
		}
	}
	for _, f := range st.AuthorityFloors {
		r := a.records[f.Key]
		if _, duplicate := a.floors[f.Resource]; duplicate || r == nil || !slices.Contains(r.Grant.Resources, f.Resource) {
			return invalid()
		}
		a.floors[f.Resource] = f.Key
	}
	for _, r := range a.records {
		for _, resource := range r.Grant.Resources {
			if floor, ok := a.floors[resource]; !ok || floor.Revision < r.Grant.Key.Revision {
				return invalid()
			}
		}
	}
	for _, record := range a.records {
		if record.Grant.Purpose != topology.AuthorityFence {
			continue
		}
		for _, resource := range record.Grant.Resources {
			if resourceConsumed(record, resource) {
				continue
			}
			predecessor := false
			for _, older := range a.records {
				if older.Grant.Key.GroupID == record.Grant.Key.GroupID && older.Grant.Key.Revision < record.Grant.Key.Revision && older.Grant.Purpose != topology.AuthorityFence && slices.Contains(older.Grant.Resources, resource) {
					predecessor = true
				}
			}
			if !predecessor {
				return invalid()
			}
		}
	}
	for _, r := range st.Revocations {
		if r.OperatorID == "" || a.revocations[r.OperatorID] != nil {
			return invalid()
		}
		seen := map[resourceFloor]bool{}
		for _, ob := range r.Obligations {
			key := resourceFloor{ob.Resource, ob.Key}
			g := a.records[ob.Key]
			if g == nil || g.Grant.Purpose != topology.AuthorityFence || seen[key] || !slices.Contains(g.Grant.Resources, ob.Resource) {
				return invalid()
			}
			seen[key] = true
			if ob.Evidence == nil && a.fenceConsumed(ob.Key, ob.Resource) {
				return invalid()
			}
			if ob.Evidence != nil {
				e := ob.Evidence
				proof := a.records[e.Key]
				if proof == nil || e.Resource != ob.Resource || e.Key.Revision < ob.Key.Revision || e.Validate(proof.Grant) != nil || !slices.Contains(proof.Fences, *e) {
					return invalid()
				}
			}
		}
		a.revocations[r.OperatorID] = &r
	}
	for _, view := range st.Operators {
		ex := operatorFromView(view)
		consumed := ex.CurrentStep - 1
		if ex.State == topology.OperatorSucceeded {
			consumed = len(ex.Steps)
		}
		for i := 0; i < consumed; i++ {
			if ex.Steps[i].Kind != topology.StepWaitCaughtUp && !a.stepConsumed(&ex, ex.Steps[i], false) {
				return invalid()
			}
		}
	}
	if !validAuthorityBoundaries(a) {
		return invalid()
	}
	if a.pendingCost() > st.Rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return invalid()
	}
	return a, nil
}
