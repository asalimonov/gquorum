package metaengine

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type retiredGroup struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
}
type allocationRecord struct {
	GroupID       string `json:"group_id"`
	NextReplicaID uint64 `json:"next_replica_id"`
}

func canonicalizeDocument(d *topology.Document) {
	slices.SortFunc(d.Nodes, func(a, b topology.Node) int { return cmp.Compare(a.Name, b.Name) })
	slices.SortFunc(d.Groups, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	for i := range d.Groups {
		slices.SortFunc(d.Groups[i].Replicas, func(a, b topology.Replica) int {
			return cmp.Compare(a.ReplicaID, b.ReplicaID)
		})
	}
}
func (sm *topologySM) rememberAllocations(d topology.Document) {
	for _, g := range d.Groups {
		sm.allocations[g.ID] = max(sm.allocations[g.ID], g.NextReplicaID)
	}
}
func (sm *topologySM) allocationSnapshot() []allocationRecord {
	result := make([]allocationRecord, 0, len(sm.allocations))
	for id, next := range sm.allocations {
		result = append(result, allocationRecord{id, next})
	}
	slices.SortFunc(result, func(a, b allocationRecord) int { return cmp.Compare(a.GroupID, b.GroupID) })
	return result
}
func (sm *topologySM) retiredSnapshot() []retiredGroup {
	result := make([]retiredGroup, 0, len(sm.retired))
	for id, version := range sm.retired {
		result = append(result, retiredGroup{id, version})
	}
	slices.SortFunc(result, func(a, b retiredGroup) int { return cmp.Compare(a.ID, b.ID) })
	return result
}
func (sm *topologySM) retireRemovedGroups(desired topology.Document) {
	for _, previous := range []topology.Document{sm.active, sm.target} {
		for _, g := range previous.Groups {
			if g.ID != topology.MetaGroupID && sm.retired[g.ID] == 0 && desired.Group(g.ID) == nil {
				sm.retired[g.ID] = desired.Version
			}
		}
	}
}
func (sm *topologySM) validateDocument(d topology.Document) error {
	if d.Version == 0 {
		return fault.New(fault.Invalid, "document version overflow", nil)
	}
	if err := topology.ValidateTransition(sm.availableIdentities(), d, sm.rules, sm.retiredIDs()); err != nil {
		return err
	}
	if err := sm.validatePlacementLocked(d); err != nil {
		return err
	}
	for _, group := range d.Groups {
		if err := sm.validateGroupAllocation(group); err != nil {
			return err
		}
	}
	return nil
}

// Placement is checked against the FSM's approved Target. A proposal's own
// document cannot grant itself the right to retain a replica on a reserved
// node.
func (sm *topologySM) validatePlacementLocked(d topology.Document) error {
	approved, base := sm.target, sm.active
	// The founding proposal has no approved or active view yet. An empty
	// same-cluster reference grants no reservation retention exceptions.
	if approved.ClusterID == "" {
		approved.ClusterID = d.ClusterID
	}
	if base.ClusterID == "" {
		base.ClusterID = d.ClusterID
	}
	if err := topology.ValidateHostingCapacityFor(approved, d, sm.rules); err != nil {
		return err
	}
	return topology.ValidatePlacementTransition(approved, base, d)
}
func operatorFromView(v topology.Operator) operatorRecord {
	s := v.Status
	plan := v.Plan.Clone()
	return operatorRecord{
		ID:            s.ID,
		Version:       s.Version,
		Attempt:       s.Attempt,
		State:         s.State,
		Steps:         plan.Steps,
		VoterBases:    plan.VoterBases,
		CurrentStep:   s.CurrentStep,
		GroupID:       s.GroupID,
		NodeName:      s.NodeName,
		WaitingReason: s.WaitingReason,
		Error:         s.Error,
		Origin:        v.Origin,
		Actor:         v.Actor,
		StartedAt:     v.StartedAt,
		UpdatedAt:     v.UpdatedAt,
	}
}
func validateRestoredState(st smState, rules topology.Rules) error {
	if slices.ContainsFunc(st.Admissions, func(a AdmissionRecord) bool { return a.ReadmissionOperatorID != "" }) {
		return fault.New(fault.Unsupported, "admission handover requires scoped metadata", nil)
	}
	if err := validateRestoredDocuments(st, rules); err != nil {
		return err
	}
	if err := validateRestoredPlacement(st, rules); err != nil {
		return err
	}
	if (st.AppliedIndex == 0) != (st.AppliedTerm == 0) {
		return fault.New(fault.Unreadable, "invalid applied index/term", nil)
	}
	if err := validateVoterSwitches(st); err != nil {
		return err
	}
	if _, err := restoreAuthority(st); err != nil {
		return err
	}
	if err := validateRestoredHistory(st, rules); err != nil {
		return err
	}
	if err := validateRestoredRetirements(st); err != nil {
		return err
	}
	if err := validateRestoredAllocations(st); err != nil {
		return err
	}
	operators, err := validateRestoredOperators(st)
	if err != nil {
		return err
	}
	if err := validateRestoredSessions(st); err != nil {
		return err
	}
	return validateRestoredAdmissions(st, operators)
}

func validateRestoredDocuments(st smState, rules topology.Rules) error {
	if st.SchemaVersion != metaStateSchemaVersion {
		return fault.New(fault.Unsupported, "unsupported meta state schema", nil)
	}
	if rules.SchemaVersion != 1 || !reflect.DeepEqual(st.Rules, rules) || rules.Limits.Validate() != nil {
		return fault.New(fault.IdentityMismatch, "persisted cluster rules differ", nil)
	}
	for _, d := range []topology.Document{st.Latest, st.Target, st.Active} {
		if err := d.Validate(rules); err != nil {
			return fault.New(fault.Unreadable, "invalid materialized document", err)
		}
	}
	if st.Target.Version > st.Latest.Version || st.Active.Version > st.Target.Version {
		return fault.New(fault.Unreadable, "document views have invalid order", nil)
	}
	return nil
}

func validateRestoredPlacement(st smState, rules topology.Rules) error {
	// Recheck the current declarations before reacquiring native authority. The
	// approved Target, not persisted Latest, grants reservation retention.
	for _, desired := range []topology.Document{st.Target, st.Latest} {
		if desired.Version == 0 {
			continue
		}
		approved, base := st.Target, st.Active
		if approved.ClusterID == "" {
			approved.ClusterID = desired.ClusterID
		}
		if base.ClusterID == "" {
			base.ClusterID = desired.ClusterID
		}
		if err := topology.ValidateHostingCapacityFor(approved, desired, rules); err != nil {
			return fault.New(fault.Unreadable, "invalid restored placement capacity", err)
		}
		if err := topology.ValidatePlacementTransition(approved, base, desired); err != nil {
			return fault.New(fault.Unreadable, "invalid restored placement", err)
		}
	}
	return nil
}

func validateRestoredHistory(st smState, rules topology.Rules) error {
	if len(st.History) > historyRetention {
		return fault.New(fault.Unreadable, "history exceeds retention", nil)
	}
	var previous uint64
	for _, d := range st.History {
		if err := d.Validate(rules); err != nil {
			return err
		}
		if d.Version <= previous || d.Version > st.Latest.Version {
			return fault.New(fault.Unreadable, "invalid history ordering", nil)
		}
		previous = d.Version
	}
	return nil
}

func validateRestoredRetirements(st smState) error {
	retired := make(map[string]bool)
	for _, r := range st.RetiredGroups {
		if r.Version == 0 || r.Version > st.Latest.Version || retired[r.ID] || topology.ValidateLoadID(r.ID) != nil {
			return fault.New(fault.Unreadable, "invalid retired group", nil)
		}
		retired[r.ID] = true
		if st.Latest.Group(r.ID) != nil || st.Target.Group(r.ID) != nil {
			return fault.New(fault.Unreadable, "retired group appears in desired topology", nil)
		}
	}
	return nil
}

func validateRestoredAllocations(st smState) error {
	alloc := make(map[string]uint64)
	for _, a := range st.Allocations {
		if a.NextReplicaID == 0 || alloc[a.GroupID] != 0 || topology.ValidateID(a.GroupID) != nil {
			return fault.New(fault.Unreadable, "invalid allocator record", nil)
		}
		alloc[a.GroupID] = a.NextReplicaID
	}
	for _, d := range []topology.Document{st.Latest, st.Target, st.Active} {
		for _, g := range d.Groups {
			if g.NextReplicaID > alloc[g.ID] {
				return fault.New(fault.Unreadable, "missing allocation high water", nil)
			}
		}
	}
	return nil
}

func validateRestoredSessions(st smState) error {
	sessions := make(map[string]bool)
	for _, s := range st.Sessions {
		if s.SessionID == "" || s.NodeName == "" || s.InstanceID == "" || s.TTLSeconds <= 0 || sessions[s.SessionID] {
			return fault.New(fault.Unreadable, "invalid session record", nil)
		}
		sessions[s.SessionID] = true
	}
	locks := make(map[string]bool)
	for _, l := range st.Locks {
		if l.Key == "" || !sessions[l.SessionID] || locks[l.Key] {
			return fault.New(fault.Unreadable, "invalid lock record", nil)
		}
		locks[l.Key] = true
	}
	return nil
}

func validateRestoredOperators(st smState) (map[topology.OperatorID]bool, error) {
	seen := make(map[topology.OperatorID]bool)
	plans := make(map[uint64]topology.Plan)
	for _, v := range st.Operators {
		r := operatorFromView(v)
		if r.Version == 0 || r.Version > st.Latest.Version || r.Attempt == 0 || seen[r.ID] ||
			v.Plan.Version != r.Version || r.CurrentStep < 0 || r.CurrentStep > len(r.Steps) ||
			v.Status.TotalSteps != len(r.Steps) || r.ID != topology.MakeOperatorID(r.Version, r.Attempt) ||
			validateExactPlan(v.Plan) != nil || !r.State.Valid() || r.StartedAt.IsZero() ||
			r.UpdatedAt.Before(r.StartedAt) {
			return nil, fault.New(fault.Unreadable, "invalid execution record", nil)
		}
		if plan, exists := plans[r.Version]; exists && !plan.Equal(operatorRecordPlan(r)) {
			return nil, fault.New(fault.Unreadable, "execution attempts disagree on their immutable plan", nil)
		}
		plans[r.Version] = operatorRecordPlan(r)
		if err := validateRestoredProgress(r); err != nil {
			return nil, err
		}
		seen[r.ID] = true
	}
	return seen, nil
}

func validateRestoredAdmissions(st smState, seen map[topology.OperatorID]bool) error {
	admissions := make(map[string]AdmissionRecord)
	liveAdmissions := make(map[string]bool)
	for _, a := range st.Admissions {
		key := admissionKey(a.NodeName, a.InstanceID)
		_, exists := admissions[key]
		identityErr := validateAdmissionIdentity(a)
		if exists || identityErr != nil {
			return fault.New(fault.Unreadable, "invalid admission record", nil)
		}
		admissions[key] = a
		if !a.State.valid() {
			return fault.New(fault.Unreadable, "unknown admission state", nil)
		}
		if a.ReadmissionOperatorID != "" && !seen[topology.OperatorID(a.ReadmissionOperatorID)] {
			return fault.New(fault.Unreadable, "missing admission origin operation", nil)
		}
		if !a.State.terminal() {
			if !seen[a.OperatorID] || liveAdmissions[a.NodeName] {
				return fault.New(fault.Unreadable, "invalid live admission execution", nil)
			}
			liveAdmissions[a.NodeName] = true
		}
	}
	return validateReadmissionOrigins(st.Admissions, admissions)
}

func validateReadmissionOrigins(records []AdmissionRecord, admissions map[string]AdmissionRecord) error {
	for _, a := range records {
		if a.ReadmissionInstanceID == "" {
			continue
		}
		original, found := admissions[admissionKey(a.NodeName, a.ReadmissionInstanceID)]
		if !found || original.ReadmissionInstanceID != "" || original.MemberID != a.MemberID ||
			original.State != AdmissionSuperseded || original.originOperator() != a.originOperator() {
			return fault.New(fault.Unreadable, "readmission origin does not name the exact superseded admission", nil)
		}
	}
	return nil
}

func (sm *topologySM) retiredIDs() map[string]bool {
	ids := make(map[string]bool, len(sm.retired))
	for id := range sm.retired {
		ids[id] = true
	}
	return ids
}

// A staged removal retires no identity. Target identities remain available
// until
// an approved replacement/removal fences them; Latest replacements still forbid
// restoring a different ID for the same node, even before approval.
func (sm *topologySM) availableIdentities() topology.Document {
	return retainedIdentities(sm.latest, sm.target)
}

func retainedIdentities(latest, target topology.Document) topology.Document {
	base := latest.Clone()
	for _, group := range target.Groups {
		current := base.Group(group.ID)
		if current == nil {
			base.Groups = append(base.Groups, group)
			continue
		}
		for _, r := range group.Replicas {
			conflict := false
			for _, retained := range current.Replicas {
				if retained.NodeName == r.NodeName || retained.ReplicaID == r.ReplicaID {
					conflict = true
					break
				}
			}
			if !conflict {
				current.Replicas = append(current.Replicas, r)
			}
		}
	}
	return base
}

func hasRestoredProgress(r operatorRecord) bool {
	return r.CurrentStep != 0 || r.GroupID != "" || r.NodeName != "" || r.Error != "" || r.WaitingReason != ""
}

func validateRestoredProgress(r operatorRecord) error {
	if r.State == topology.OperatorPending {
		if hasRestoredProgress(r) {
			return fault.New(fault.Unreadable, "pending execution has progress", nil)
		}
		return nil
	}
	transition := topology.OperatorTransition{State: r.State, StepSeq: r.CurrentStep, GroupID: r.GroupID,
		NodeName: r.NodeName, Error: r.Error, WaitingReason: r.WaitingReason}
	if err := validateTransition(r, transition); err != nil {
		// Supersession before the first effect is a valid approval boundary.
		if r.State != topology.OperatorSuperseded || hasRestoredProgress(r) {
			return fault.New(fault.Unreadable, "invalid restored execution progress", err)
		}
	}
	return nil
}

func (sm *topologySM) validateGroupAllocation(g topology.Group) error {
	floor := sm.allocations[g.ID]
	if g.NextReplicaID < floor {
		return fault.New(fault.Invalid, "allocator regressed below historical high water", nil)
	}
	for _, r := range g.Replicas {
		if r.ReplicaID >= floor {
			continue
		}
		if err := sm.validateHistoricalReplica(g.ID, r); err != nil {
			return err
		}
	}
	return nil
}

func (sm *topologySM) validateHistoricalReplica(groupID string, r topology.Replica) error {
	retained := false
	for _, view := range []topology.Document{sm.latest, sm.target} {
		if old := documentReplica(view, groupID, r.ReplicaID); old != nil && old.NodeName == r.NodeName {
			retained = true
			break
		}
	}
	if !retained {
		return fault.New(fault.Invalid, fmt.Sprintf("group %s reuses historical replica %d", groupID,
			r.ReplicaID), nil)
	}
	// An approved replacement/removal fences the old Active identity
	// even while its cleanup is unfinished and a later document tries
	// to restore it.
	if sm.target.Version > sm.active.Version {
		if old := documentReplica(sm.active, groupID, r.ReplicaID); old != nil && old.NodeName == r.NodeName &&
			documentReplica(sm.target, groupID, r.ReplicaID) == nil {
			return fault.New(fault.Invalid, "replica was retired by approved topology", nil)
		}
	}
	return nil
}
