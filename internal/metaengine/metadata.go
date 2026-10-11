package metaengine

import (
	"fmt"
	"reflect"
	"sort"

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
	sort.Slice(d.Nodes, func(i, j int) bool { return d.Nodes[i].Name < d.Nodes[j].Name })
	sort.Slice(d.Groups, func(i, j int) bool { return d.Groups[i].ID < d.Groups[j].ID })
	for i := range d.Groups {
		sort.Slice(d.Groups[i].Replicas, func(a, b int) bool { return d.Groups[i].Replicas[a].ReplicaID < d.Groups[i].Replicas[b].ReplicaID })
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
	sort.Slice(result, func(i, j int) bool { return result[i].GroupID < result[j].GroupID })
	return result
}
func (sm *topologySM) retiredSnapshot() []retiredGroup {
	result := make([]retiredGroup, 0, len(sm.retired))
	for id, version := range sm.retired {
		result = append(result, retiredGroup{id, version})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func (sm *topologySM) retireRemovedGroups(desired topology.Document) {
	for _, previous := range []topology.Document{sm.active, sm.target} {
		for _, g := range previous.Groups {
			if g.ID != topology.MetaGroupID && desired.Group(g.ID) == nil && sm.retired[g.ID] == 0 {
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
	for _, g := range d.Groups {
		floor := sm.allocations[g.ID]
		if g.NextReplicaID < floor {
			return fault.New(fault.Invalid, "allocator regressed below historical high water", nil)
		}
		for _, r := range g.Replicas {
			if r.ReplicaID >= floor {
				continue
			}
			retained := false
			for _, view := range []topology.Document{sm.latest, sm.target} {
				if old := documentReplica(view, g.ID, r.ReplicaID); old != nil && old.NodeName == r.NodeName {
					retained = true
					break
				}
			}
			if !retained {
				return fault.New(fault.Invalid, fmt.Sprintf("group %s reuses historical replica %d", g.ID, r.ReplicaID), nil)
			}
			// An approved replacement/removal fences the old Active identity even
			// while its cleanup is unfinished and a later document tries to restore it.
			if sm.target.Version > sm.active.Version {
				if old := documentReplica(sm.active, g.ID, r.ReplicaID); old != nil && old.NodeName == r.NodeName && documentReplica(sm.target, g.ID, r.ReplicaID) == nil {
					return fault.New(fault.Invalid, "replica was retired by approved topology", nil)
				}
			}
		}
	}
	return nil
}
func operatorFromView(v topology.Operator) operatorRecord {
	s := v.Status
	return operatorRecord{ID: s.ID, Version: s.Version, Attempt: s.Attempt, State: s.State, Steps: append([]topology.Step(nil), v.Plan.Steps...), CurrentStep: s.CurrentStep, GroupID: s.GroupID, NodeName: s.NodeName, WaitingReason: s.WaitingReason, Error: s.Error, Origin: v.Origin, Actor: v.Actor, StartedAt: v.StartedAt, UpdatedAt: v.UpdatedAt}
}
func validateRestoredState(st smState, rules topology.Rules) error {
	if st.SchemaVersion != 1 {
		return fault.New(fault.Unsupported, "unsupported meta state schema", nil)
	}
	if !reflect.DeepEqual(st.Rules, rules) || rules.SchemaVersion != 1 || rules.Limits.Validate() != nil {
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
	if (st.AppliedIndex == 0) != (st.AppliedTerm == 0) {
		return fault.New(fault.Unreadable, "invalid applied index/term", nil)
	}
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
	retired := make(map[string]bool)
	for _, r := range st.RetiredGroups {
		if topology.ValidateLoadID(r.ID) != nil || r.Version == 0 || r.Version > st.Latest.Version || retired[r.ID] {
			return fault.New(fault.Unreadable, "invalid retired group", nil)
		}
		retired[r.ID] = true
		if st.Latest.Group(r.ID) != nil || st.Target.Group(r.ID) != nil {
			return fault.New(fault.Unreadable, "retired group appears in desired topology", nil)
		}
	}
	alloc := make(map[string]uint64)
	for _, a := range st.Allocations {
		if topology.ValidateID(a.GroupID) != nil || a.NextReplicaID == 0 || alloc[a.GroupID] != 0 {
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
	seen := make(map[topology.OperatorID]bool)
	for _, v := range st.Operators {
		r := operatorFromView(v)
		if r.Version == 0 || r.Version > st.Latest.Version || r.Attempt == 0 || r.ID != topology.MakeOperatorID(r.Version, r.Attempt) || seen[r.ID] || v.Plan.Version != r.Version || validateExactPlan(v.Plan) != nil || r.CurrentStep < 0 || r.CurrentStep > len(r.Steps) || !r.State.Valid() || v.Status.TotalSteps != len(r.Steps) || r.StartedAt.IsZero() || r.UpdatedAt.Before(r.StartedAt) {
			return fault.New(fault.Unreadable, "invalid execution record", nil)
		}
		if r.State == topology.OperatorPending {
			if r.CurrentStep != 0 || r.GroupID != "" || r.NodeName != "" || r.Error != "" || r.WaitingReason != "" {
				return fault.New(fault.Unreadable, "pending execution has progress", nil)
			}
		} else if err := validateTransition(r, topology.OperatorTransition{State: r.State, StepSeq: r.CurrentStep, GroupID: r.GroupID, NodeName: r.NodeName, Error: r.Error, WaitingReason: r.WaitingReason}); err != nil {
			// Supersession before the first effect is a valid approval boundary.
			if r.State != topology.OperatorSuperseded || r.CurrentStep != 0 || r.GroupID != "" || r.NodeName != "" || r.Error != "" || r.WaitingReason != "" {
				return fault.New(fault.Unreadable, "invalid restored execution progress", err)
			}
		}
		seen[r.ID] = true
	}
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
	admissions := make(map[string]bool)
	liveAdmissions := make(map[string]bool)
	for _, a := range st.Admissions {
		key := admissionKey(a.NodeName, a.InstanceID)
		if validateAdmissionIdentity(a) != nil || admissions[key] {
			return fault.New(fault.Unreadable, "invalid admission record", nil)
		}
		admissions[key] = true
		if !a.State.valid() {
			return fault.New(fault.Unreadable, "unknown admission state", nil)
		}
		if !a.State.terminal() {
			if !seen[a.OperatorID] || liveAdmissions[a.NodeName] {
				return fault.New(fault.Unreadable, "invalid live admission execution", nil)
			}
			liveAdmissions[a.NodeName] = true
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

// A staged removal retires no identity. Target identities remain available until
// an approved replacement/removal fences them; Latest replacements still forbid
// restoring a different ID for the same node, even before approval.
func (sm *topologySM) availableIdentities() topology.Document {
	base := sm.latest.Clone()
	for _, group := range sm.target.Groups {
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
