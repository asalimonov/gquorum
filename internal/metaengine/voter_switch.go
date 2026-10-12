package metaengine

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (sm *topologySM) VoterSwitches() []topology.VoterSwitchState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.voterSwitchSnapshotLocked()
}

func (sm *topologySM) voterSwitchSnapshotLocked() []topology.VoterSwitchState {
	if len(sm.voterSwitches) == 0 {
		return nil
	}
	out := make([]topology.VoterSwitchState, 0, len(sm.voterSwitches))
	for _, state := range sm.voterSwitches {
		out = append(out, state.Clone())
	}
	slices.SortFunc(out, func(a, b topology.VoterSwitchState) int { return cmp.Compare(a.GroupID, b.GroupID) })
	return out
}

func (sm *topologySM) recordRoleEffectLocked(index uint64, ex *operatorRecord, step topology.Step, at time.Time) {
	from, to := topology.ReplicaLearner, topology.ReplicaVoter
	switch step.Kind {
	case topology.StepPromoteVoter:
	case topology.StepDemoteVoter:
		from, to = to, from
	default:
		return
	}
	state := sm.voterSwitches[step.GroupID]
	for _, previous := range state.Replicas {
		if previous.OperatorID == ex.ID && previous.Version == ex.Version && previous.Attempt == ex.Attempt && previous.StepSeq == step.Seq {
			return
		}
	}
	r := topology.RoleChangeRecord{OperatorID: ex.ID, Version: ex.Version, Attempt: ex.Attempt, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, From: from, To: to, At: at, Index: index}
	state.GroupID, state.Last = step.GroupID, r
	found := false
	for i, old := range state.Replicas {
		if old.NodeName == r.NodeName && old.ReplicaID == r.ReplicaID {
			state.Replicas[i] = r
			found = true
			break
		}
	}
	if !found {
		state.Replicas = append(state.Replicas, r)
	}
	slices.SortFunc(state.Replicas, func(a, b topology.RoleChangeRecord) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	sm.voterSwitches[step.GroupID] = state
}

// Records outlive ordinary topology history. Bound obsolete identities by the
// approved execution/cleanup evidence already retained by the metadata machine.
func (sm *topologySM) pruneVoterSwitchesLocked() {
	protected := make(map[string]map[topology.ReplicaKey]bool)
	protect := func(group, name string, id uint64) {
		if protected[group] == nil {
			protected[group] = make(map[topology.ReplicaKey]bool)
		}
		protected[group][topology.ReplicaKey{NodeName: name, ReplicaID: id}] = true
	}
	for _, doc := range []topology.Document{sm.latest, sm.target, sm.active} {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				protect(g.ID, r.NodeName, r.ReplicaID)
			}
		}
	}
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			protect(step.GroupID, step.NodeName, step.ReplicaID)
		}
	}
	for id, state := range sm.voterSwitches {
		if len(protected[id]) == 0 {
			delete(sm.voterSwitches, id)
			continue
		}
		state.Replicas = slices.DeleteFunc(state.Replicas, func(r topology.RoleChangeRecord) bool {
			// Last also supplies the surviving group's cooldown after removal
			// of its former role holder, and occupies at most one extra slot.
			return r != state.Last && !protected[id][topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}]
		})
		sm.voterSwitches[id] = state
	}
}

func validateVoterSwitches(st smState) error {
	if len(st.VoterSwitches) == 0 {
		return nil
	}
	bad := func() error { return fault.New(fault.Unreadable, "invalid voter switch record", nil) }
	allocations := make(map[string]uint64, len(st.Allocations))
	for _, a := range st.Allocations {
		allocations[a.GroupID] = a.NextReplicaID
	}
	type replicaKey struct {
		group string
		id    uint64
	}
	names := map[replicaKey]string{}
	protected := map[replicaKey]bool{}
	groups := map[string]bool{}
	remember := func(group, name string, id uint64, keep bool) bool {
		if name == "" || id == 0 {
			return true
		}
		key := replicaKey{group, id}
		if previous := names[key]; previous != "" && previous != name {
			return false
		}
		names[key] = name
		if keep {
			protected[key], groups[group] = true, true
		}
		return true
	}
	for _, doc := range []topology.Document{st.Latest, st.Target, st.Active} {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				if !remember(g.ID, r.NodeName, r.ReplicaID, true) {
					return bad()
				}
			}
		}
	}
	for _, doc := range st.History {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				if !remember(g.ID, r.NodeName, r.ReplicaID, false) {
					return bad()
				}
			}
		}
	}
	for _, op := range st.Operators {
		for _, step := range op.Plan.Steps {
			if !remember(step.GroupID, step.NodeName, step.ReplicaID, true) {
				return bad()
			}
		}
	}
	valid := func(r topology.RoleChangeRecord, group string) bool {
		knownName := names[replicaKey{group, r.ReplicaID}]
		return r.GroupID == group && topology.ValidateID(group) == nil && r.NodeName != "" && !strings.ContainsRune(r.NodeName, 0) && r.ReplicaID != 0 && r.ReplicaID < allocations[group] && (knownName == "" || knownName == r.NodeName) &&
			r.Version != 0 && r.Version <= st.Target.Version && r.Attempt != 0 && r.OperatorID == topology.MakeOperatorID(r.Version, r.Attempt) && r.StepSeq > 0 &&
			r.Index != 0 && r.Index <= st.AppliedIndex && !r.At.IsZero() &&
			(r.From == topology.ReplicaVoter && r.To == topology.ReplicaLearner || r.From == topology.ReplicaLearner && r.To == topology.ReplicaVoter)
	}
	seenGroups := make(map[string]bool)
	seenIndexes := make(map[uint64]bool)
	for _, state := range st.VoterSwitches {
		if seenGroups[state.GroupID] || !groups[state.GroupID] || !valid(state.Last, state.GroupID) {
			return bad()
		}
		seenGroups[state.GroupID] = true
		var previous uint64
		lastFound := false
		for _, r := range state.Replicas {
			if !valid(r, state.GroupID) || r.ReplicaID <= previous || r.Index > state.Last.Index || seenIndexes[r.Index] {
				return bad()
			}
			// The group's latest effect is retained as its bounded cooldown
			// anchor even if that replica was removed. Other historical rows
			// require a current assignment or retained execution obligation.
			if r != state.Last && !protected[replicaKey{state.GroupID, r.ReplicaID}] {
				return bad()
			}
			previous = r.ReplicaID
			seenIndexes[r.Index] = true
			if r == state.Last {
				lastFound = true
			}
			// When the exact operator is retained, its completed effect must
			// agree. Old current-identity records may outlive that operator.
			for _, op := range st.Operators {
				if op.Status.ID != r.OperatorID {
					continue
				}
				if r.StepSeq > len(op.Plan.Steps) {
					return bad()
				}
				s := op.Plan.Steps[r.StepSeq-1]
				kind := topology.StepPromoteVoter
				if r.To == topology.ReplicaLearner {
					kind = topology.StepDemoteVoter
				}
				record := operatorFromView(op)
				if s.Kind != kind || s.GroupID != r.GroupID || s.NodeName != r.NodeName || s.ReplicaID != r.ReplicaID || !completedStep(&record, s) {
					return bad()
				}
			}
		}
		if !lastFound {
			return fmt.Errorf("%w: voter switch last effect is absent", fault.ErrUnreadable)
		}
	}
	return nil
}
