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
		if previous.OperatorID == ex.ID && previous.Version == ex.Version && previous.Attempt == ex.Attempt &&
			previous.StepSeq == step.Seq {
			return
		}
	}
	r := topology.RoleChangeRecord{
		OperatorID: ex.ID,
		Version:    ex.Version,
		Attempt:    ex.Attempt,
		StepSeq:    step.Seq,
		GroupID:    step.GroupID,
		NodeName:   step.NodeName,
		ReplicaID:  step.ReplicaID,
		From:       from,
		To:         to,
		At:         at,
		Index:      index,
	}
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
	slices.SortFunc(state.Replicas, func(a, b topology.RoleChangeRecord) int {
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
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
		state.Replicas = slices.DeleteFunc(
			state.Replicas,
			func(r topology.RoleChangeRecord) bool {
				// Last also supplies the surviving group's cooldown after
				// removal of its former role holder, and occupies at most one
				// extra slot.
				return r != state.Last &&
					!protected[id][topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}]
			},
		)
		sm.voterSwitches[id] = state
	}
}

func validateVoterSwitches(st smState) error {
	if len(st.VoterSwitches) == 0 {
		return nil
	}
	check := voterSwitchValidation{state: st, allocations: make(map[string]uint64, len(st.Allocations)),
		names: map[voterSwitchKey]string{}, protected: map[voterSwitchKey]bool{}, groups: map[string]bool{},
		seenGroups: map[string]bool{}, seenIndexes: map[uint64]bool{}}
	for _, a := range st.Allocations {
		check.allocations[a.GroupID] = a.NextReplicaID
	}
	if err := check.rememberCurrent(); err != nil {
		return err
	}
	if err := check.rememberHistory(); err != nil {
		return err
	}
	if err := check.rememberOperators(); err != nil {
		return err
	}
	for _, state := range st.VoterSwitches {
		if err := check.groupState(state); err != nil {
			return err
		}
	}
	return nil
}

type voterSwitchKey struct {
	group string
	id    uint64
}

type switchRetention uint8

const (
	archiveSwitchIdentity switchRetention = iota
	retainSwitchIdentity
)

type voterSwitchValidation struct {
	state              smState
	allocations        map[string]uint64
	names              map[voterSwitchKey]string
	protected          map[voterSwitchKey]bool
	groups, seenGroups map[string]bool
	seenIndexes        map[uint64]bool
}

func invalidVoterSwitch() error {
	return fault.New(fault.Unreadable, "invalid voter switch record", nil)
}

func (check *voterSwitchValidation) remember(group, name string, id uint64, retention switchRetention) bool {
	if name == "" || id == 0 {
		return true
	}
	key := voterSwitchKey{group, id}
	if previous := check.names[key]; previous != "" && previous != name {
		return false
	}
	check.names[key] = name
	if retention == retainSwitchIdentity {
		check.protected[key], check.groups[group] = true, true
	}
	return true
}

func (check *voterSwitchValidation) rememberCurrent() error {
	for _, doc := range []topology.Document{check.state.Latest, check.state.Target, check.state.Active} {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				if !check.remember(g.ID, r.NodeName, r.ReplicaID, retainSwitchIdentity) {
					return invalidVoterSwitch()
				}
			}
		}
	}
	return nil
}

func (check *voterSwitchValidation) rememberHistory() error {
	for _, doc := range check.state.History {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				if !check.remember(g.ID, r.NodeName, r.ReplicaID, archiveSwitchIdentity) {
					return invalidVoterSwitch()
				}
			}
		}
	}
	return nil
}

func (check *voterSwitchValidation) rememberOperators() error {
	for _, op := range check.state.Operators {
		for _, step := range op.Plan.Steps {
			if !check.remember(step.GroupID, step.NodeName, step.ReplicaID, retainSwitchIdentity) {
				return invalidVoterSwitch()
			}
		}
	}
	return nil
}

func (check *voterSwitchValidation) valid(r topology.RoleChangeRecord, group string) bool {
	knownName := check.names[voterSwitchKey{group, r.ReplicaID}]
	return r.GroupID == group && r.NodeName != "" && r.ReplicaID != 0 && r.ReplicaID < check.allocations[group] &&
		(knownName == "" || knownName == r.NodeName) && r.Version != 0 && r.Version <= check.state.Target.Version &&
		r.Attempt != 0 && r.StepSeq > 0 && r.Index != 0 && r.Index <= check.state.AppliedIndex &&
		(r.From == topology.ReplicaVoter && r.To == topology.ReplicaLearner ||
			r.From == topology.ReplicaLearner && r.To == topology.ReplicaVoter) &&
		topology.ValidateID(group) == nil && !strings.ContainsRune(r.NodeName, 0) &&
		r.OperatorID == topology.MakeOperatorID(r.Version, r.Attempt) && !r.At.IsZero()
}

func (check *voterSwitchValidation) groupState(state topology.VoterSwitchState) error {
	if check.seenGroups[state.GroupID] || !check.groups[state.GroupID] || !check.valid(state.Last, state.GroupID) {
		return invalidVoterSwitch()
	}
	check.seenGroups[state.GroupID] = true
	var previous uint64
	lastFound := false
	for _, r := range state.Replicas {
		if r.ReplicaID <= previous || r.Index > state.Last.Index || check.seenIndexes[r.Index] ||
			!check.valid(r, state.GroupID) {
			return invalidVoterSwitch()
		}
		// The group's latest effect is retained as its bounded cooldown
		// anchor even if that replica was removed. Other historical rows
		// require a current assignment or retained execution obligation.
		if r != state.Last && !check.protected[voterSwitchKey{state.GroupID, r.ReplicaID}] {
			return invalidVoterSwitch()
		}
		previous = r.ReplicaID
		check.seenIndexes[r.Index] = true
		if r == state.Last {
			lastFound = true
		}
		if err := check.retainedEffect(r); err != nil {
			return err
		}
	}
	if !lastFound {
		return fmt.Errorf("%w: voter switch last effect is absent", fault.ErrUnreadable)
	}
	return nil
}

func (check *voterSwitchValidation) retainedEffect(r topology.RoleChangeRecord) error {
	// When the exact operator is retained, its completed effect must
	// agree. Old current-identity records may outlive that operator.
	for _, op := range check.state.Operators {
		if op.Status.ID != r.OperatorID {
			continue
		}
		if r.StepSeq > len(op.Plan.Steps) {
			return invalidVoterSwitch()
		}
		s := op.Plan.Steps[r.StepSeq-1]
		kind := topology.StepPromoteVoter
		if r.To == topology.ReplicaLearner {
			kind = topology.StepDemoteVoter
		}
		record := operatorFromView(op)
		if s.Kind != kind || s.GroupID != r.GroupID || s.NodeName != r.NodeName || s.ReplicaID != r.ReplicaID ||
			!completedStep(&record, s) {
			return invalidVoterSwitch()
		}
	}
	return nil
}
