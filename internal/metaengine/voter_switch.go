package metaengine

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// VoterSwitches returns owned cooldown proof for scheduler decisions, including
// archived effects whose operation has been pruned.
func (m *Machine) VoterSwitches() []topology.VoterSwitchState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rows := make([]topology.VoterSwitchState, len(m.scoped.VoterSwitches))
	for i, state := range m.scoped.VoterSwitches {
		rows[i] = state.Clone()
	}
	return rows
}

func recordRoleChange(state topology.VoterSwitchState, r topology.RoleChangeRecord) topology.VoterSwitchState {
	for _, previous := range state.Replicas {
		if previous.OperationID == r.OperationID && previous.Version == r.Version && previous.Attempt == r.Attempt &&
			previous.StepSeq == r.StepSeq {
			return state
		}
	}
	state.GroupID, state.Last = r.GroupID, r
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
	return state
}

func newVoterSwitchValidation(floors []allocationRecord, targetVersion, appliedIndex uint64) voterSwitchValidation {
	check := voterSwitchValidation{targetVersion: targetVersion, appliedIndex: appliedIndex,
		allocations: make(map[string]uint64, len(floors)),
		names:       map[voterSwitchKey]string{}, protected: map[voterSwitchKey]bool{}, groups: map[string]bool{},
		seenGroups: map[string]bool{}, seenIndexes: map[uint64]bool{}}
	for _, a := range floors {
		check.allocations[a.GroupID] = a.NextReplicaID
	}
	return check
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
	targetVersion      uint64
	appliedIndex       uint64
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

func (check *voterSwitchValidation) rememberDocuments(docs []topology.Document, retention switchRetention) error {
	for _, doc := range docs {
		for _, group := range doc.Groups {
			for _, replica := range group.Replicas {
				if !check.remember(group.ID, replica.NodeName, replica.ReplicaID, retention) {
					return invalidVoterSwitch()
				}
			}
		}
	}
	return nil
}

func (check *voterSwitchValidation) valid(r topology.RoleChangeRecord, group string) bool {
	knownName := check.names[voterSwitchKey{group, r.ReplicaID}]
	return r.GroupID == group && r.NodeName != "" && r.ReplicaID != 0 && r.ReplicaID < check.allocations[group] &&
		(knownName == "" || knownName == r.NodeName) && r.Version != 0 && r.Version <= check.targetVersion &&
		r.Attempt != 0 && r.StepSeq > 0 && r.Index != 0 && r.Index <= check.appliedIndex &&
		(r.From == topology.ReplicaVoter && r.To == topology.ReplicaLearner ||
			r.From == topology.ReplicaLearner && r.To == topology.ReplicaVoter) &&
		topology.ValidateID(group) == nil && !strings.ContainsRune(r.NodeName, 0) &&
		r.OperationID == topology.MakeOperationID(r.Version, r.Attempt) && !r.At.IsZero()
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
	}
	if !lastFound {
		return fmt.Errorf("%w: voter switch last effect is absent", fault.ErrUnreadable)
	}
	return nil
}
