package operator

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// ValidateRemainingPlan simulates the immutable suffix against confirmed
// applied membership. It never manufactures an effect receipt or changes the plan.
// Base includes retained host identities still requiring cleanup.
func ValidateRemainingPlan(view RemainingPlanView, ex Record, native map[string]topology.NativeState,
	rules Rules) error {
	first := max(ex.CurrentStep, 1)
	if first > len(ex.Steps) && len(ex.Steps) != 0 {
		return ErrOperatorAuthorization
	}
	proofs, err := remainingStepProofs(ex, native)
	if err != nil {
		return err
	}
	if err := validateRetainedPlan(view, ex); err != nil {
		return err
	}
	validation := remainingPlanValidation{view: view, ex: ex, native: native, rules: rules,
		first: first, proofs: proofs}
	return validation.validateGroups()
}

// RemainingPlanView pairs retained resources with the approved target document.
type RemainingPlanView struct {
	Base, Target Document
}

func remainingPlanError(format string, args ...any) error {
	return fault.New(fault.ReplanRequired, fmt.Sprintf(format, args...), nil)
}

func remainingPlanHas(ex Record, kind StepKind, group string, replica topology.ReplicaKey) bool {
	return slices.ContainsFunc(ex.Steps, func(s Step) bool {
		return s.Kind == kind && s.GroupID == group && (replica.NodeName == "" ||
			s.NodeName == replica.NodeName && s.ReplicaID == replica.ReplicaID)
	})
}

func validateRetainedPlan(view RemainingPlanView, ex Record) error {
	// Host resources can exist without native membership. Earlier steps are
	// durable progress; the full approved plan must cover other retained identities.
	for _, group := range view.Base.Groups {
		if err := validateRetainedGroup(view, ex, group); err != nil {
			return err
		}
	}
	return nil
}

type retainedPlanGroup struct {
	group   Group
	desired *Group
}

func validateRetainedGroup(view RemainingPlanView, ex Record, group Group) error {
	desired := view.Target.Group(group.ID)
	retiredByReadmission := true
	for _, r := range group.Replicas {
		retiredByReadmission = retiredByReadmission &&
			topology.ReadmissionCovers(view.Base, view.Target, ex.Plan(), r.NodeName)
	}
	if desired == nil && !retiredByReadmission &&
		!remainingPlanHas(ex, StepDeleteGroupState, group.ID, topology.ReplicaKey{}) {
		return remainingPlanError("plan omits deletion of retained group %q", group.ID)
	}
	for _, replica := range group.Replicas {
		if err := validateRetainedReplica(view, ex, retainedPlanGroup{group, desired}, replica); err != nil {
			return err
		}
	}
	return nil
}

func validateRetainedReplica(view RemainingPlanView, ex Record, group retainedPlanGroup, replica Replica) error {
	if next := documentReplicaByID(view.Target, group.group.ID, replica.ReplicaID); next != nil &&
		next.NodeName == replica.NodeName {
		return nil
	}
	if topology.ReadmissionCovers(view.Base, view.Target, ex.Plan(), replica.NodeName) {
		return nil
	}
	key := topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	if !remainingPlanHas(ex, StepStopReplica, group.group.ID, key) {
		return remainingPlanError("plan omits stop for retained replica %s/%s/%d", group.group.ID,
			replica.NodeName, replica.ReplicaID)
	}
	if group.group.ID != MetaGroupID && group.desired != nil &&
		!remainingPlanHas(ex, StepCleanupReplicaState, group.group.ID, key) {
		return remainingPlanError("plan omits cleanup for retained replica %s/%s/%d", group.group.ID,
			replica.NodeName, replica.ReplicaID)
	}
	return nil
}

type remainingPlanValidation struct {
	view   RemainingPlanView
	ex     Record
	native map[string]topology.NativeState
	rules  Rules
	first  int
	proofs map[int]bool
}

func (v remainingPlanValidation) validateGroups() error {
	for _, group := range v.view.Target.Groups {
		if err := v.validateGroup(group); err != nil {
			return err
		}
	}
	return nil
}

func (v remainingPlanValidation) validateGroup(group Group) error {
	simulation, err := v.initialGroup(group)
	if err != nil {
		return err
	}
	if err := v.simulateSteps(&simulation); err != nil {
		return err
	}
	return simulation.validateTarget()
}

type remainingGroupSimulation struct {
	group            Group
	exists           bool
	conf             raftpb.ConfState
	voters, learners map[uint64]bool
	maxVoters        int
}

func (v remainingPlanValidation) maxVoters(group Group) int {
	_, limit := v.view.Target.GroupBounds(group.ID)
	if group.ID == MetaGroupID {
		limit = v.rules.Limits.MaxNodes
	}
	if p := group.VoterConfig; p != nil && p.TargetVoters < limit {
		limit = p.TargetVoters + p.MaxSurgeVoters
	}
	return limit
}

func (v remainingPlanValidation) initialGroup(group Group) (remainingGroupSimulation, error) {
	limit := v.maxVoters(group)
	state, exists := v.native[group.ID]
	if exists && (state.GroupID != group.ID || state.Applied == 0 || state.ReplicaID == 0) {
		return remainingGroupSimulation{}, fault.ErrUnreadable
	}
	cs := state.ConfState
	if cs.AutoLeave || len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 {
		return remainingGroupSimulation{}, fault.ErrAuthorityPending
	}
	return remainingGroupSimulation{group: group, exists: exists, conf: cs, maxVoters: limit,
		voters: remainingMembers(cs.Voters), learners: remainingMembers(cs.Learners)}, nil
}

func remainingMembers(ids []uint64) map[uint64]bool {
	members := map[uint64]bool{}
	for _, id := range ids {
		members[id] = true
	}
	return members
}

func (v remainingPlanValidation) simulateSteps(simulation *remainingGroupSimulation) error {
	for _, step := range v.ex.Steps {
		if step.Seq < v.first || step.GroupID != simulation.group.ID {
			continue
		}
		if err := v.simulateStep(simulation, step); err != nil {
			return err
		}
	}
	return nil
}

func (v remainingPlanValidation) simulateStep(simulation *remainingGroupSimulation, step Step) error {
	before := voterIDs(simulation.voters)
	if err := v.validatePredecessor(step, before); err != nil {
		return err
	}
	if err := simulation.applyStep(step); err != nil {
		return err
	}
	return v.validateStepResult(simulation, step, before)
}

func (v remainingPlanValidation) validatePredecessor(step Step, before []uint64) error {
	if v.ex.ID == "" || !membershipStep(step.Kind) {
		return nil
	}
	want := step.ExpectedVotersDigest
	if v.proofs[step.Seq] {
		if step.Seq != v.first {
			return remainingPlanError("step %d has completion evidence ahead of the operator cursor", step.Seq)
		}
		want = step.ResultVotersDigest
	}
	if want == "" || step.ResultVotersDigest == "" || topology.VoterSetDigest(before) != want {
		return remainingPlanError(
			"step %d native voter predecessor changed without matching completion evidence", step.Seq)
	}
	return nil
}

func (s *remainingGroupSimulation) applyStep(step Step) error {
	switch step.Kind {
	case StepCreateFoundingGroup:
		return s.foundGroup()
	case StepAddLearner:
		return s.addLearner(step)
	case StepWaitCaughtUp, StepPromoteVoter, StepDemoteVoter, StepTransferLeader:
		return s.changeMember(step)
	case StepRemoveReplica:
		return s.removeMember(step)
	case StepStopReplica, StepCleanupReplicaState:
		return s.retireMember(step)
	default: // Other steps do not change simulated membership.
		return nil
	}
}

func (s *remainingGroupSimulation) foundGroup() error {
	if s.exists {
		if !foundingMatchesTarget(&s.group, GroupStatus{Found: true, ConfState: s.conf}) {
			return remainingPlanError("group %q no longer matches its immutable founding step", s.group.ID)
		}
		return nil
	}
	for _, r := range s.group.Replicas {
		if r.Role == topology.ReplicaVoter {
			s.voters[r.ReplicaID] = true
		}
	}
	s.exists = true
	return nil
}

func (s *remainingGroupSimulation) addLearner(step Step) error {
	if !s.voters[step.ReplicaID] {
		s.learners[step.ReplicaID] = true
	}
	return nil
}

func (s *remainingGroupSimulation) changeMember(step Step) error {
	present := s.voters[step.ReplicaID] || s.learners[step.ReplicaID]
	if !present {
		return remainingPlanError("step %d requires missing member %s/%d", step.Seq, s.group.ID, step.ReplicaID)
	}
	if step.Kind == StepTransferLeader && !s.voters[step.ReplicaID] {
		return remainingPlanError("step %d transfers to a nonvoter", step.Seq)
	}
	if step.Kind == StepPromoteVoter {
		return s.promoteMember(step)
	}
	if step.Kind == StepDemoteVoter {
		return s.demoteMember(step)
	}
	return nil
}

func (s *remainingGroupSimulation) promoteMember(step Step) error {
	if !s.voters[step.ReplicaID] && len(s.voters)+1 > s.maxVoters {
		return remainingPlanError("step %d would exceed group %q voter capacity", step.Seq, s.group.ID)
	}
	s.voters[step.ReplicaID] = true
	delete(s.learners, step.ReplicaID)
	return nil
}

func (s *remainingGroupSimulation) demoteMember(step Step) error {
	if s.voters[step.ReplicaID] && step.TransferReplicaID != 0 && !s.voters[step.TransferReplicaID] {
		return remainingPlanError("step %d lost its voter transfer prerequisite", step.Seq)
	}
	if s.voters[step.ReplicaID] && len(s.voters)-1 < topology.QuorumOf(len(s.voters)) {
		return remainingPlanError("step %d cannot safely demote the observed voter set", step.Seq)
	}
	delete(s.voters, step.ReplicaID)
	s.learners[step.ReplicaID] = true
	return nil
}

func (s *remainingGroupSimulation) removeMember(step Step) error {
	if s.voters[step.ReplicaID] && len(s.voters)-1 < topology.QuorumOf(len(s.voters)) {
		return remainingPlanError("step %d cannot safely remove from the observed voter set", step.Seq)
	}
	delete(s.voters, step.ReplicaID)
	delete(s.learners, step.ReplicaID)
	return nil
}

func (s *remainingGroupSimulation) retireMember(step Step) error {
	if s.voters[step.ReplicaID] || s.learners[step.ReplicaID] {
		return remainingPlanError("step %d would retire a replica still in native membership", step.Seq)
	}
	return nil
}

func (v remainingPlanValidation) validateStepResult(s *remainingGroupSimulation, step Step, before []uint64) error {
	if membershipStep(step.Kind) {
		after := voterIDs(s.voters)
		if v.ex.ID != "" && topology.VoterSetDigest(after) != step.ResultVotersDigest {
			return remainingPlanError("step %d cannot produce its approved voter result", step.Seq)
		}
		if len(v.view.Base.Nodes) != 0 || len(v.view.Target.Nodes) != 0 {
			sets := topology.VoterSets{Before: before, After: after}
			if err := topology.ValidateVoterSetTransition(v.view.Base, v.view.Target, s.group.ID, sets); err != nil {
				return remainingPlanError("step %d violates retained placement: %v", step.Seq, err)
			}
		}
	}
	return v.validateNativeCapacity(s, step)
}

func (v remainingPlanValidation) validateNativeCapacity(s *remainingGroupSimulation, step Step) error {
	capacity := v.rules.Limits.MaxReplicas
	if s.group.ID == MetaGroupID {
		capacity = v.rules.Limits.MaxNodes
	}
	if len(s.voters)+len(s.learners) > capacity {
		return remainingPlanError("step %d exceeds native replica capacity", step.Seq)
	}
	return nil
}

func (s *remainingGroupSimulation) validateTarget() error {
	if !s.exists {
		return fault.ErrUnavailable
	}
	if len(s.voters)+len(s.learners) != len(s.group.Replicas) {
		return remainingPlanError("remaining plan cannot reach membership of group %q", s.group.ID)
	}
	for _, r := range s.group.Replicas {
		if r.Role == topology.ReplicaVoter && !s.voters[r.ReplicaID] ||
			r.Role == topology.ReplicaLearner && !s.learners[r.ReplicaID] {
			return remainingPlanError("remaining plan cannot reach role of replica %s/%d", s.group.ID, r.ReplicaID)
		}
	}
	return nil
}
