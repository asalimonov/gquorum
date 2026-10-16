package topology

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
)

// PlanOptions combines planning rules with an optional metadata leadership preference.
// A preference is used only when that member is an eligible transfer destination.
type PlanOptions struct {
	Rules                 Rules
	PreferredMetaTransfer uint64
}

// BuildPlan derives safe desired work from approved intent, proven declarations,
// retained effects and confirmed native membership. It does not authorize effects.
func BuildPlan(input PlanInput, desired Document, rules Rules) (Plan, error) {
	return constructPlan(input, desired, PlanOptions{Rules: rules})
}

func constructPlan(input PlanInput, desired Document, options PlanOptions) (Plan, error) {
	// Whole-document completion still consumes this path until Task 77.
	return constructGroupPlan(input, desired, options, planScope{groups: unionGroupIDs(input, desired)})
}

// Remove the flat construction option with the schema-3 caller at Task 77.
// This controls plan construction only, never the active authority model.
type planScope struct {
	groups    []string
	partition bool
}

// Keep full identity evidence; only execution work and native reads are scoped.
func constructGroupPlan(input PlanInput, desired Document, options PlanOptions, scope planScope) (Plan, error) {
	groups := scope.groups
	rules, preferredMetaTransfer := options.Rules, options.PreferredMetaTransfer
	if err := desired.Validate(rules); err != nil {
		return Plan{}, err
	}
	observed, err := plannerObservations(input, desired, groups)
	if err != nil {
		return Plan{}, err
	}
	if err := validatePlanMembers(input, desired); err != nil {
		return Plan{}, err
	}
	if err := observedVoterIdentities(input, desired, observed); err != nil {
		return Plan{}, err
	}
	builder := topologyPlanBuilder{input: input, desired: desired, observed: observed,
		options: options, plan: Plan{Version: desired.Version}}
	for _, groupID := range groups {
		if err := builder.planGroup(groupID); err != nil {
			return Plan{}, err
		}
	}
	plan := builder.plan
	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
	}
	plan = bindReadmissionRetirement(input, desired, plan, scope)
	if err := bindDemotionGuards(&plan, desired, observed, preferredMetaTransfer); err != nil {
		return Plan{}, err
	}
	if err := bindVoterGuards(&plan, observed, desired); err != nil {
		return Plan{}, err
	}
	if scope.partition {
		bindPlanBlocks(input, desired, &plan, groups)
	}
	if err := validatePlanShapeFor(input, desired, plan, groups); err != nil {
		return Plan{}, planUnsafef("%v", err)
	}
	v := plannedStepValidator{input: input, desired: desired, plan: plan}
	if err := v.validate(observed, groups); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func targetReplicaByID(group *Group, id uint64) *Replica {
	if group == nil {
		return nil
	}
	for i := range group.Replicas {
		if group.Replicas[i].ReplicaID == id {
			return &group.Replicas[i]
		}
	}
	return nil
}

func replicaNode(group *Group, id uint64) string {
	if replica := targetReplicaByID(group, id); replica != nil {
		return replica.NodeName
	}
	return ""
}

func (b *topologyPlanBuilder) planDeletedGroup(groupID string) {
	retire := false
	for _, replica := range b.input.formerReplicas(groupID) {
		if replica.Progress != RetainedStopped {
			b.emit(groupID, StepStopReplica, replica.NodeName, replica.ReplicaID)
		}
		retire = retire || replica.Progress == RetainedHosted
	}
	if retire {
		b.emit(groupID, StepDeleteGroupState, "", 0)
	}
}

func validateDestructiveBoundary(groupID string, before int) error {
	after := before - 1
	if after < 1 || after < QuorumOf(before) {
		return planUnsafef("group %q would reduce %d voters to %d, below the current quorum %d", groupID, before, after,
			QuorumOf(before))
	}
	return nil
}

func planGroupBounds(doc *Document, groupID string, rules Rules) (minimum, maximum int) {
	if groupID == MetaGroupID {
		return 1, rules.Limits.MaxNodes
	}
	return doc.GroupBounds(groupID)
}

func (v *plannedStepValidator) validate(observed map[string]GroupStatus, groups []string) error {
	v.seenGroups, v.states = make(map[string]bool), make(map[string]*validationGroup, len(observed))
	for id, status := range observed {
		v.states[id] = newValidationGroup(status)
	}
	for i, step := range v.plan.Steps {
		if err := v.validateOrder(i, step); err != nil {
			return err
		}
		if err := v.applyStep(i, step, v.groupState(step.GroupID)); err != nil {
			return err
		}
	}
	for _, id := range groups {
		if v.desired.Group(id) == nil {
			if err := v.applyDeleteGroupState(Step{GroupID: id}, v.groupState(id)); err != nil {
				return err
			}
		}
	}
	return nil
}

type validationGroup struct {
	found                              bool
	voters, learners                   map[uint64]struct{}
	prepared, waited, removed, stopped map[uint64]bool
}

func transferTarget(group *Group, voters map[uint64]struct{}, exclude, preferred uint64) (Replica, bool) {
	if preferred != 0 && preferred != exclude {
		if replica, ok := preferredTransferTarget(group, voters, preferred); ok {
			return replica, true
		}
	}
	for _, replica := range sortedReplicas(group.Replicas) {
		if replica.ReplicaID == exclude || replica.Role != ReplicaVoter {
			continue
		}
		if _, ok := voters[replica.ReplicaID]; ok {
			return replica, true
		}
	}
	return Replica{}, false
}

func replicaIdentity(previous, target *Group, id uint64) (Replica, bool) {
	for _, group := range []*Group{previous, target} {
		if group == nil {
			continue
		}
		for _, replica := range group.Replicas {
			if replica.ReplicaID == id {
				return replica, true
			}
		}
	}
	return Replica{}, false
}

func unionGroupIDs(input PlanInput, desired Document) []string {
	seen := make(map[string]struct{})
	var ids []string
	add := func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range input.Groups {
		add(id)
	}
	for _, group := range desired.Groups {
		add(group.ID)
	}
	for id := range input.Native {
		add(id)
	}
	for id := range input.Retained {
		add(id)
	}
	slices.SortFunc(ids, comparePlanGroups)
	return ids
}

func idSet(ids []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func cloneIDSet(in map[uint64]struct{}) map[uint64]struct{} {
	out := make(map[uint64]struct{}, len(in))
	for id := range in {
		out[id] = struct{}{}
	}
	return out
}

func sortedReplicas(replicas []Replica) []Replica {
	out := append([]Replica(nil), replicas...)
	slices.SortFunc(out, func(a, b Replica) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	return out
}

func planUnsafef(format string, args ...any) error {
	return fault.New(fault.UnsafeMembership, fmt.Sprintf(format, args...), nil)
}

func validateObservedMembership(status GroupStatus) error {
	cs := status.ConfState
	if cs.AutoLeave || len(cs.VotersOutgoing) > 0 || len(cs.LearnersNext) > 0 {
		return fault.New(fault.Unsupported, "planner does not support joint membership", nil)
	}
	if len(cs.Voters) == 0 {
		return fault.New(fault.Unavailable, "observed group has no established voters", nil)
	}
	seen := make(map[uint64]bool, len(cs.Voters)+len(cs.Learners))
	for _, ids := range [][]uint64{cs.Voters, cs.Learners} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return planUnsafef("group %q reports zero or duplicate member %d", status.GroupID, id)
			}
			seen[id] = true
		}
	}
	return nil
}

// BuildPlanPreferred uses the same rules while preferring an eligible meta
// leadership destination (for example, when draining the current leader).
func BuildPlanPreferred(input PlanInput, desired Document, options PlanOptions) (Plan, error) {
	return constructPlan(input, desired, options)
}

type topologyPlanBuilder struct {
	input    PlanInput
	desired  Document
	observed map[string]GroupStatus
	options  PlanOptions
	plan     Plan
}

func (b *topologyPlanBuilder) emit(groupID string, kind StepKind, nodeName string, replicaID uint64) {
	b.plan.Steps = append(b.plan.Steps, Step{GroupID: groupID, Kind: kind, NodeName: nodeName, ReplicaID: replicaID})
}

func (b *topologyPlanBuilder) observedStatus(groupID string) (GroupStatus, error) {
	status, observedGroup := b.observed[groupID]
	if !observedGroup {
		return GroupStatus{}, fmt.Errorf("%w: no global status for group %q", fault.ErrUnavailable, groupID)
	}
	if status.GroupID != "" && status.GroupID != groupID {
		return GroupStatus{}, planUnsafef("status for group %q identifies group %q", groupID, status.GroupID)
	}
	if status.Found {
		if err := validateObservedMembership(status); err != nil {
			return GroupStatus{}, err
		}
	}
	return status, nil
}

func (b *topologyPlanBuilder) planGroup(groupID string) error {
	target := b.desired.Group(groupID)
	previous := b.input.provenGroup(groupID)
	status, err := b.observedStatus(groupID)
	if err != nil {
		return err
	}

	if target == nil {
		if status.Found && len(b.input.formerReplicas(groupID)) == 0 {
			return planUnsafef("observed group %q has no proven identities for safe stop", groupID)
		}
		b.planDeletedGroup(groupID)
		return nil
	}
	lo, hi := planGroupBounds(&b.desired, groupID, b.options.Rules)
	if voters := len(b.desired.VotersOf(groupID)); voters < lo || voters > hi {
		return planUnsafef("group %q declares %d voters, outside bounds [%d,%d]", groupID, voters, lo, hi)
	}
	if !status.Found {
		return b.planFoundingGroup(groupID, previous, target)
	}
	preferred := b.planMetaTransfer(groupID, status, target)
	moves, err := planVoterMoves(voterPlanTopology{base: b.input, desired: b.desired, groupID: groupID}, status,
		PlanOptions{Rules: b.options.Rules, PreferredMetaTransfer: preferred})
	if err != nil {
		return err
	}
	b.plan.Steps = append(b.plan.Steps, moves...)
	return nil
}

func (b *topologyPlanBuilder) planFoundingGroup(groupID string, previous, target *Group) error {
	if previous != nil || !b.input.foundingGroup(groupID) {
		return fmt.Errorf("%w: established group %q has no observable host", fault.ErrUnavailable,
			groupID)
	}
	b.emit(groupID, StepCreateFoundingGroup, "", 0)
	for _, replica := range sortedReplicas(target.Replicas) {
		if replica.Role == ReplicaLearner {
			b.emit(groupID, StepPrepareReplica, replica.NodeName, replica.ReplicaID)
			b.emit(groupID, StepAddLearner, replica.NodeName, replica.ReplicaID)
		}
	}
	return nil
}

func (b *topologyPlanBuilder) planMetaTransfer(groupID string, status GroupStatus, target *Group) uint64 {
	preferred := uint64(0)
	if groupID == MetaGroupID {
		preferred = b.options.PreferredMetaTransfer
		// A drain changes node administrative state without removing its
		// _meta replica. Move leadership as an explicit authorized step
		// even
		// though membership itself is unchanged.
		if preferred != 0 && status.LeaderID != preferred {
			if leader := targetReplicaByID(target, status.LeaderID); leader != nil {
				b.emit(groupID, StepTransferLeader, replicaNode(target, preferred), preferred)
			}
		}
	}
	return preferred
}

type plannedStepValidator struct {
	input      PlanInput
	desired    Document
	plan       Plan
	seenGroups map[string]bool
	lastGroup  string
	states     map[string]*validationGroup
}

func newValidationGroup(status GroupStatus) *validationGroup {
	return &validationGroup{found: status.Found, voters: idSet(status.ConfState.Voters),
		learners: idSet(status.ConfState.Learners), prepared: make(map[uint64]bool), waited: make(map[uint64]bool),
		removed: make(map[uint64]bool), stopped: make(map[uint64]bool)}
}

func (v *plannedStepValidator) groupState(groupID string) *validationGroup {
	state := v.states[groupID]
	if state == nil {
		state = newValidationGroup(GroupStatus{})
		v.states[groupID] = state
	}
	return state
}

func (v *plannedStepValidator) validateOrder(i int, step Step) error {
	if step.Seq != i+1 {
		return planUnsafef("step position %d has sequence %d", i+1, step.Seq)
	}
	if step.GroupID != v.lastGroup {
		if v.seenGroups[step.GroupID] || (v.lastGroup != "" && comparePlanGroups(step.GroupID, v.lastGroup) < 0) {
			return planUnsafef("group %q appears out of deterministic order", step.GroupID)
		}
		v.seenGroups[step.GroupID] = true
		v.lastGroup = step.GroupID
	}
	return nil
}

func (v *plannedStepValidator) applyStep(i int, step Step, state *validationGroup) error {
	switch step.Kind {
	case StepCreateFoundingGroup:
		return v.applyCreateFoundingGroup(step, state)
	case StepPrepareReplica:
		view := voterPlanTopology{base: v.input, desired: v.desired, groupID: step.GroupID}
		if err := requireCleanupOwnerStop(view, v.plan, step, state.stopped); err != nil {
			return err
		}
		return state.applyPrepareReplica(step)
	case StepAddLearner:
		return v.applyAddLearner(i, step, state)
	case StepWaitCaughtUp:
		return state.applyWaitCaughtUp(step)
	case StepPromoteVoter:
		return v.applyPromoteVoter(i, step, state)
	case StepDemoteVoter:
		return state.applyDemoteVoter(step)
	case StepRemoveReplica:
		return v.applyRemoveReplica(step, state)
	case StepStopReplica:
		return state.applyStopReplica(step)
	case StepCleanupReplicaState:
		return v.applyCleanupReplicaState(i, step, state)
	case StepDeleteGroupState:
		return v.applyDeleteGroupState(step, state)
	case StepTransferLeader:
		return state.applyTransferLeader(step)
	default:
		return planUnsafef("unknown step kind %q", step.Kind)
	}
}

func (v *plannedStepValidator) applyCreateFoundingGroup(step Step, state *validationGroup) error {
	if state.found || !v.input.foundingGroup(step.GroupID) || v.desired.Group(step.GroupID) == nil {
		return planUnsafef("create founding group %q is not a fresh-group operation", step.GroupID)
	}
	state.found = true
	for _, replica := range v.desired.VotersOf(step.GroupID) {
		state.voters[replica.ReplicaID] = struct{}{}
	}

	return nil
}

func (state *validationGroup) applyPrepareReplica(step Step) error {
	state.prepared[step.ReplicaID] = true

	return nil
}

func (v *plannedStepValidator) applyAddLearner(i int, step Step, state *validationGroup) error {
	if !state.prepared[step.ReplicaID] || !adjacentPlanStep(v.plan.Steps, i, StepPrepareReplica, step) {
		return planUnsafef("add learner %s/%d has no prior prepare acknowledgement", step.GroupID,
			step.ReplicaID)
	}
	if _, voter := state.voters[step.ReplicaID]; !voter {
		state.learners[step.ReplicaID] = struct{}{}
	}

	return nil
}

func (state *validationGroup) applyWaitCaughtUp(step Step) error {
	if _, ok := state.learners[step.ReplicaID]; !ok {
		if _, voter := state.voters[step.ReplicaID]; !voter {
			return planUnsafef("wait %s/%d does not name a learner or voter", step.GroupID, step.ReplicaID)
		}
	}
	state.waited[step.ReplicaID] = true

	return nil
}

func (v *plannedStepValidator) applyPromoteVoter(i int, step Step, state *validationGroup) error {
	_, learner := state.learners[step.ReplicaID]
	_, voter := state.voters[step.ReplicaID]
	if (!learner && !voter) || !state.waited[step.ReplicaID] ||
		!adjacentPlanStep(v.plan.Steps, i, StepWaitCaughtUp, step) {
		return planUnsafef("promotion %s/%d lacks learner presence and catch-up wait", step.GroupID,
			step.ReplicaID)
	}
	delete(state.learners, step.ReplicaID)
	state.voters[step.ReplicaID] = struct{}{}

	return nil
}

func (state *validationGroup) applyDemoteVoter(step Step) error {
	if _, ok := state.voters[step.ReplicaID]; !ok {
		return planUnsafef("demotion %s/%d does not name a voter", step.GroupID, step.ReplicaID)
	}
	if step.TransferReplicaID != 0 {
		if _, ok := state.voters[step.TransferReplicaID]; !ok {
			return planUnsafef("demotion guard %s/%d does not target a current voter", step.GroupID,
				step.TransferReplicaID)
		}
	}
	delete(state.voters, step.ReplicaID)
	state.learners[step.ReplicaID] = struct{}{}

	return nil
}

func (v *plannedStepValidator) applyRemoveReplica(step Step, state *validationGroup) error {
	if _, voter := state.voters[step.ReplicaID]; !voter {
		if _, learner := state.learners[step.ReplicaID]; !learner {
			if !v.input.formerReplica(step) {
				return planUnsafef("remove %s/%d has no retained owner", step.GroupID, step.ReplicaID)
			}
		}
	}
	delete(state.voters, step.ReplicaID)
	delete(state.learners, step.ReplicaID)
	state.removed[step.ReplicaID] = true

	return nil
}

func (state *validationGroup) applyStopReplica(step Step) error {
	state.stopped[step.ReplicaID] = true

	return nil
}

func (v *plannedStepValidator) applyCleanupReplicaState(i int, step Step, state *validationGroup) error {
	if step.GroupID == MetaGroupID || !state.removed[step.ReplicaID] || !state.stopped[step.ReplicaID] ||
		!adjacentPlanStep(v.plan.Steps, i, StepStopReplica, step) ||
		!adjacentPlanStep(v.plan.Steps, i-1, StepRemoveReplica, step) {
		return planUnsafef("cleanup %s/%d lacks exact removal and stop", step.GroupID, step.ReplicaID)
	}

	return nil
}

func (v *plannedStepValidator) applyDeleteGroupState(step Step, state *validationGroup) error {
	for _, replica := range v.input.formerReplicas(step.GroupID) {
		if replica.Progress != RetainedStopped && !state.stopped[replica.ReplicaID] &&
			!ReadmissionCovers(v.input, v.desired, v.plan, replica.NodeName) {
			return planUnsafef("delete group %q lacks stop for replica %d", step.GroupID, replica.ReplicaID)
		}
	}

	return nil
}

func (state *validationGroup) applyTransferLeader(step Step) error {
	if _, ok := state.voters[step.ReplicaID]; !ok {
		return planUnsafef("leadership transfer %s/%d does not target a voter", step.GroupID, step.ReplicaID)
	}

	return nil
}

func preferredTransferTarget(group *Group, voters map[uint64]struct{}, preferred uint64) (Replica, bool) {
	for _, replica := range group.Replicas {
		if replica.ReplicaID == preferred && replica.Role == ReplicaVoter {
			if _, ok := voters[preferred]; ok {
				return replica, true
			}
		}
	}
	return Replica{}, false
}
