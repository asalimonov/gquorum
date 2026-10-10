package topology

import (
	"fmt"
	"github.com/asalimonov/gquorum/fault"
	"sort"
)

// BuildPlan diffs desired against the fully converged Active document and
// global Raft group observations. Active supplies established identities that
// desired may have removed; observed supplies the live membership truth.
func BuildPlan(active, desired Document, observed map[string]GroupStatus, rules Rules) (Plan, error) {
	return buildPlan(active, desired, observed, rules, 0)
}

func buildPlan(active, desired Document, observed map[string]GroupStatus, rules Rules, preferredMetaTransfer uint64) (Plan, error) {
	if err := desired.Validate(rules); err != nil {
		return Plan{}, err
	}
	plan := Plan{Version: desired.Version}
	emit := func(groupID string, kind StepKind, nodeName string, replicaID uint64) {
		plan.Steps = append(plan.Steps, Step{
			GroupID: groupID, Kind: kind, NodeName: nodeName, ReplicaID: replicaID,
		})
	}

	for _, groupID := range unionGroupIDs(active, desired, observed) {
		target := desired.Group(groupID)
		previous := active.Group(groupID)
		status, observedGroup := observed[groupID]
		if !observedGroup {
			return Plan{}, fmt.Errorf("%w: no global status for group %q", fault.ErrUnavailable, groupID)
		}
		if status.GroupID != "" && status.GroupID != groupID {
			return Plan{}, planUnsafef("status for group %q identifies group %q", groupID, status.GroupID)
		}
		if status.Found {
			if err := validateObservedMembership(status); err != nil {
				return Plan{}, err
			}
		}

		if target == nil {
			if previous == nil && status.Found {
				return Plan{}, planUnsafef("observed group %q has no Active identities for safe stop", groupID)
			}
			planDeletedGroup(groupID, previous, emit)
			continue
		}
		lo, hi := planGroupBounds(&desired, groupID, rules)
		if voters := len(desired.VotersOf(groupID)); voters < lo || voters > hi {
			return Plan{}, planUnsafef("group %q declares %d voters, outside bounds [%d,%d]",
				groupID, voters, lo, hi)
		}
		if !status.Found {
			if previous != nil {
				return Plan{}, fmt.Errorf("%w: established group %q has no observable host", fault.ErrUnavailable, groupID)
			}
			emit(groupID, StepCreateFoundingGroup, "", 0)
			for _, replica := range sortedReplicas(target.Replicas) {
				if replica.Role == ReplicaLearner {
					emit(groupID, StepPrepareReplica, replica.NodeName, replica.ReplicaID)
					emit(groupID, StepAddLearner, replica.NodeName, replica.ReplicaID)
				}
			}
			continue
		}
		preferred := uint64(0)
		if groupID == MetaGroupID {
			preferred = preferredMetaTransfer
			// A drain changes node administrative state without removing its
			// _meta replica. Move leadership as an explicit authorized step even
			// though membership itself is unchanged.
			if preferred != 0 && status.LeaderID != preferred {
				if leader := targetReplicaByID(target, status.LeaderID); leader != nil {
					emit(groupID, StepTransferLeader, replicaNode(target, preferred), preferred)
				}
			}
		}
		if err := planEstablishedGroup(rules, &desired, previous, target, status, preferred, emit); err != nil {
			return Plan{}, err
		}
	}

	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
	}
	if err := ValidatePlanShape(active, desired, plan); err != nil {
		return Plan{}, planUnsafef("%v", err)
	}
	if err := validatePlannedSteps(active, desired, observed, plan); err != nil {
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

func planDeletedGroup(groupID string, previous *Group, emit func(string, StepKind, string, uint64)) {
	if previous != nil {
		for _, replica := range sortedReplicas(previous.Replicas) {
			emit(groupID, StepStopReplica, replica.NodeName, replica.ReplicaID)
		}
	}
	emit(groupID, StepDeleteGroupState, "", 0)
}

func planEstablishedGroup(
	rules Rules,
	doc *Document,
	previous *Group,
	target *Group,
	status GroupStatus,
	preferredTransfer uint64,
	emit func(string, StepKind, string, uint64),
) error {
	haveVoters := idSet(status.ConfState.Voters)
	haveLearners := idSet(status.ConfState.Learners)
	want := make(map[uint64]Replica, len(target.Replicas))
	for _, replica := range target.Replicas {
		want[replica.ReplicaID] = replica
	}

	var additions []Replica
	var demotions []Replica
	for _, replica := range sortedReplicas(target.Replicas) {
		_, voter := haveVoters[replica.ReplicaID]
		_, learner := haveLearners[replica.ReplicaID]
		switch replica.Role {
		case ReplicaVoter:
			if !voter {
				additions = append(additions, replica)
			}
		case ReplicaLearner:
			if voter {
				demotions = append(demotions, replica)
			} else if !learner {
				additions = append(additions, replica)
			}
		}
	}

	var removals []Replica
	for id := range haveVoters {
		if _, keep := want[id]; !keep {
			replica, ok := replicaIdentity(previous, target, id)
			if !ok {
				return planUnsafef("group %q cannot map live voter %d to a node", target.ID, id)
			}
			removals = append(removals, replica)
		}
	}
	for id := range haveLearners {
		if _, keep := want[id]; !keep {
			replica, ok := replicaIdentity(previous, target, id)
			if !ok {
				return planUnsafef("group %q cannot map live learner %d to a node", target.ID, id)
			}
			removals = append(removals, replica)
		}
	}
	sort.Slice(removals, func(i, j int) bool { return removals[i].ReplicaID < removals[j].ReplicaID })

	promotions := 0
	for _, replica := range additions {
		if replica.Role == ReplicaVoter {
			promotions++
		}
	}
	voterDestructions := len(demotions)
	for _, replica := range removals {
		if _, voter := haveVoters[replica.ReplicaID]; voter {
			voterDestructions++
		}
	}
	_, hi := planGroupBounds(doc, target.ID, rules)
	shrinkFirst := promotions > 0 && voterDestructions > 0 && len(haveVoters)+promotions > hi

	currentVoters := cloneIDSet(haveVoters)
	currentLeader := status.LeaderID
	emitDestructive := func() error {
		for _, replica := range demotions {
			if err := validateDestructiveBoundary(target.ID, len(currentVoters)); err != nil {
				return err
			}
			if replica.ReplicaID == currentLeader {
				transfer, ok := transferTarget(target, currentVoters, replica.ReplicaID, preferredTransfer)
				if !ok {
					return planUnsafef("group %q has no voter to receive leadership before demoting %d", target.ID, replica.ReplicaID)
				}
				emit(target.ID, StepTransferLeader, transfer.NodeName, transfer.ReplicaID)
				currentLeader = transfer.ReplicaID
			}
			emit(target.ID, StepDemoteVoter, replica.NodeName, replica.ReplicaID)
			delete(currentVoters, replica.ReplicaID)
		}
		for _, replica := range removals {
			if _, voter := currentVoters[replica.ReplicaID]; voter {
				if err := validateDestructiveBoundary(target.ID, len(currentVoters)); err != nil {
					return err
				}
				if replica.ReplicaID == currentLeader {
					transfer, ok := transferTarget(target, currentVoters, replica.ReplicaID, preferredTransfer)
					if !ok {
						return planUnsafef("group %q has no voter to receive leadership before removing %d", target.ID, replica.ReplicaID)
					}
					emit(target.ID, StepTransferLeader, transfer.NodeName, transfer.ReplicaID)
					currentLeader = transfer.ReplicaID
				}
				delete(currentVoters, replica.ReplicaID)
			}
			emit(target.ID, StepRemoveReplica, replica.NodeName, replica.ReplicaID)
			emit(target.ID, StepStopReplica, replica.NodeName, replica.ReplicaID)
			if target.ID != MetaGroupID {
				emit(target.ID, StepCleanupReplicaState, replica.NodeName, replica.ReplicaID)
			}
		}
		return nil
	}
	emitAdditions := func() error {
		for _, replica := range additions {
			_, learner := haveLearners[replica.ReplicaID]
			if !learner {
				emit(target.ID, StepPrepareReplica, replica.NodeName, replica.ReplicaID)
				emit(target.ID, StepAddLearner, replica.NodeName, replica.ReplicaID)
			}
			if replica.Role == ReplicaVoter {
				if len(currentVoters)+1 > hi {
					return planUnsafef("group %q would promote to %d voters, above max %d", target.ID, len(currentVoters)+1, hi)
				}
				emit(target.ID, StepWaitCaughtUp, replica.NodeName, replica.ReplicaID)
				emit(target.ID, StepPromoteVoter, replica.NodeName, replica.ReplicaID)
				currentVoters[replica.ReplicaID] = struct{}{}
			}
		}
		return nil
	}

	if shrinkFirst {
		if err := emitDestructive(); err != nil {
			return err
		}
		return emitAdditions()
	}
	if err := emitAdditions(); err != nil {
		return err
	}
	return emitDestructive()
}

func validateDestructiveBoundary(groupID string, before int) error {
	after := before - 1
	if after < 1 || after < QuorumOf(before) {
		return planUnsafef("group %q would reduce %d voters to %d, below the current quorum %d",
			groupID, before, after, QuorumOf(before))
	}
	return nil
}

func planGroupBounds(doc *Document, groupID string, rules Rules) (int, int) {
	if groupID == MetaGroupID {
		return 1, rules.Limits.MaxNodes
	}
	return doc.GroupBounds(groupID)
}

func validatePlannedSteps(active, desired Document, observed map[string]GroupStatus, plan Plan) error {
	seenGroups := make(map[string]bool)
	lastGroup := ""
	states := make(map[string]*validationGroup, len(observed))
	for id, status := range observed {
		states[id] = &validationGroup{
			found: status.Found, voters: idSet(status.ConfState.Voters), learners: idSet(status.ConfState.Learners),
			prepared: make(map[uint64]bool), waited: make(map[uint64]bool),
			removed: make(map[uint64]bool), stopped: make(map[uint64]bool),
		}
	}
	for i, step := range plan.Steps {
		if step.Seq != i+1 {
			return planUnsafef("step position %d has sequence %d", i+1, step.Seq)
		}
		if step.GroupID != lastGroup {
			if seenGroups[step.GroupID] || (lastGroup != "" && step.GroupID < lastGroup) {
				return planUnsafef("group %q appears out of deterministic order", step.GroupID)
			}
			seenGroups[step.GroupID] = true
			lastGroup = step.GroupID
		}
		state := states[step.GroupID]
		if state == nil {
			state = &validationGroup{voters: make(map[uint64]struct{}), learners: make(map[uint64]struct{}), prepared: make(map[uint64]bool), waited: make(map[uint64]bool), removed: make(map[uint64]bool), stopped: make(map[uint64]bool)}
			states[step.GroupID] = state
		}
		switch step.Kind {
		case StepCreateFoundingGroup:
			if state.found || active.Group(step.GroupID) != nil || desired.Group(step.GroupID) == nil {
				return planUnsafef("create founding group %q is not a fresh-group operation", step.GroupID)
			}
			state.found = true
			for _, replica := range desired.VotersOf(step.GroupID) {
				state.voters[replica.ReplicaID] = struct{}{}
			}
		case StepPrepareReplica:
			state.prepared[step.ReplicaID] = true
		case StepAddLearner:
			if !adjacentPlanStep(plan.Steps, i, StepPrepareReplica, step) || !state.prepared[step.ReplicaID] {
				return planUnsafef("add learner %s/%d has no prior prepare acknowledgement", step.GroupID, step.ReplicaID)
			}
			state.learners[step.ReplicaID] = struct{}{}
		case StepWaitCaughtUp:
			if _, ok := state.learners[step.ReplicaID]; !ok {
				return planUnsafef("wait %s/%d does not name a learner", step.GroupID, step.ReplicaID)
			}
			state.waited[step.ReplicaID] = true
		case StepPromoteVoter:
			if _, ok := state.learners[step.ReplicaID]; !ok || !state.waited[step.ReplicaID] ||
				!adjacentPlanStep(plan.Steps, i, StepWaitCaughtUp, step) {
				return planUnsafef("promotion %s/%d lacks learner presence and catch-up wait", step.GroupID, step.ReplicaID)
			}
			delete(state.learners, step.ReplicaID)
			state.voters[step.ReplicaID] = struct{}{}
		case StepDemoteVoter:
			if _, ok := state.voters[step.ReplicaID]; !ok {
				return planUnsafef("demotion %s/%d does not name a voter", step.GroupID, step.ReplicaID)
			}
			delete(state.voters, step.ReplicaID)
			state.learners[step.ReplicaID] = struct{}{}
		case StepRemoveReplica:
			if _, voter := state.voters[step.ReplicaID]; !voter {
				if _, learner := state.learners[step.ReplicaID]; !learner {
					return planUnsafef("remove %s/%d does not name a live member", step.GroupID, step.ReplicaID)
				}
			}
			delete(state.voters, step.ReplicaID)
			delete(state.learners, step.ReplicaID)
			state.removed[step.ReplicaID] = true
		case StepStopReplica:
			state.stopped[step.ReplicaID] = true
		case StepCleanupReplicaState:
			if step.GroupID == MetaGroupID || !state.removed[step.ReplicaID] || !state.stopped[step.ReplicaID] ||
				!adjacentPlanStep(plan.Steps, i, StepStopReplica, step) ||
				!adjacentPlanStep(plan.Steps, i-1, StepRemoveReplica, step) {
				return planUnsafef("cleanup %s/%d lacks exact removal and stop", step.GroupID, step.ReplicaID)
			}
		case StepDeleteGroupState:
			if previous := active.Group(step.GroupID); previous != nil {
				for _, replica := range previous.Replicas {
					if !state.stopped[replica.ReplicaID] {
						return planUnsafef("delete group %q lacks stop for replica %d", step.GroupID, replica.ReplicaID)
					}
				}
			}
		case StepTransferLeader:
			if _, ok := state.voters[step.ReplicaID]; !ok {
				return planUnsafef("leadership transfer %s/%d does not target a voter", step.GroupID, step.ReplicaID)
			}
		default:
			return planUnsafef("unknown step kind %q", step.Kind)
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
		for _, replica := range group.Replicas {
			if replica.ReplicaID == preferred && replica.Role == ReplicaVoter {
				if _, ok := voters[preferred]; ok {
					return replica, true
				}
			}
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

func unionGroupIDs(active, desired Document, observed map[string]GroupStatus) []string {
	seen := make(map[string]struct{}, len(active.Groups)+len(desired.Groups)+len(observed))
	ids := make([]string, 0, len(active.Groups)+len(desired.Groups)+len(observed))
	add := func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, group := range active.Groups {
		add(group.ID)
	}
	for _, group := range desired.Groups {
		add(group.ID)
	}
	for id := range observed {
		add(id)
	}
	sort.Strings(ids)
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
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
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
func BuildPlanPreferred(active, desired Document, observed map[string]GroupStatus, rules Rules, preferred uint64) (Plan, error) {
	return buildPlan(active, desired, observed, rules, preferred)
}
