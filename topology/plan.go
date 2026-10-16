package topology

import (
	"cmp"
	"fmt"
)

// Metadata admission establishes host ownership before load effects run.
func comparePlanGroups(a, b string) int {
	if a == b {
		return 0
	}
	if a == MetaGroupID {
		return -1
	}
	if b == MetaGroupID {
		return 1
	}
	return cmp.Compare(a, b)
}

// ValidatePlanShape checks immutable step ordering and exact identities against
// approved intent, proven declarations and separately retained host evidence.
// It also validates existing plans after effects complete; new effect ownership
// must be checked separately at approval against retained grants and cleanup.
func ValidatePlanShape(input PlanInput, desired Document, plan Plan) error {
	groups := unionGroupIDs(input, desired)
	if plan.Blocks != nil {
		groups = make([]string, 0, len(plan.Blocks))
		for _, block := range plan.Blocks {
			groups = append(groups, block.GroupID)
		}
	}
	return validatePlanShapeFor(input, desired, plan, groups)
}

// Scopes select deletion coverage, never identity evidence or original-plan steps.
func validatePlanShapeFor(input PlanInput, desired Document, plan Plan, groups []string) error {
	if err := validatePlanHeader(input, desired, plan); err != nil {
		return err
	}
	v := planShapeValidator{input: input, desired: desired, plan: plan,
		seenGroups: make(map[string]bool), stopped: make(map[string]map[uint64]bool)}
	for i, current := range plan.Steps {
		if err := v.validatePosition(i, current); err != nil {
			return err
		}
		if err := validateStepIdentity(current); err != nil {
			return err
		}
		if err := v.validateReplicaRole(current); err != nil {
			return err
		}
		if err := v.validateStepOrder(i, current); err != nil {
			return err
		}
	}

	if err := v.validateDeletionCoverage(groups); err != nil {
		return err
	}
	return validatePlanVoterProof(input, desired, plan)
}

func validatePlanHeader(input PlanInput, desired Document, plan Plan) error {
	if plan.Version != desired.Version {
		return fmt.Errorf("plan version %d does not match desired version %d", plan.Version, desired.Version)
	}
	if err := validateRetainedInput(input, desired); err != nil {
		return err
	}
	if err := validatePlanMembers(input, desired); err != nil {
		return err
	}
	// The current planner emits flat plans until its scoped-input conversion.
	// Remove this transitional guard when all producers supply block partitions.
	if len(plan.Blocks) != 0 || len(plan.Dependencies) != 0 {
		if err := validatePlanBlocks(plan); err != nil {
			return err
		}
		if err := validateBlockSafety(desired, plan); err != nil {
			return err
		}
	}
	return nil
}

func preparatoryWaitPair(steps []Step, index int, incoming Step) bool {
	i := index + 1
	if i < len(steps) && steps[i].Kind == StepTransferLeader && steps[i].GroupID == incoming.GroupID {
		i++
	}
	if i >= len(steps) || steps[i].GroupID != incoming.GroupID || steps[i].ReplicaID == incoming.ReplicaID {
		return false
	}
	departing := steps[i]
	switch departing.Kind {
	case StepDemoteVoter:
		i++
	case StepRemoveReplica:
		i++
		if i < len(steps) && steps[i].Kind == StepStopReplica && samePlanIdentity(steps[i], departing) {
			i++
			if i < len(steps) && steps[i].Kind == StepCleanupReplicaState && samePlanIdentity(steps[i], departing) {
				i++
			}
		}
	default:
		return false
	}
	return i < len(steps) && steps[i].Kind == StepWaitCaughtUp && samePlanIdentity(steps[i], incoming) &&
		nextPlanStepMatches(steps, i, StepPromoteVoter, incoming)
}

func samePlanIdentity(a, b Step) bool {
	return a.GroupID == b.GroupID && a.NodeName == b.NodeName && a.ReplicaID == b.ReplicaID
}

func exactPlanReplica(d Document, s Step) *Replica {
	if g := d.Group(s.GroupID); g != nil {
		for i := range g.Replicas {
			if g.Replicas[i].ReplicaID == s.ReplicaID && g.Replicas[i].NodeName == s.NodeName {
				return &g.Replicas[i]
			}
		}
	}
	return nil
}

func adjacentPlanStep(steps []Step, current int, kind StepKind, target Step) bool {
	if current < 1 || current > len(steps)-1 {
		return false
	}
	previous := steps[current-1]
	return previous.Kind == kind && previous.GroupID == target.GroupID && previous.NodeName == target.NodeName &&
		previous.ReplicaID == target.ReplicaID
}

func nextPlanStepMatches(steps []Step, current int, kind StepKind, target Step) bool {
	if current < 0 || current >= len(steps)-1 {
		return false
	}
	next := steps[current+1]
	return next.Kind == kind && next.GroupID == target.GroupID && next.NodeName == target.NodeName &&
		next.ReplicaID == target.ReplicaID
}

type planShapeValidator struct {
	input      PlanInput
	desired    Document
	plan       Plan
	seenGroups map[string]bool
	lastGroup  string
	stopped    map[string]map[uint64]bool
}

func (v *planShapeValidator) validatePosition(i int, current Step) error {
	if current.Seq != i+1 {
		return fmt.Errorf("step position %d has sequence %d", i+1, current.Seq)
	}
	if current.GroupID == "" {
		return fmt.Errorf("step %d has no group", current.Seq)
	}
	if current.GroupID != v.lastGroup {
		if v.seenGroups[current.GroupID] || (v.lastGroup != "" && comparePlanGroups(current.GroupID, v.lastGroup) < 0) {
			return fmt.Errorf("group %q appears out of deterministic order", current.GroupID)
		}
		v.seenGroups[current.GroupID] = true
		v.lastGroup = current.GroupID
	}
	return nil
}

func validateStepIdentity(current Step) error {
	wholeGroup := current.Kind == StepCreateFoundingGroup || current.Kind == StepDeleteGroupState
	if wholeGroup {
		if current.NodeName != "" || current.ReplicaID != 0 {
			return fmt.Errorf("whole-group step %d carries replica identity", current.Seq)
		}
	} else if current.NodeName == "" || current.ReplicaID == 0 {
		return fmt.Errorf("replica step %d lacks exact node/replica identity", current.Seq)
	}
	return nil
}

func (v *planShapeValidator) validateReplicaRole(current Step) error {
	old := v.input.provenReplica(current)
	next := exactPlanReplica(v.desired, current)
	if err := v.validateTransferGuard(current); err != nil {
		return err
	}
	switch current.Kind {
	case StepPrepareReplica,
		StepAddLearner,
		StepWaitCaughtUp,
		StepPromoteVoter:
		return validateDesiredStepReplica(current, next)
	case StepDemoteVoter:
		if old == nil || next == nil || next.Role != ReplicaLearner {
			return fmt.Errorf("step %d does not authorize a retained learner", current.Seq)
		}
	case StepRemoveReplica, StepStopReplica, StepCleanupReplicaState:
		if next != nil || !v.input.formerReplica(current) {
			return fmt.Errorf("step %d does not name a removed replica", current.Seq)
		}
	case StepTransferLeader:
		if (old == nil || old.Role != ReplicaVoter) && (next == nil || next.Role != ReplicaVoter) {
			return fmt.Errorf("step %d does not name an authorized voter", current.Seq)
		}
	case StepDeleteGroupState:
		return v.validateDeletedGroup(current)
	default: // Other step kinds need no replica-role checks.
	}
	return nil
}

func (v *planShapeValidator) validateTransferGuard(current Step) error {
	if current.TransferNodeName != "" || current.TransferReplicaID != 0 {
		guard := exactPlanReplica(v.desired, Step{GroupID: current.GroupID, NodeName: current.TransferNodeName,
			ReplicaID: current.TransferReplicaID})
		if current.Kind != StepDemoteVoter || guard == nil || guard.Role != ReplicaVoter || guard.PinnedLearner ||
			guard.ReplicaID == current.ReplicaID {
			return fmt.Errorf("step %d has an invalid exact demotion transfer guard", current.Seq)
		}
	}
	return nil
}

func (v *planShapeValidator) validateStepOrder(i int, current Step) error {
	switch current.Kind {
	case StepCreateFoundingGroup:
		if !v.input.foundingGroup(current.GroupID) || v.desired.Group(current.GroupID) == nil {
			return fmt.Errorf("create founding group %q is not fresh", current.GroupID)
		}
	case StepPrepareReplica:
		if !nextPlanStepMatches(v.plan.Steps, i, StepAddLearner, current) {
			return fmt.Errorf("prepare %s/%d is not immediately followed by add learner", current.GroupID,
				current.ReplicaID)
		}
		return requireCleanupOwnerStop(voterPlanTopology{base: v.input, desired: v.desired, groupID: current.GroupID},
			v.plan, current, v.stopped[current.GroupID])
	case StepWaitCaughtUp:
		if !nextPlanStepMatches(v.plan.Steps, i, StepPromoteVoter, current) &&
			!preparatoryWaitPair(v.plan.Steps, i, current) {
			return fmt.Errorf("wait %s/%d is not immediately followed by promotion", current.GroupID,
				current.ReplicaID)
		}
	case StepRemoveReplica:
		return v.validateRemovalOrder(i, current)
	case StepDemoteVoter, StepTransferLeader:
	case StepAddLearner:
		if !adjacentPlanStep(v.plan.Steps, i, StepPrepareReplica, current) {
			return fmt.Errorf("add learner %s/%d is not immediately preceded by prepare", current.GroupID,
				current.ReplicaID)
		}
	case StepPromoteVoter:
		if !adjacentPlanStep(v.plan.Steps, i, StepWaitCaughtUp, current) {
			return fmt.Errorf("promote voter %s/%d is not immediately preceded by wait", current.GroupID,
				current.ReplicaID)
		}
	case StepStopReplica:
		return v.validateStopOrder(i, current)
	case StepCleanupReplicaState:
		if current.GroupID == MetaGroupID || !adjacentPlanStep(v.plan.Steps, i, StepStopReplica, current) ||
			!adjacentPlanStep(v.plan.Steps, i-1, StepRemoveReplica, current) {
			return fmt.Errorf("cleanup %s/%d lacks adjacent remove and stop", current.GroupID, current.ReplicaID)
		}
	case StepDeleteGroupState:
		return v.validateDeletionOrder(current)
	default:
		return fmt.Errorf("step %d has unknown kind %q", current.Seq, current.Kind)
	}
	return nil
}

func (v *planShapeValidator) validateRemovalOrder(i int, current Step) error {
	stoppedNext := nextPlanStepMatches(v.plan.Steps, i, StepStopReplica, current)
	if !stoppedNext && !ReadmissionCovers(v.input, v.desired, v.plan, current.NodeName) {
		return fmt.Errorf("remove %s/%d is not immediately followed by stop", current.GroupID,
			current.ReplicaID)
	}
	if stoppedNext && current.GroupID != MetaGroupID &&
		!nextPlanStepMatches(v.plan.Steps, i+1, StepCleanupReplicaState, current) {
		return fmt.Errorf("remove %s/%d lacks ordered cleanup", current.GroupID, current.ReplicaID)
	}

	return nil
}

func (v *planShapeValidator) validateStopOrder(i int, current Step) error {
	key := ReplicaKey{NodeName: current.NodeName, ReplicaID: current.ReplicaID}
	progress := v.input.retirementProgress(current.GroupID, key)
	debtStop := (progress == RetainedCleanup || progress == RetainedStopped) &&
		requiredRetainedStop(v.desired.Group(current.GroupID), key)
	if !debtStop && v.desired.Group(current.GroupID) != nil &&
		!adjacentPlanStep(v.plan.Steps, i, StepRemoveReplica, current) {
		return fmt.Errorf("stop %s/%d lacks adjacent removal", current.GroupID, current.ReplicaID)
	}
	if v.stopped[current.GroupID] == nil {
		v.stopped[current.GroupID] = make(map[uint64]bool)
	}
	v.stopped[current.GroupID][current.ReplicaID] = true

	return nil
}

func (v *planShapeValidator) validateDeletionOrder(current Step) error {
	for _, replica := range v.input.formerReplicas(current.GroupID) {
		if replica.Progress != RetainedStopped && !v.stopped[current.GroupID][replica.ReplicaID] &&
			!ReadmissionCovers(v.input, v.desired, v.plan, replica.NodeName) {
			return fmt.Errorf("delete group %q lacks stop for replica %d", current.GroupID, replica.ReplicaID)
		}
	}

	return nil
}

func validateDesiredStepReplica(current Step, next *Replica) error {
	if next == nil {
		return fmt.Errorf("step %d does not name a desired replica", current.Seq)
	}
	if current.Kind == StepPromoteVoter && (next.Role != ReplicaVoter || next.PinnedLearner) {
		return fmt.Errorf("step %d promotes an intentional learner", current.Seq)
	}

	return nil
}

func (v *planShapeValidator) validateDeletedGroup(current Step) error {
	if current.GroupID == MetaGroupID || len(v.input.formerReplicas(current.GroupID)) == 0 ||
		v.desired.Group(current.GroupID) != nil {
		return fmt.Errorf("step %d does not name a deleted load group", current.Seq)
	}

	return nil
}
