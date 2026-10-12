package topology

import "fmt"

func ValidatePlanShape(active, desired Document, plan Plan) error {
	if plan.Version != desired.Version {
		return fmt.Errorf("plan version %d does not match desired version %d", plan.Version, desired.Version)
	}
	seenGroups := make(map[string]bool)
	lastGroup := ""
	stopped := make(map[string]map[uint64]bool)
	for i, current := range plan.Steps {
		if current.Seq != i+1 {
			return fmt.Errorf("step position %d has sequence %d", i+1, current.Seq)
		}
		if current.GroupID == "" {
			return fmt.Errorf("step %d has no group", current.Seq)
		}
		if current.GroupID != lastGroup {
			if seenGroups[current.GroupID] || (lastGroup != "" && current.GroupID < lastGroup) {
				return fmt.Errorf("group %q appears out of deterministic order", current.GroupID)
			}
			seenGroups[current.GroupID] = true
			lastGroup = current.GroupID
		}
		wholeGroup := current.Kind == StepCreateFoundingGroup || current.Kind == StepDeleteGroupState
		if wholeGroup {
			if current.NodeName != "" || current.ReplicaID != 0 {
				return fmt.Errorf("whole-group step %d carries replica identity", current.Seq)
			}
		} else if current.NodeName == "" || current.ReplicaID == 0 {
			return fmt.Errorf("replica step %d lacks exact node/replica identity", current.Seq)
		}
		old := exactPlanReplica(active, current)
		next := exactPlanReplica(desired, current)
		if current.TransferNodeName != "" || current.TransferReplicaID != 0 {
			guard := exactPlanReplica(desired, Step{GroupID: current.GroupID, NodeName: current.TransferNodeName, ReplicaID: current.TransferReplicaID})
			if current.Kind != StepDemoteVoter || guard == nil || guard.Role != ReplicaVoter || guard.PinnedLearner || guard.ReplicaID == current.ReplicaID {
				return fmt.Errorf("step %d has an invalid exact demotion transfer guard", current.Seq)
			}
		}
		switch current.Kind {
		case StepPrepareReplica, StepAddLearner, StepWaitCaughtUp, StepPromoteVoter:
			if next == nil {
				return fmt.Errorf("step %d does not name a desired replica", current.Seq)
			}
			if current.Kind == StepPromoteVoter && (next.Role != ReplicaVoter || next.PinnedLearner) {
				return fmt.Errorf("step %d promotes an intentional learner", current.Seq)
			}
		case StepDemoteVoter:
			if old == nil || next == nil || next.Role != ReplicaLearner {
				return fmt.Errorf("step %d does not authorize a retained learner", current.Seq)
			}
		case StepRemoveReplica, StepStopReplica, StepCleanupReplicaState:
			if old == nil || next != nil {
				return fmt.Errorf("step %d does not name a removed replica", current.Seq)
			}
		case StepTransferLeader:
			if (old == nil || old.Role != ReplicaVoter) && (next == nil || next.Role != ReplicaVoter) {
				return fmt.Errorf("step %d does not name an authorized voter", current.Seq)
			}
		case StepDeleteGroupState:
			if current.GroupID == MetaGroupID || active.Group(current.GroupID) == nil || desired.Group(current.GroupID) != nil {
				return fmt.Errorf("step %d does not name a deleted load group", current.Seq)
			}
		}
		switch current.Kind {
		case StepCreateFoundingGroup:
			if active.Group(current.GroupID) != nil || desired.Group(current.GroupID) == nil {
				return fmt.Errorf("create founding group %q is not fresh", current.GroupID)
			}
		case StepPrepareReplica:
			if !nextPlanStepMatches(plan.Steps, i, StepAddLearner, current) {
				return fmt.Errorf("prepare %s/%d is not immediately followed by add learner", current.GroupID, current.ReplicaID)
			}
		case StepWaitCaughtUp:
			if !nextPlanStepMatches(plan.Steps, i, StepPromoteVoter, current) {
				return fmt.Errorf("wait %s/%d is not immediately followed by promotion", current.GroupID, current.ReplicaID)
			}
		case StepRemoveReplica:
			if !nextPlanStepMatches(plan.Steps, i, StepStopReplica, current) {
				return fmt.Errorf("remove %s/%d is not immediately followed by stop", current.GroupID, current.ReplicaID)
			}
		case StepDemoteVoter, StepTransferLeader:
		case StepAddLearner:
			if !adjacentPlanStep(plan.Steps, i, StepPrepareReplica, current) {
				return fmt.Errorf("add learner %s/%d is not immediately preceded by prepare", current.GroupID, current.ReplicaID)
			}
		case StepPromoteVoter:
			if !adjacentPlanStep(plan.Steps, i, StepWaitCaughtUp, current) {
				return fmt.Errorf("promote voter %s/%d is not immediately preceded by wait", current.GroupID, current.ReplicaID)
			}
		case StepStopReplica:
			if stopped[current.GroupID] == nil {
				stopped[current.GroupID] = make(map[uint64]bool)
			}
			stopped[current.GroupID][current.ReplicaID] = true
		case StepCleanupReplicaState:
			if current.GroupID == MetaGroupID ||
				!adjacentPlanStep(plan.Steps, i, StepStopReplica, current) ||
				!adjacentPlanStep(plan.Steps, i-1, StepRemoveReplica, current) {
				return fmt.Errorf("cleanup %s/%d lacks adjacent remove and stop", current.GroupID, current.ReplicaID)
			}
		case StepDeleteGroupState:
			if previous := active.Group(current.GroupID); previous != nil {
				for _, replica := range previous.Replicas {
					if !stopped[current.GroupID][replica.ReplicaID] {
						return fmt.Errorf("delete group %q lacks stop for replica %d", current.GroupID, replica.ReplicaID)
					}
				}
			}
		default:
			return fmt.Errorf("step %d has unknown kind %q", current.Seq, current.Kind)
		}
	}
	return nil
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
	return previous.Kind == kind && previous.GroupID == target.GroupID &&
		previous.NodeName == target.NodeName && previous.ReplicaID == target.ReplicaID
}

func nextPlanStepMatches(steps []Step, current int, kind StepKind, target Step) bool {
	if current < 0 || current >= len(steps)-1 {
		return false
	}
	next := steps[current+1]
	return next.Kind == kind && next.GroupID == target.GroupID &&
		next.NodeName == target.NodeName && next.ReplicaID == target.ReplicaID
}
