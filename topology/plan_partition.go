package topology

import "slices"

// An obsolete same-host prerequisite becomes an exact deferred action in the
// successor. Already-detached debt keeps its existing approval and emits nothing.
func bindDetachedStops(input PlanInput, desired Document, plan *Plan, groups []string) {
	steps := make(map[string][]Step, len(groups))
	for _, step := range plan.Steps {
		steps[step.GroupID] = append(steps[step.GroupID], step)
	}
	plan.Steps = nil
	for _, id := range groups {
		plan.Steps = append(plan.Steps, steps[id]...)
		for _, replica := range input.formerReplicas(id) {
			if replica.Progress == RetainedCleanup &&
				requiredRetainedStop(input.Approved.Group(id), replica.ReplicaKey) &&
				!requiredRetainedStop(desired.Group(id), replica.ReplicaKey) {
				plan.Steps = append(plan.Steps, Step{Kind: StepStopReplica, GroupID: id,
					NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
			}
		}
	}
}

// Partition after all step rewrites: global sequences, guards and exact actions
// remain immutable. The flat order retains adjacent safety pairs; a scoped
// executor visits only required references, and cleanup uses retained actions.
func bindPlanBlocks(input PlanInput, desired Document, plan *Plan, groups []string) {
	plan.Blocks, plan.Dependencies = nil, nil
	positions := make(map[string]int, len(groups))
	for _, id := range groups {
		positions[id] = len(plan.Blocks)
		plan.Blocks = append(plan.Blocks, PlanBlock{GroupID: id, SourceVersion: input.Groups[id].ConvergedVersion})
	}
	for _, step := range plan.Steps {
		i, found := positions[step.GroupID]
		if !found {
			continue // Shape validation refuses the uncovered step.
		}
		block := &plan.Blocks[i]
		if detachedPlanStep(desired, step) {
			block.Cleanup = append(block.Cleanup, step.Seq)
		} else {
			block.Steps = append(block.Steps, step.Seq)
		}
	}
	bindPlanBarriers(desired, plan)
}

func detachedPlanStep(desired Document, step Step) bool {
	switch step.Kind {
	case StepCleanupReplicaState, StepDeleteGroupState:
		return true
	case StepStopReplica:
		return !requiredRetainedStop(desired.Group(step.GroupID),
			ReplicaKey{NodeName: step.NodeName, ReplicaID: step.ReplicaID})
	default:
		return false
	}
}

// FI_ADMISSION_STATE: edges explain same-plan ordering only. Every preparation
// and founding voter still requires the FSM's exact admission predicate even
// when the proof comes from a different operation and no edge can be emitted.
func bindPlanBarriers(desired Document, plan *Plan) {
	preparations, evacuations := planBarrierInputs(desired, plan.Steps)
	for _, step := range plan.Steps {
		var requires []int
		if step.GroupID == MetaGroupID {
			// A replacement member uses readmission retirement, not evacuation
			// of its newly admitted successor on the same node.
			if step.Kind == StepRemoveReplica && desired.MemberID(step.NodeName) == 0 {
				requires = evacuations[step.NodeName]
			}
		} else {
			requires = admissionPrerequisites(desired, step, preparations)
		}
		if len(requires) != 0 {
			plan.Dependencies = append(plan.Dependencies, StepDependency{StepSeq: step.Seq, Requires: requires})
		}
	}
}

func admissionPrerequisites(desired Document, step Step, preparations map[string]int) []int {
	if step.Kind == StepPrepareReplica {
		if seq := preparations[step.NodeName]; seq != 0 {
			return []int{seq}
		}
	}
	if step.Kind != StepCreateFoundingGroup || desired.Group(step.GroupID) == nil {
		return nil
	}
	var requires []int
	for _, founder := range desired.Group(step.GroupID).Replicas {
		if seq := preparations[founder.NodeName]; founder.Role == ReplicaVoter && seq != 0 {
			requires = append(requires, seq)
		}
	}
	slices.Sort(requires)
	return requires
}

func planBarrierInputs(desired Document, steps []Step) (map[string]int, map[string][]int) {
	preparations := make(map[string]int)
	evacuations := make(map[string][]int)
	for _, step := range steps {
		if step.GroupID == MetaGroupID && step.Kind == StepPrepareReplica &&
			desired.MemberID(step.NodeName) == step.ReplicaID {
			preparations[step.NodeName] = step.Seq
		}
		if step.GroupID != MetaGroupID && (step.Kind == StepRemoveReplica || step.Kind == StepStopReplica) &&
			!detachedPlanStep(desired, step) {
			evacuations[step.NodeName] = append(evacuations[step.NodeName], step.Seq)
		}
	}
	return preparations, evacuations
}
