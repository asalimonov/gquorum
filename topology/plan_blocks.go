package topology

import "fmt"

// validatePlanBlocks checks immutable partitions and the restricted barrier graph.
// Receipt consumption and current evacuation state are checked separately by the FSM.
func validatePlanBlocks(plan Plan) error {
	required := make(map[int]bool, len(plan.Steps))
	for i, block := range plan.Blocks {
		if block.SourceVersion > plan.Version || ValidateID(block.GroupID) != nil ||
			i > 0 && comparePlanGroups(plan.Blocks[i-1].GroupID, block.GroupID) >= 0 {
			return invalid("plan blocks require sorted unique groups and a valid source version")
		}
		if err := indexBlockSteps(plan, block, required); err != nil {
			return err
		}
	}
	if len(required) != len(plan.Steps) {
		return invalid("every step must belong to exactly one execution or cleanup block")
	}
	if err := validatePlanDependencies(plan, required); err != nil {
		return err
	}
	return validateBarrierOrder(plan, required)
}

func indexBlockSteps(plan Plan, block PlanBlock, required map[int]bool) error {
	for _, sequences := range [][]int{block.Steps, block.Cleanup} {
		for i, seq := range sequences {
			if seq < 1 || seq > len(plan.Steps) || i > 0 && sequences[i-1] >= seq {
				return invalid("block sequences must be ordered unique references to plan steps")
			}
			step := plan.Steps[seq-1]
			if _, exists := required[seq]; exists || step.Seq != seq || step.GroupID != block.GroupID {
				return invalid("block sequence has a duplicate owner or mismatched step identity")
			}
			required[seq] = true
		}
	}
	for _, seq := range block.Cleanup {
		kind := plan.Steps[seq-1].Kind
		if kind != StepStopReplica && kind != StepCleanupReplicaState && kind != StepDeleteGroupState {
			return invalid("cleanup cannot authorize preparation or native membership effects")
		}
		required[seq] = false
	}
	return nil
}

func validatePlanDependencies(plan Plan, required map[int]bool) error {
	for i, dependency := range plan.Dependencies {
		if !required[dependency.StepSeq] || len(dependency.Requires) == 0 ||
			i > 0 && plan.Dependencies[i-1].StepSeq >= dependency.StepSeq {
			return invalid("barriers require ordered unique execution steps and nonempty prerequisites")
		}
		step := plan.Steps[dependency.StepSeq-1]
		for j, seq := range dependency.Requires {
			if !required[seq] || j > 0 && dependency.Requires[j-1] >= seq {
				return invalid("barrier prerequisites must be ordered unique required steps")
			}
			if !validBarrierShape(step, plan.Steps[seq-1]) {
				return fmt.Errorf("step %d has an unsupported admission/removal barrier on %d", step.Seq, seq)
			}
		}
	}
	return nil
}

func validBarrierShape(step, prerequisite Step) bool {
	if prerequisite.GroupID == MetaGroupID && step.GroupID != MetaGroupID &&
		prerequisite.Kind == StepPrepareReplica {
		return step.Kind == StepCreateFoundingGroup ||
			step.Kind == StepPrepareReplica && step.NodeName == prerequisite.NodeName
	}
	if step.GroupID != MetaGroupID || prerequisite.GroupID == MetaGroupID || step.Kind != StepRemoveReplica {
		return false
	}
	return step.NodeName == prerequisite.NodeName &&
		(prerequisite.Kind == StepRemoveReplica || prerequisite.Kind == StepStopReplica)
}

func validateBarrierOrder(plan Plan, required map[int]bool) error {
	predecessors := make(map[int]int, len(required))
	followers := make(map[int][]int, len(required))
	link := func(before, after int) {
		predecessors[after]++
		followers[before] = append(followers[before], after)
	}
	for _, block := range plan.Blocks {
		for i := 1; i < len(block.Steps); i++ {
			link(block.Steps[i-1], block.Steps[i])
		}
	}
	for _, dependency := range plan.Dependencies {
		for _, seq := range dependency.Requires {
			link(seq, dependency.StepSeq)
		}
	}
	return drainBarrierGraph(required, predecessors, followers)
}

func drainBarrierGraph(required map[int]bool, predecessors map[int]int, followers map[int][]int) error {
	ready := make([]int, 0, len(required))
	want := 0
	for seq, execution := range required {
		if execution {
			want++
			if predecessors[seq] == 0 {
				ready = append(ready, seq)
			}
		}
	}
	for i := 0; i < len(ready); i++ {
		for _, seq := range followers[ready[i]] {
			predecessors[seq]--
			if predecessors[seq] == 0 {
				ready = append(ready, seq)
			}
		}
	}
	if len(ready) != want {
		return invalid("barriers and within-block order contain a cycle")
	}
	return nil
}
