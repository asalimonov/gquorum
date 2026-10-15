package topology

func validateBlockSafety(desired Document, plan Plan) error {
	for _, block := range plan.Blocks {
		for _, seq := range block.Cleanup {
			step := plan.Steps[seq-1]
			if step.Kind != StepStopReplica {
				continue
			}
			if desired.Group(block.GroupID) == nil {
				return invalid("whole-group deletion requires every exact stop before convergence")
			}
			if replacesStoppedHost(plan, block, step) {
				return invalid("same-host replacement requires its predecessor stop before convergence")
			}
		}
	}
	return validateFoundingBarriers(desired, plan)
}

func replacesStoppedHost(plan Plan, block PlanBlock, stop Step) bool {
	for _, seq := range block.Steps {
		required := plan.Steps[seq-1]
		if required.NodeName == stop.NodeName && required.ReplicaID != stop.ReplicaID {
			return true
		}
	}
	return false
}

func validateFoundingBarriers(desired Document, plan Plan) error {
	for _, dependency := range plan.Dependencies {
		step := plan.Steps[dependency.StepSeq-1]
		if step.Kind != StepCreateFoundingGroup {
			continue
		}
		for _, seq := range dependency.Requires {
			node := plan.Steps[seq-1].NodeName
			if group := desired.Group(step.GroupID); group == nil || groupReplicaOnNode(group, node) == nil {
				return invalid("founding admission barrier names a host outside the founding group")
			}
		}
	}
	return nil
}
