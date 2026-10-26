package topology

func validateBlockSafety(desired Document, plan Plan) error {
	for _, block := range plan.Blocks {
		for _, seq := range block.Cleanup {
			step := plan.Steps[seq-1]
			if step.Kind != StepStopReplica {
				continue
			}
			if group := desired.Group(block.GroupID); group != nil && requiredRetainedStop(group,
				ReplicaKey{NodeName: step.NodeName, ReplicaID: step.ReplicaID}) {
				return invalid("same-host replacement requires its predecessor stop before convergence")
			}
		}
	}
	return validateFoundingBarriers(desired, plan)
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
			if groupReplicaOnNode(desired.Group(step.GroupID), node).Role != ReplicaVoter {
				return invalid("founding admission barrier names a learner instead of a founding voter")
			}
		}
	}
	return nil
}
