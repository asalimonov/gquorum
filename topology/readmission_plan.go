package topology

import "slices"

// ReadmissionCovers reports whether the plan's exact replacement metadata
// preparation is the retirement boundary for this former host. The metadata FSM
// must require all readmission receipts before granting that preparation.
func ReadmissionCovers(active, desired Document, plan Plan, node string) bool {
	member := desired.MemberID(node)
	if active.Version == 0 || member == 0 || member <= active.MemberID(node) {
		return false
	}
	replica := exactPlanReplica(desired, Step{GroupID: MetaGroupID, NodeName: node, ReplicaID: member})
	return replica != nil && ValidAdmissionSequence(plan.Steps, node, member, replica.Role)
}

// bindReadmissionRetirement constructs the final plan before approval. A fresh
// metadata owner retires its predecessor through the independently granted
// readmission protocol before PrepareReplica can be consumed. Later host calls
// to the retired member would be both redundant and unroutable.
func bindReadmissionRetirement(active, desired Document, plan Plan) Plan {
	if active.Version == 0 {
		return plan
	}
	meta := desired.MetaGroup()
	if meta == nil {
		return plan
	}
	var replacements []Replica
	for _, r := range sortedReplicas(meta.Replicas) {
		if r.ReplicaID > active.MemberID(r.NodeName) {
			replacements = append(replacements, r)
		}
	}
	if len(replacements) == 0 {
		return plan
	}
	replacement := func(node string) *Replica {
		for i := range replacements {
			if replacements[i].NodeName == node {
				return &replacements[i]
			}
		}
		return nil
	}
	var steps []Step
	emitted := make(map[string]bool)
	emit := func(r Replica) {
		if emitted[r.NodeName] {
			return
		}
		emitted[r.NodeName] = true
		kinds := []StepKind{StepPrepareReplica, StepAddLearner}
		if r.Role == ReplicaVoter {
			kinds = append(kinds, StepWaitCaughtUp, StepPromoteVoter)
		}
		for _, kind := range kinds {
			steps = append(steps, Step{GroupID: MetaGroupID, NodeName: r.NodeName, ReplicaID: r.ReplicaID, Kind: kind})
		}
	}
	flush := func() {
		for _, r := range replacements {
			emit(r)
		}
	}
	for _, step := range plan.Steps {
		if step.GroupID != MetaGroupID {
			flush()
		}
		r := replacement(step.NodeName)
		if step.GroupID == MetaGroupID && r != nil && step.ReplicaID == r.ReplicaID {
			emit(*r)
			if slices.Contains([]StepKind{StepPrepareReplica, StepAddLearner, StepWaitCaughtUp, StepPromoteVoter}, step.Kind) {
				continue
			}
		}
		if r != nil && (step.Kind == StepStopReplica || step.Kind == StepCleanupReplicaState) && exactPlanReplica(desired, step) == nil {
			continue
		}
		if step.Kind == StepDeleteGroupState {
			old := active.Group(step.GroupID)
			if old != nil && !slices.ContainsFunc(old.Replicas, func(r Replica) bool { return replacement(r.NodeName) == nil }) {
				continue
			}
		}
		steps = append(steps, step)
	}
	flush()
	for i := range steps {
		steps[i].Seq = i + 1
	}
	plan.Steps = steps
	return plan
}
