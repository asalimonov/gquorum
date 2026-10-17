package topology

import "slices"

func validatePlannedSteps(input PlanInput, desired Document, observed map[string]GroupStatus, plan Plan) error {
	v := plannedStepValidator{input: input, desired: desired, plan: plan}
	return v.validate(observed, unionGroupIDs(input, desired))
}

func convergedPlanInput(doc Document) PlanInput {
	in := PlanInput{Approved: doc, Groups: make(map[string]GroupProgress, len(doc.Groups)),
		Nodes: make(map[string]NodeProgress, len(doc.Nodes))}
	for i := range doc.Groups {
		group := &doc.Groups[i]
		in.Groups[group.ID] = GroupProgress{GroupID: group.ID, ConvergedVersion: doc.Version, Converged: group}
	}
	for i := range doc.Nodes {
		node := &doc.Nodes[i]
		in.Nodes[node.Name] = NodeProgress{NodeName: node.Name, ConvergedVersion: doc.Version,
			Converged: node, MemberID: doc.MemberID(node.Name)}
	}
	return in
}

// These pure fixtures explicitly model the supplied statuses as confirmed native reads.
func testPlanInput(base Document, observed map[string]GroupStatus) PlanInput {
	input := convergedPlanInput(base)
	input.Native = make(map[string]NativeState)
	for id, status := range observed {
		row := input.Groups[id]
		owned := status.Clone()
		row.GroupID, row.Observation = id, &owned
		input.Groups[id] = row
		if status.Found {
			reporter := uint64(1)
			if len(status.ConfState.Voters) != 0 {
				reporter = status.ConfState.Voters[0]
			}
			input.Native[id] = NativeState{GroupID: id, ReplicaID: reporter, Applied: 1,
				ConfState: status.Clone().ConfState}
		}
	}
	return input
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
