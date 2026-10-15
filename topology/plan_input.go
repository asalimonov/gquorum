package topology

// PlanInput separates approved intent, proven declarations and native membership.
// A missing converged declaration is unknown initially or a completed deletion
// when ConvergedVersion is nonzero; Approved alone does not establish a replica.
// Callers retain ownership and must not mutate inputs while planning.
type PlanInput struct {
	Approved Document
	Groups   map[string]GroupProgress
	Nodes    map[string]NodeProgress
	Native   map[string]NativeState
}

func (in PlanInput) provenGroup(id string) *Group { return in.Groups[id].Converged }

func (in PlanInput) provenNode(name string) *Node { return in.Nodes[name].Converged }

// Transitional adapter for existing whole-document entry points. Remove planner
// callers in Task 18 and remaining legacy API adapters at cutover Task 62.
// It decomposes an actual caller-supplied base; it never synthesizes a Document.
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
