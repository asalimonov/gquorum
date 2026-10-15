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
	Retained map[string]RetainedGroup
}

func (in PlanInput) provenGroup(id string) *Group { return in.Groups[id].Converged }

func (in PlanInput) provenNode(name string) *Node { return in.Nodes[name].Converged }

func (in PlanInput) provenReplica(step Step) *Replica {
	replica := targetReplicaByID(in.provenGroup(step.GroupID), step.ReplicaID)
	if replica == nil || replica.NodeName != step.NodeName {
		return nil
	}
	return replica
}

func (in PlanInput) foundingGroup(id string) bool {
	row := in.Groups[id]
	retained := in.Retained[id]
	return row.Converged == nil && row.ConvergedVersion == 0 &&
		!retained.FoundingIssued && len(retained.Replicas) == 0
}

// Load ownership follows the node checkpoint until every affected load group
// converges; newer metadata membership cannot erase that predecessor.
func (in PlanInput) loadOwnerMember(name string) (uint64, bool) {
	node := in.Nodes[name]
	if node.ConvergedVersion != 0 && node.MemberID != 0 {
		return node.MemberID, true
	}
	if member := in.convergedMetaMember(name); member != 0 {
		return member, true
	}
	return 0, node.ConvergedVersion != 0
}

func (in PlanInput) convergedMetaMember(name string) uint64 {
	meta := in.Groups[MetaGroupID]
	if meta.ConvergedVersion != 0 && meta.Converged != nil {
		for _, replica := range meta.Converged.Replicas {
			if replica.NodeName == name {
				return replica.ReplicaID
			}
		}
	}
	return 0
}
