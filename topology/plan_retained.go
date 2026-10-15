package topology

// RetainedGroup describes possible effects beyond the current declarations.
// FoundingIssued survives filtering Replicas against the desired assignments.
type RetainedGroup struct {
	FoundingIssued bool              `json:"founding_issued"`
	Replicas       []RetainedReplica `json:"replicas,omitempty"`
}

// RetainedReplica names an omitted exact owner and its cleanup progress. It
// supplies neither a replica role nor authority to perform another effect.
type RetainedReplica struct {
	ReplicaKey
	Progress RetainedProgress `json:"progress"`
}

// RetainedProgress separates possible host state from debt and proven stop.
type RetainedProgress string

// Retained progress describes host obligations, independently of native membership.
const (
	RetainedHosted  RetainedProgress = "hosted"  // Possible host state without cleanup debt.
	RetainedCleanup RetainedProgress = "cleanup" // Debt owns retirement; stop is unproven.
	RetainedStopped RetainedProgress = "stopped" // Stop proven; application retirement may remain.
)

func validateRetainedInput(input PlanInput, desired Document) error {
	for groupID, group := range input.Retained {
		if groupID == "" {
			return planUnsafef("retained group has no identity")
		}
		seen := make(map[uint64]bool, len(group.Replicas))
		for _, replica := range group.Replicas {
			if replica.ReplicaID == 0 || replica.NodeName == "" || seen[replica.ReplicaID] {
				return planUnsafef("group %q has zero, unnamed or duplicate retained identity %d",
					groupID, replica.ReplicaID)
			}
			seen[replica.ReplicaID] = true
			switch replica.Progress {
			case RetainedHosted, RetainedCleanup, RetainedStopped:
			default:
				return planUnsafef("group %q has unknown retained progress %q", groupID, replica.Progress)
			}
			if err := checkRetainedIdentity(input, desired, groupID, replica.ReplicaKey); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkRetainedIdentity(input PlanInput, desired Document, groupID string, key ReplicaKey) error {
	approved, target := input.Approved.Group(groupID), desired.Group(groupID)
	if targetReplicaByID(target, key.ReplicaID) != nil {
		return planUnsafef("group %q retained replica %d overlaps desired membership", groupID, key.ReplicaID)
	}
	for _, group := range []*Group{approved, input.provenGroup(groupID)} {
		if replica := targetReplicaByID(group, key.ReplicaID); replica != nil && replica.NodeName != key.NodeName {
			return planUnsafef("group %q retained replica %d changes host binding", groupID, key.ReplicaID)
		}
	}
	// An older converged counter cannot bound identities from later approvals.
	// Deleted groups may have neither current declaration: the FSM then checks
	// the full retained allocation history, without fabricating a declaration.
	var ceiling uint64
	for _, group := range []*Group{approved, target} {
		if group != nil {
			ceiling = max(ceiling, group.NextReplicaID)
		}
	}
	if ceiling != 0 && key.ReplicaID >= ceiling {
		return planUnsafef("group %q retained replica %d exceeds available allocation bound", groupID, key.ReplicaID)
	}
	return nil
}

func (in PlanInput) planReplicaKey(groupID string, target *Group, id uint64) (ReplicaKey, bool) {
	if replica, ok := replicaIdentity(in.provenGroup(groupID), target, id); ok {
		return ReplicaKey{NodeName: replica.NodeName, ReplicaID: id}, true
	}
	for _, replica := range in.Retained[groupID].Replicas {
		if replica.ReplicaID == id {
			return replica.ReplicaKey, true
		}
	}
	return ReplicaKey{}, false
}
