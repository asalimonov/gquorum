package topology

import (
	"cmp"
	"slices"
)

// formerReplicas retains exact owners independently of current native presence.
// Explicit cleanup progress supersedes the older declaration's possible state.
func (in PlanInput) formerReplicas(groupID string) []RetainedReplica {
	owners := make(map[uint64]RetainedReplica)
	if group := in.provenGroup(groupID); group != nil {
		for _, replica := range group.Replicas {
			owners[replica.ReplicaID] = RetainedReplica{
				ReplicaKey: ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
				Progress:   RetainedHosted}
		}
	}
	for _, replica := range in.Retained[groupID].Replicas {
		owners[replica.ReplicaID] = replica
	}
	result := make([]RetainedReplica, 0, len(owners))
	for _, replica := range owners {
		result = append(result, replica)
	}
	slices.SortFunc(result, func(a, b RetainedReplica) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	return result
}

func (in PlanInput) retirementProgress(groupID string, key ReplicaKey) RetainedProgress {
	for _, replica := range in.Retained[groupID].Replicas {
		if replica.ReplicaKey == key {
			return replica.Progress
		}
	}
	replica := targetReplicaByID(in.provenGroup(groupID), key.ReplicaID)
	if replica != nil && replica.NodeName == key.NodeName {
		return RetainedHosted
	}
	return ""
}

func (in PlanInput) formerReplica(step Step) bool {
	return in.retirementProgress(step.GroupID, ReplicaKey{NodeName: step.NodeName, ReplicaID: step.ReplicaID}) != ""
}

func requiredRetainedStop(desired *Group, key ReplicaKey) bool {
	return desired == nil || slices.ContainsFunc(desired.Replicas, func(r Replica) bool {
		return r.NodeName == key.NodeName && r.ReplicaID != key.ReplicaID
	})
}

func (s *voterPlanState) retireHost(key ReplicaKey) {
	if s.base.retirementProgress(s.groupID, key) == RetainedStopped {
		return
	}
	s.emitRetirement(StepStopReplica, key)
	if s.groupID != MetaGroupID {
		s.emitRetirement(StepCleanupReplicaState, key)
	}
}

func (s *voterPlanState) emitRetirement(kind StepKind, key ReplicaKey) {
	s.steps = append(s.steps, Step{Kind: kind, GroupID: s.groupID, NodeName: key.NodeName, ReplicaID: key.ReplicaID})
}

func (s *voterPlanState) validateNativeRetirement() error {
	for _, replica := range s.base.Retained[s.groupID].Replicas {
		_, voter := s.voters[replica.ReplicaID]
		_, learner := s.learners[replica.ReplicaID]
		if replica.Progress == RetainedCleanup && (voter || learner) {
			return planUnsafef("group %q cleanup-owned replica %d remains native", s.groupID, replica.ReplicaID)
		}
	}
	return nil
}

// Existing debt retains retirement ownership; only its exact required stop can
// be carried by a successor plan before preparing another owner on that host.
func requireCleanupOwnerStop(view voterPlanTopology, plan Plan, incoming Step, stopped map[uint64]bool) error {
	for _, replica := range view.base.Retained[view.groupID].Replicas {
		if replica.Progress != RetainedCleanup || replica.NodeName != incoming.NodeName ||
			replica.ReplicaID == incoming.ReplicaID || stopped[replica.ReplicaID] {
			continue
		}
		if !hasReadmissionBoundary(view.base, view.desired, plan, replica.NodeName) {
			return planUnsafef("prepare %s/%d lacks required stop for former owner %d", incoming.GroupID,
				incoming.ReplicaID, replica.ReplicaID)
		}
	}
	return nil
}

func (v *planShapeValidator) validateDeletionCoverage() error {
	groups := make(map[string]bool)
	for id := range v.input.Groups {
		groups[id] = true
	}
	for id := range v.input.Retained {
		groups[id] = true
	}
	for id := range groups {
		if v.desired.Group(id) == nil {
			if err := v.validateDeletionOrder(Step{GroupID: id}); err != nil {
				return err
			}
			if err := v.validateDeletionRetirement(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *planShapeValidator) validateDeletionRetirement(group string) error {
	for _, owner := range v.input.formerReplicas(group) {
		if owner.Progress != RetainedHosted || hasReadmissionBoundary(v.input, v.desired, v.plan, owner.NodeName) {
			continue
		}
		retirement := slices.ContainsFunc(v.plan.Steps, func(s Step) bool {
			return s.GroupID == group && s.Kind == StepDeleteGroupState
		})
		if !retirement {
			return planUnsafef("plan omits cleanup of proven or retained group %q", group)
		}
		break
	}
	return nil
}
