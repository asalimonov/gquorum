package operator

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateRemainingPlan simulates the immutable suffix against confirmed applied
// membership. It never manufactures an effect receipt or changes the plan.
// Base includes retained host identities still requiring cleanup.
func ValidateRemainingPlan(base, target Document, ex Record, native map[string]topology.NativeState, rules Rules) error {
	replan := func(format string, args ...any) error {
		return fault.New(fault.ReplanRequired, fmt.Sprintf(format, args...), nil)
	}
	first := max(ex.CurrentStep, 1)
	if first > len(ex.Steps) && len(ex.Steps) != 0 {
		return ErrOperatorAuthorization
	}
	// Host resources can exist without native membership. Check the full approved
	// plan: earlier steps are durable progress, and remaining steps must cover all
	// other retained identities even when no member is currently observable.
	has := func(kind StepKind, group, node string, id uint64) bool {
		return slices.ContainsFunc(ex.Steps, func(s Step) bool {
			return s.Kind == kind && s.GroupID == group && (node == "" || s.NodeName == node && s.ReplicaID == id)
		})
	}
	for _, group := range base.Groups {
		desired := target.Group(group.ID)
		retiredByReadmission := true
		for _, r := range group.Replicas {
			retiredByReadmission = retiredByReadmission && topology.ReadmissionCovers(base, target, Plan{Version: ex.Version, Steps: ex.Steps}, r.NodeName)
		}
		if desired == nil && !retiredByReadmission && !has(StepDeleteGroupState, group.ID, "", 0) {
			return replan("plan omits deletion of retained group %q", group.ID)
		}
		for _, replica := range group.Replicas {
			if next := documentReplicaByID(target, group.ID, replica.ReplicaID); next != nil && next.NodeName == replica.NodeName {
				continue
			}
			if topology.ReadmissionCovers(base, target, Plan{Version: ex.Version, Steps: ex.Steps}, replica.NodeName) {
				continue
			}
			if !has(StepStopReplica, group.ID, replica.NodeName, replica.ReplicaID) {
				return replan("plan omits stop for retained replica %s/%s/%d", group.ID, replica.NodeName, replica.ReplicaID)
			}
			if group.ID != MetaGroupID && desired != nil && !has(StepCleanupReplicaState, group.ID, replica.NodeName, replica.ReplicaID) {
				return replan("plan omits cleanup for retained replica %s/%s/%d", group.ID, replica.NodeName, replica.ReplicaID)
			}
		}
	}
	for _, group := range target.Groups {
		_, maxVoters := target.GroupBounds(group.ID)
		if group.ID == MetaGroupID {
			maxVoters = rules.Limits.MaxNodes
		}
		if p := group.VoterPolicy; p != nil && p.TargetVoters < maxVoters {
			maxVoters = p.TargetVoters + p.MaxSurgeVoters
		}
		state, exists := native[group.ID]
		if exists && (state.GroupID != group.ID || state.Applied == 0 || state.ReplicaID == 0) {
			return fault.ErrUnreadable
		}
		cs := state.ConfState
		if cs.AutoLeave || len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 {
			return fault.ErrAuthorityPending
		}
		voters, learners := map[uint64]bool{}, map[uint64]bool{}
		for _, id := range cs.Voters {
			voters[id] = true
		}
		for _, id := range cs.Learners {
			learners[id] = true
		}
		for _, step := range ex.Steps {
			if step.Seq < first || step.GroupID != group.ID {
				continue
			}
			present := voters[step.ReplicaID] || learners[step.ReplicaID]
			switch step.Kind {
			case StepCreateFoundingGroup:
				if exists {
					if !foundingMatchesTarget(&group, GroupStatus{Found: true, ConfState: cs}) {
						return replan("group %q no longer matches its immutable founding step", group.ID)
					}
				} else {
					for _, r := range group.Replicas {
						if r.Role == topology.ReplicaVoter {
							voters[r.ReplicaID] = true
						}
					}
					exists = true
				}
			case StepAddLearner:
				if !voters[step.ReplicaID] {
					learners[step.ReplicaID] = true
				}
			case StepWaitCaughtUp, StepPromoteVoter, StepDemoteVoter, StepTransferLeader:
				if !present {
					return replan("step %d requires missing member %s/%d", step.Seq, group.ID, step.ReplicaID)
				}
				if step.Kind == StepTransferLeader && !voters[step.ReplicaID] {
					return replan("step %d transfers to a nonvoter", step.Seq)
				}
				if step.Kind == StepPromoteVoter {
					if !voters[step.ReplicaID] && len(voters)+1 > maxVoters {
						return replan("step %d would exceed group %q voter capacity", step.Seq, group.ID)
					}
					voters[step.ReplicaID] = true
					delete(learners, step.ReplicaID)
				}
				if step.Kind == StepDemoteVoter {
					if voters[step.ReplicaID] && step.TransferReplicaID != 0 && !voters[step.TransferReplicaID] {
						return replan("step %d lost its voter transfer prerequisite", step.Seq)
					}
					if voters[step.ReplicaID] && len(voters)-1 < topology.QuorumOf(len(voters)) {
						return replan("step %d cannot safely demote the observed voter set", step.Seq)
					}
					delete(voters, step.ReplicaID)
					learners[step.ReplicaID] = true
				}
			case StepRemoveReplica:
				if voters[step.ReplicaID] && len(voters)-1 < topology.QuorumOf(len(voters)) {
					return replan("step %d cannot safely remove from the observed voter set", step.Seq)
				}
				delete(voters, step.ReplicaID)
				delete(learners, step.ReplicaID)
			case StepStopReplica, StepCleanupReplicaState:
				if present {
					return replan("step %d would retire a replica still in native membership", step.Seq)
				}
			}
		}
		if !exists {
			return fault.ErrUnavailable
		}
		if len(voters)+len(learners) != len(group.Replicas) {
			return replan("remaining plan cannot reach membership of group %q", group.ID)
		}
		for _, r := range group.Replicas {
			if r.Role == topology.ReplicaVoter && !voters[r.ReplicaID] || r.Role == topology.ReplicaLearner && !learners[r.ReplicaID] {
				return replan("remaining plan cannot reach role of replica %s/%d", group.ID, r.ReplicaID)
			}
		}
	}
	return nil
}
