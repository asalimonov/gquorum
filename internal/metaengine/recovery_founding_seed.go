package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// recoveryFoundingSeeds proves only the initial B singleton's voter basis after
// a retry replaced its birth grant with a real, recorded native fence. The new
// attempt still owes its own complete effect receipt; nothing is consumed here.
func (s *scopedState) recoveryFoundingSeeds(read PlanRead) []string {
	if s.Recovery == nil || s.Recovery.Phase != "importing" || read.Operation.Status.Version != s.Recovery.Version {
		return nil
	}
	var result []string
	for _, group := range s.Recovery.Groups {
		native, found := read.Input.Native[group.GroupID]
		if !found || native.ReplicaID != group.Destination.ReplicaID ||
			len(native.ConfState.Voters) != 1 || native.ConfState.Voters[0] != group.Destination.ReplicaID ||
			len(native.ConfState.Learners)+len(native.ConfState.LearnersNext)+
				len(native.ConfState.VotersOutgoing) != 0 ||
			native.ConfState.AutoLeave || native.Grant == nil || native.Grant.Purpose != topology.AuthorityFence {
			continue
		}
		if !s.recoveryNativeFenceKnown(native) {
			continue
		}
		if recoveryBirthInPlan(read, group.GroupID, topology.AuthorityTarget{NodeName: group.Destination.NodeName,
			MemberID: s.Recovery.Survivor.ReplicaID, ReplicaID: group.Destination.ReplicaID}) {
			result = append(result, group.GroupID)
		}
	}
	slices.Sort(result)
	return result
}

func (s *scopedState) recoveryNativeFenceKnown(native topology.NativeState) bool {
	record := s.indexes.grants[native.Grant.Key]
	if record == nil || !record.Grant.Equal(*native.Grant) {
		return false
	}
	expected := topology.AuthorityTarget{NodeName: s.Recovery.Survivor.NodeName,
		MemberID: s.Recovery.Survivor.ReplicaID, ReplicaID: native.ReplicaID}
	if !slices.Contains(record.Grant.Targets, expected) {
		return false
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: native.GroupID}
	return slices.ContainsFunc(record.Fences, func(proof topology.FenceEvidence) bool {
		return proof.Resource == resource && proof.Key == record.Grant.Key && proof.AppliedIndex != 0 &&
			proof.AppliedIndex <= native.Applied && proof.Validate(record.Grant) == nil
	})
}

func recoveryBirthInPlan(read PlanRead, group string, target topology.AuthorityTarget) bool {
	return slices.ContainsFunc(read.Grants, func(grant topology.AuthorityGrant) bool {
		action := grant.Action
		if action == nil || action.Kind != topology.StepCreateFoundingGroup || action.GroupID != group ||
			action.Version != read.Operation.Status.Version || action.Attempt >= read.Operation.Status.Attempt {
			return false
		}
		if action.StepSeq < 1 || action.StepSeq > len(read.Operation.Plan.Steps) {
			return false
		}
		step := read.Operation.Plan.Steps[action.StepSeq-1]
		if !action.Matches(grant.OperationID, action.Attempt, action.Version, step) {
			return false
		}
		return len(grant.Targets) == 1 && grant.Targets[0] == target && grant.Key.ClusterID == read.Target.ClusterID
	})
}
