package topology

import "slices"

func isMembershipStep(kind StepKind) bool {
	switch kind {
	case StepCreateFoundingGroup,
		StepAddLearner,
		StepPromoteVoter,
		StepDemoteVoter,
		StepRemoveReplica:
		return true
	default:
		return false
	}
}

func observedVoterIdentities(base PlanInput, desired Document, observed map[string]GroupStatus) error {
	for groupID, status := range observed {
		if !status.Found {
			continue
		}
		if err := validateObservedMembership(status); err != nil {
			return err
		}
		for _, ids := range [][]uint64{status.ConfState.Voters, status.ConfState.Learners} {
			for _, id := range ids {
				if _, ok := replicaIdentity(base.provenGroup(groupID), desired.Group(groupID), id); !ok {
					return planUnsafef("group %q has unknown observed replica identity %d", groupID, id)
				}
			}
		}
	}
	return nil
}

func bindVoterGuards(plan *Plan, observed map[string]GroupStatus, desired Document) error {
	groups := make(map[string]bool)
	for _, step := range plan.Steps {
		if isMembershipStep(step.Kind) {
			groups[step.GroupID] = true
		}
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, comparePlanGroups)
	plan.VoterBases = make([]PlanVoterBase, 0, len(ids))
	sets := make(map[string]map[uint64]struct{}, len(ids))
	for _, id := range ids {
		status := observed[id]
		voters := slices.Clone(status.ConfState.Voters)
		if !status.Found {
			voters = nil
		}
		slices.Sort(voters)
		if len(voters) > 0 && (voters[0] == 0 || len(slices.Compact(slices.Clone(voters))) != len(voters)) {
			return planUnsafef("group %q has invalid voter base", id)
		}
		plan.VoterBases = append(plan.VoterBases, PlanVoterBase{GroupID: id, Voters: voters})
		sets[id] = idSet(voters)
	}
	for i := range plan.Steps {
		step := &plan.Steps[i]
		if !isMembershipStep(step.Kind) {
			continue
		}
		if err := bindStepVoterGuard(step, sets[step.GroupID], desired); err != nil {
			return err
		}
	}
	return verifyVoterGuardResult(ids, sets, desired)
}

func digestIDSet(set map[uint64]struct{}) string {
	ids := make([]uint64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return VoterSetDigest(ids)
}

func bindStepVoterGuard(step *Step, set map[uint64]struct{}, desired Document) error {
	step.ExpectedVotersDigest = digestIDSet(set)
	switch step.Kind {
	case StepCreateFoundingGroup:
		if len(set) != 0 {
			return planUnsafef("founding group %q has a nonempty voter base", step.GroupID)
		}
		for _, replica := range desired.VotersOf(step.GroupID) {
			set[replica.ReplicaID] = struct{}{}
		}
	case StepPromoteVoter:
		set[step.ReplicaID] = struct{}{}
	case StepDemoteVoter, StepRemoveReplica:
		delete(set, step.ReplicaID)
	default: // Other steps preserve the simulated voter set.
	}
	step.ResultVotersDigest = digestIDSet(set)
	return nil
}

func verifyVoterGuardResult(ids []string, sets map[string]map[uint64]struct{}, desired Document) error {
	for _, id := range ids {
		if desired.Group(id) == nil {
			continue
		}
		want := desired.VotersOf(id)
		if len(sets[id]) != len(want) {
			return planUnsafef("group %q plan ends with %d voters, desired %d", id, len(sets[id]), len(want))
		}
		for _, replica := range want {
			if _, ok := sets[id][replica.ReplicaID]; !ok {
				return planUnsafef("group %q plan lacks desired voter %d", id, replica.ReplicaID)
			}
		}
	}
	return nil
}
