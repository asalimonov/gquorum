package topology

import (
	"fmt"
	"slices"
)

func voterKeys(s voterPlanState) []ReplicaKey {
	return voterKeysForSet(s, s.voters)
}

func voterKeysForSet(s voterPlanState, voters map[uint64]struct{}) []ReplicaKey {
	keys := make([]ReplicaKey, 0, len(voters))
	for id := range voters {
		replica, ok := s.base.planReplicaKey(s.groupID, s.desired.Group(s.groupID), id)
		if ok {
			keys = append(keys, replica)
		}
	}
	slices.SortFunc(keys, func(a, b ReplicaKey) int {
		if a.ReplicaID < b.ReplicaID {
			return -1
		}
		if a.ReplicaID > b.ReplicaID {
			return 1
		}
		return compareNames(a.NodeName, b.NodeName)
	})
	return keys
}

// VoterSets pairs native member IDs before and after one voter-set transition.
type VoterSets struct {
	Before, After []uint64
}

// ValidateVoterSetTransition checks one native voter-set change against the
// proven and desired label interpretations and exact retained identities.
func ValidateVoterSetTransition(input PlanInput, desired Document, groupID string, sets VoterSets) error {
	if err := validateRetainedInput(input, desired); err != nil {
		return err
	}
	before, after := sets.Before, sets.After
	for _, ids := range [][]uint64{before, after} {
		for _, id := range ids {
			if _, ok := input.planReplicaKey(groupID, desired.Group(groupID), id); !ok {
				return planUnsafef("group %q has unknown voter identity %d", groupID, id)
			}
		}
	}
	state := voterPlanState{base: input, desired: desired, groupID: groupID}
	return validateVoterTransition(voterPlanTopology{base: input, desired: desired, groupID: groupID},
		voterKeysForSet(state, idSet(before)), voterKeysForSet(state, idSet(after)), state)
}

// validateVoterTransition freezes old and new label interpretations separately.
// Target-invalid exact identities and pairs may disappear, never be exchanged
// for different violations during an approved migration.
func validateVoterTransition(view voterPlanTopology, before, after []ReplicaKey, state voterPlanState) error {
	base, desired, groupID := view.base, view.desired, view.groupID
	if state.groupID != groupID {
		return planUnsafef("voter transition group identity changed")
	}
	if err := validateOldDomains(base, desired, groupID, after); err != nil {
		return err
	}
	prior, err := targetVoterViolations(base, desired, groupID, before)
	if err != nil {
		return err
	}
	next, err := targetVoterViolations(base, desired, groupID, after)
	if err != nil {
		return err
	}
	for violation := range next {
		if !prior[violation] {
			return planUnsafef("group %q introduces Target placement violation %s", groupID, violation)
		}
	}
	return nil
}

func validateOldDomains(base PlanInput, desired Document, groupID string, voters []ReplicaKey) error {
	group := base.provenGroup(groupID)
	if group == nil || group.VoterConfig == nil || group.VoterConfig.DistinctVoterLabel == "" {
		return nil
	}
	key := group.VoterConfig.DistinctVoterLabel
	seen := make(map[string]uint64, len(voters))
	for _, voter := range voters {
		node := base.provenNode(voter.NodeName)
		if node == nil {
			node = desired.Node(voter.NodeName)
		}
		if node == nil || node.Labels[key] == "" {
			return planUnsafef("group %q has voter %d without old-rule domain %q", groupID, voter.ReplicaID, key)
		}
		if prior := seen[node.Labels[key]]; prior != 0 {
			return planUnsafef("group %q old-rule domain %q has voters %d and %d", groupID, node.Labels[key], prior,
				voter.ReplicaID)
		}
		seen[node.Labels[key]] = voter.ReplicaID
	}
	return nil
}

func targetVoterViolations(base PlanInput, desired Document,
	groupID string, voters []ReplicaKey) (map[string]bool, error) {
	report, err := EvaluatePlacement(PlacementInput{Approved: base.Approved, Desired: desired, GroupID: groupID})
	if err != nil {
		return nil, err
	}
	group := desired.Group(groupID)
	candidates := make(map[string]PlacementCandidate, len(report.Candidates))
	for _, candidate := range report.Candidates {
		candidates[candidate.NodeName] = candidate
	}
	violations := make(map[string]bool)
	domains := make(map[string][]uint64)
	for _, voter := range voters {
		candidate, exists := candidates[voter.NodeName]
		if !exists || !candidate.CanVote {
			violations[fmt.Sprintf("invalid:%d", voter.ReplicaID)] = true
		}
		if group.VoterConfig != nil && group.VoterConfig.DistinctVoterLabel != "" {
			recordDomainViolations(candidate, voter, domains, violations)
		}
	}
	return violations, nil
}

func recordDomainViolations(candidate PlacementCandidate, voter ReplicaKey, domains map[string][]uint64,
	violations map[string]bool) {
	if candidate.Domain == "" {
		violations[fmt.Sprintf("missing:%d", voter.ReplicaID)] = true
	} else {
		for _, prior := range domains[candidate.Domain] {
			lo, hi := prior, voter.ReplicaID
			if lo > hi {
				lo, hi = hi, lo
			}
			violations[fmt.Sprintf("pair:%d:%d", lo, hi)] = true
		}
		domains[candidate.Domain] = append(domains[candidate.Domain], voter.ReplicaID)
	}
}
