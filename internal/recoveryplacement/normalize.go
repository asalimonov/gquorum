package recoveryplacement

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type allowance struct{ minimum, target int }

func allowances(c Context, approved, base, desired topology.Document) (map[string]allowance, error) {
	allowed := make(map[string]allowance)
	for _, group := range desired.Groups {
		if !c.applies(desired, group.ID) {
			continue
		}
		initial := c.initial(base, desired, group.ID)
		if !initial && (!c.applies(approved, group.ID) || !c.applies(base, group.ID)) {
			continue
		}
		before := base
		if initial {
			before = desired
		}
		if err := noWorsenedDeficit(before, desired, group.ID); err != nil {
			return nil, err
		}
		available, err := eligibleCount(desired, group.ID)
		if err != nil {
			return nil, err
		}
		if available == 0 {
			return nil, fault.New(fault.Invalid, "recovery group has no eligible voter", nil)
		}
		allowed[group.ID] = normalizedCounts(desired, group, available)
	}
	return allowed, nil
}

func (c Context) initial(base, desired topology.Document, groupID string) bool {
	if c.phase != "importing" || base.Version != 1 || len(base.Groups) != 1 ||
		desired.Version != c.version || base.ClusterID != c.cluster {
		return false
	}
	meta := base.MetaGroup()
	if meta == nil || len(meta.Replicas) != 1 || meta.Replicas[0].NodeName != c.survivor.NodeName ||
		meta.Replicas[0].ReplicaID != c.survivor.ReplicaID || meta.Replicas[0].Role != topology.ReplicaVoter {
		return false
	}
	group := desired.Group(groupID)
	destination, found := c.groups[groupID]
	return found && group != nil && len(group.Replicas) == 1 &&
		group.Replicas[0].NodeName == destination.NodeName &&
		group.Replicas[0].ReplicaID == destination.ReplicaID && group.Replicas[0].Role == topology.ReplicaVoter &&
		!group.Replicas[0].PinnedLearner
}

func eligibleCount(doc topology.Document, id string) (int, error) {
	report, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: doc, Desired: doc, GroupID: id})
	if err != nil {
		return 0, err
	}
	group := doc.Group(id)
	if group.VoterConfig != nil && group.VoterConfig.DistinctVoterLabel != "" {
		return len(report.EligibleDomains), nil
	}
	count := 0
	for _, candidate := range report.Candidates {
		if candidate.CanVote {
			count++
		}
	}
	return count, nil
}

func noWorsenedDeficit(before, desired topology.Document, id string) error {
	old, next := before.Group(id), desired.Group(id)
	if old == nil || next == nil {
		return fault.ErrOperationAuthorization
	}
	current, nextCount := len(before.VotersOf(id)), len(desired.VotersOf(id))
	if nextCount == 0 {
		return fault.New(fault.Invalid, "recovery cannot remove the last voter", nil)
	}
	minimum, _ := desired.GroupBounds(id)
	if id == topology.MetaGroupID {
		minimum = 1
	}
	required := minimum
	if next.VoterConfig != nil {
		required = max(required, next.VoterConfig.TargetVoters)
	}
	if max(0, required-nextCount) > max(0, required-current) {
		return worseDeficit(id)
	}
	oldEligible, err := eligibleCount(before, id)
	if err != nil {
		return err
	}
	nextEligible, err := eligibleCount(desired, id)
	if err != nil {
		return err
	}
	if max(0, required-nextEligible) > max(0, required-oldEligible) {
		return worseDeficit(id)
	}
	return nil
}

func worseDeficit(group string) error {
	return fault.New(fault.Invalid, fmt.Sprintf("recovery edit worsens the retained policy deficit for %q",
		group), nil)
}

func normalized(input topology.Document, allowed map[string]allowance) topology.Document {
	doc := input.Clone()
	for id, counts := range allowed {
		group := doc.Group(id)
		if group == nil {
			continue
		}
		if id != topology.MetaGroupID {
			placement := doc.GroupPlacement(id).Clone()
			placement.MinReplicas = min(placement.MinReplicas, counts.minimum)
			group.Placement = &placement
		}
		if group.VoterConfig != nil {
			policy := *group.VoterConfig
			policy.TargetVoters = min(policy.TargetVoters, counts.target)
			group.VoterConfig = &policy
		}
	}
	return doc
}

func restorePolicies(output *topology.Document, original topology.Document) {
	output.Placement = original.Placement.Clone()
	for _, originalGroup := range original.Groups {
		group := output.Group(originalGroup.ID)
		if group == nil {
			continue
		}
		group.Placement = nil
		if originalGroup.Placement != nil {
			placement := originalGroup.Placement.Clone()
			group.Placement = &placement
		}
		group.VoterConfig = nil
		if originalGroup.VoterConfig != nil {
			policy := *originalGroup.VoterConfig
			group.VoterConfig = &policy
		}
	}
}

func normalizedCounts(desired topology.Document, group topology.Group, available int) allowance {
	minimum, _ := desired.GroupBounds(group.ID)
	if group.ID == topology.MetaGroupID {
		minimum = 1
	}
	target := minimum
	if group.VoterConfig != nil {
		target = group.VoterConfig.TargetVoters
	}
	assigned := len(desired.VotersOf(group.ID))
	return allowance{minimum: min(minimum, available, assigned), target: min(target, available)}
}
