package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Caller supplies trusted approved ancestry. This grants no effect authority;
// apply must separately check versions, allocation history and current owners.
func validateIndependentEdits(base, staged, repair topology.Document) error {
	return validateIndependentRebase(rebaseSemanticDocument(base), rebaseSemanticDocument(staged),
		rebaseSemanticDocument(repair))
}

func validateIndependentRebase(base, staged, repair topology.Document) error {
	if base.ClusterID != staged.ClusterID || base.ClusterID != repair.ClusterID {
		return fault.ErrVersionConflict
	}
	stagedGroups := topology.AffectedGroups(base, staged)
	repairGroups := topology.AffectedGroups(base, repair)
	writes := make(map[string]bool, len(stagedGroups))
	for _, group := range stagedGroups {
		writes[group] = true
	}
	for _, group := range repairGroups {
		if writes[group] {
			return fault.New(fault.VersionConflict, "staged and repair ownership scopes overlap: "+group, nil)
		}
	}
	if err := validatePlacementReads(base, repair, staged, repairGroups); err != nil {
		return err
	}
	return validatePlacementReads(base, staged, repair, stagedGroups)
}

func validatePlacementReads(base, edited, other topology.Document, groups []string) error {
	placement := make(map[string]bool)
	for _, id := range topology.AffectedPlacementGroups(base, edited) {
		placement[id] = true
	}
	for _, id := range groups {
		group := edited.Group(id)
		if id == topology.MetaGroupID || group == nil || !placement[id] {
			continue // Deletion and descriptive labels do not select new hosts.
		}
		if group.Placement == nil && !reflect.DeepEqual(base.Placement, other.Placement) {
			return fault.New(fault.VersionConflict, "effective placement rule changed: "+id, nil)
		}
		before, err := placementReadSet(base, edited, base, id)
		if err != nil {
			return fault.New(fault.VersionConflict, "cannot prove prior placement dependencies", err)
		}
		after, err := placementReadSet(base, edited, other, id)
		if err != nil || !slices.Equal(before, after) {
			return fault.New(fault.VersionConflict, "placement candidate dependencies changed: "+id, err)
		}
	}
	return nil
}

// Candidate eligibility is evaluated under the edit's proposed group rules, not
// only the old rules: adopting a selector reads labels that were irrelevant before.
type placementRead struct {
	nodeName                   string
	memberID                   uint64
	canAdd, canRetain, canVote bool
	domain                     string
}

func placementReadSet(approved, edited, nodes topology.Document, id string) ([]placementRead, error) {
	view := edited.Clone()
	view.Nodes = nodes.Nodes
	report, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: approved, Desired: view, GroupID: id})
	if err != nil {
		return nil, err
	}
	assigned := make(map[string]bool)
	for _, replica := range edited.Group(id).Replicas {
		assigned[replica.NodeName] = true
	}
	reads := make([]placementRead, 0, len(report.Candidates))
	for _, candidate := range report.Candidates {
		if !assigned[candidate.NodeName] && !candidate.CanAdd && !candidate.CanVote {
			continue
		}
		member := nodes.MemberID(candidate.NodeName)
		if member == 0 {
			return nil, fault.New(fault.VersionConflict, "placement candidate has no exact member identity", nil)
		}
		reads = append(reads, placementRead{nodeName: candidate.NodeName, memberID: member,
			canAdd: candidate.CanAdd, canRetain: candidate.CanRetain, canVote: candidate.CanVote,
			domain: candidate.Domain})
	}
	return reads, nil
}
