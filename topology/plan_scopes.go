package topology

import (
	"reflect"
	"slices"
)

// AffectedGroups derives operation ownership scopes from approved intent and a
// candidate. The result is deterministic and includes _meta for node edits.
// Unassigned repair candidates are read dependencies, not ownership scopes.
// It does not include runtime admission dependencies or authorize any effects.
// Callers must validate documents separately; uncertain eligibility conflicts.
func AffectedGroups(approved, desired Document) []string {
	affected := make(map[string]bool)
	for _, id := range documentGroupIDs(approved, desired) {
		if approved.ClusterID != desired.ClusterID || !reflect.DeepEqual(approved.Group(id), desired.Group(id)) ||
			assignedPlacementChanged(approved, desired, id) {
			affected[id] = true
		}
	}
	if nodeDeclarationsChanged(approved, desired) {
		affected[MetaGroupID] = true
	}
	ids := sortedMapKeys(affected)
	slices.SortFunc(ids, comparePlanGroups)
	return ids
}

func documentGroupIDs(before, after Document) []string {
	ids := make(map[string]bool, len(before.Groups)+len(after.Groups))
	for _, group := range before.Groups {
		ids[group.ID] = true
	}
	for _, group := range after.Groups {
		ids[group.ID] = true
	}
	return sortedMapKeys(ids)
}

func nodeDeclarationsChanged(before, after Document) bool {
	if len(before.Nodes) != len(after.Nodes) {
		return true
	}
	for _, node := range before.Nodes {
		if !reflect.DeepEqual(&node, after.Node(node.Name)) {
			return true
		}
	}
	return false
}

// Only assigned hosts can change this group's execution ownership. The FSM
// separately checks candidate reads when proving staged/repair independence.
func assignedPlacementChanged(before, after Document, id string) bool {
	if id == MetaGroupID {
		return false
	}
	if !reflect.DeepEqual(before.GroupPlacement(id), after.GroupPlacement(id)) {
		return true
	}
	for _, replica := range before.Group(id).Replicas {
		old, next := before.Node(replica.NodeName), after.Node(replica.NodeName)
		if old == nil || next == nil || old.State != next.State ||
			before.MemberID(replica.NodeName) != after.MemberID(replica.NodeName) {
			return true
		}
	}
	previous, err := assignedCandidates(before, before, id)
	if err != nil {
		return true
	}
	proposed, err := assignedCandidates(before, after, id)
	return err != nil || !reflect.DeepEqual(previous, proposed)
}

func assignedCandidates(approved, desired Document, id string) ([]PlacementCandidate, error) {
	report, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: id})
	if err != nil {
		return nil, err
	}
	group := desired.Group(id)
	return slices.DeleteFunc(report.Candidates, func(c PlacementCandidate) bool {
		return groupReplicaOnNode(group, c.NodeName) == nil
	}), nil
}
