package topology

import (
	"reflect"
	"slices"
)

// AffectedGroups derives declaration and placement scopes from approved intent
// and a candidate. The result is deterministic and includes _meta for node edits.
// It does not include runtime admission dependencies or authorize any effects.
// Callers must validate documents separately; uncertain eligibility conflicts.
func AffectedGroups(approved, desired Document) []string {
	affected := make(map[string]bool)
	for _, id := range AffectedPlacementGroups(approved, desired) {
		if id != MetaGroupID {
			affected[id] = true
		}
	}
	for _, id := range documentGroupIDs(approved, desired) {
		if approved.ClusterID != desired.ClusterID || !reflect.DeepEqual(approved.Group(id), desired.Group(id)) ||
			placementIdentityChanged(approved, desired, id) {
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

// A repair reads exact identities of eligible candidates as well as current
// hosts. A member reseat cannot be rebased through that read even if the node's
// placement labels are unchanged. Descriptive node fields are not such reads.
func placementIdentityChanged(before, after Document, id string) bool {
	if id == MetaGroupID {
		return false
	}
	for _, doc := range []Document{before, after} {
		group := doc.Group(id)
		if group == nil {
			return true
		}
		for _, replica := range group.Replicas {
			if before.MemberID(replica.NodeName) != after.MemberID(replica.NodeName) {
				return true
			}
		}
		report, err := EvaluatePlacement(PlacementInput{Approved: before, Desired: doc, GroupID: id})
		if err != nil {
			return true
		}
		if candidateIdentityChanged(before, after, report) {
			return true
		}
	}
	return false
}

func candidateIdentityChanged(before, after Document, report GroupPlacementReport) bool {
	for _, candidate := range report.Candidates {
		if (candidate.CanAdd || candidate.CanRetain || candidate.CanVote) &&
			before.MemberID(candidate.NodeName) != after.MemberID(candidate.NodeName) {
			return true
		}
	}
	return false
}
