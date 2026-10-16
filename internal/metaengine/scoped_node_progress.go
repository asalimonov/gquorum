package metaengine

import (
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Called on the owned approval/completion candidate. Prepare the complete node
// slice before assigning it, so a refused graph cannot leave partial progress.
func (s *scopedState) advanceNodeProgressLocked(at time.Time) error {
	if at.IsZero() {
		return fault.ErrOperationAuthorization
	}
	rows := approvalNodeRows(s.Nodes, s.Target)
	metadata, _ := s.latestGroupOperation(topology.MetaGroupID)
	if metadata == nil {
		s.Nodes = rows
		return nil
	}
	index, err := s.buildIndexes()
	if err != nil {
		return err
	}
	for i := range rows {
		row := &rows[i]
		if nodeMatchesTarget(*row, s.Target) {
			row.Owner = ""
			continue
		}
		row.Owner = pendingNodeOwner(*row, metadata)
		proofAt, ready := nodeApprovalProof(*row, s, index)
		if !ready {
			continue
		}
		declaration := (topology.NodeProgress{Converged: s.Target.Node(row.NodeName)}).Clone().Converged
		*row = nodeConvergence{NodeName: row.NodeName, Declaration: declaration,
			MemberID:      s.Target.MemberID(row.NodeName),
			SourceVersion: s.Target.Version, CompletedAt: latestTimestamp(at, row.CompletedAt, proofAt)}
	}
	s.Nodes = rows
	return nil
}

func pendingNodeOwner(row nodeConvergence, metadata *topology.Operation) topology.OperationID {
	if row.SourceVersion >= metadata.Status.Version || metadata.Status.State.Terminal() {
		return ""
	}
	return metadata.Status.ID
}

func approvalNodeRows(source []nodeConvergence, target topology.Document) []nodeConvergence {
	rows := slices.Clone(source)
	for i := range rows {
		rows[i].Declaration = (topology.NodeProgress{Converged: rows[i].Declaration}).Clone().Converged
	}
	for _, node := range target.Nodes {
		if !slices.ContainsFunc(rows, func(row nodeConvergence) bool { return row.NodeName == node.Name }) {
			rows = append(rows, nodeConvergence{NodeName: node.Name})
		}
	}
	return rows
}

func nodeMatchesTarget(row nodeConvergence, target topology.Document) bool {
	return row.SourceVersion != 0 && reflect.DeepEqual(row.Declaration, target.Node(row.NodeName)) &&
		row.MemberID == target.MemberID(row.NodeName)
}

func nodeApprovalProof(row nodeConvergence, next *scopedState,
	index *scopedIndexes) (time.Time, bool) {
	meta := index.groups[topology.MetaGroupID]
	if meta == nil || meta.Owner != "" || meta.SourceVersion == 0 ||
		!reflect.DeepEqual(meta.Declaration, next.Target.MetaGroup()) {
		return time.Time{}, false
	}
	at := meta.CompletedAt
	for _, group := range next.Groups {
		if group.GroupID == topology.MetaGroupID ||
			!groupNamesNode(group.Declaration, row.NodeName) &&
				!groupNamesNode(next.Target.Group(group.GroupID), row.NodeName) {
			continue
		}
		dependent := row.MemberID != next.Target.MemberID(row.NodeName) ||
			nodePlacementDependency(row, group, next.Target)
		if dependent {
			if !nodeGroupReady(group, row.NodeName, next.Target, index) {
				return time.Time{}, false
			}
			at = latestTimestamp(at, group.CompletedAt)
		}
	}
	return at, true
}

// FI_NODE_DEPENDENCY: metadata supersession does not erase an older node edit's
// load prerequisites. Compare the node declarations under the current rule and
// retain the former assignment only as placement context, never as Active proof.
func nodePlacementDependency(row nodeConvergence, group groupConvergence, target topology.Document) bool {
	if row.Declaration == nil || target.Node(row.NodeName) == nil {
		return true
	}
	after := target.Clone()
	if after.Group(group.GroupID) == nil {
		if group.Declaration == nil {
			return false
		}
		after.Groups = append(after.Groups, *group.Declaration)
	}
	context := after.Group(group.GroupID)
	if !groupNamesNode(context, row.NodeName) && groupNamesNode(group.Declaration, row.NodeName) {
		context.Replicas = slices.Clone(context.Replicas)
		for _, replica := range group.Declaration.Replicas {
			if replica.NodeName == row.NodeName {
				context.Replicas = append(context.Replicas, replica)
			}
		}
	}
	before := after.Clone()
	*before.Node(row.NodeName) = *row.Declaration
	return slices.Contains(topology.AffectedGroups(before, after), group.GroupID)
}

func groupNamesNode(group *topology.Group, name string) bool {
	return group != nil && slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
		return r.NodeName == name
	})
}

func nodeGroupReady(group groupConvergence, node string, target topology.Document, index *scopedIndexes) bool {
	if group.SourceVersion == 0 || group.Owner != "" || group.Deleting ||
		!reflect.DeepEqual(group.Declaration, target.Group(group.GroupID)) {
		return false
	}
	if !groupNamesNode(group.Declaration, node) {
		return true // Native exclusion is complete; ordinary detached cleanup may remain.
	}
	source := index.documents[group.SourceVersion]
	return source.MemberID(node) == target.MemberID(node)
}

func (s *scopedState) updateFullyConvergedLocked(at time.Time) {
	if s.Target.Version == 0 || s.FullyConverged.Version >= s.Target.Version || at.IsZero() {
		return
	}
	completedAt := latestTimestamp(at, s.FullyConverged.At)
	for _, row := range s.Groups {
		if row.Owner != "" || row.Deleting || row.SourceVersion == 0 && s.Target.Group(row.GroupID) != nil ||
			!reflect.DeepEqual(row.Declaration, s.Target.Group(row.GroupID)) {
			return
		}
		completedAt = latestTimestamp(completedAt, row.CompletedAt)
	}
	for _, row := range s.Nodes {
		if !nodeMatchesTarget(row, s.Target) {
			return
		}
		completedAt = latestTimestamp(completedAt, row.CompletedAt)
	}
	for _, op := range s.Operations {
		for _, block := range op.Status.Blocks {
			if !block.State.Terminal() {
				return
			}
		}
	}
	s.FullyConverged = topology.FullyConverged{Version: s.Target.Version, At: completedAt}
}

func latestTimestamp(values ...time.Time) time.Time {
	var latest time.Time
	for _, value := range values {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}
