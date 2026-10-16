package metaengine

import (
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func prepareApprovalNodes(source, next *scopedState, operation *topology.Operation) error {
	next.Nodes = approvalNodeRows(source.Nodes, next.Target)
	metadata, _ := next.latestGroupOperation(topology.MetaGroupID)
	if metadata == nil {
		return nil
	}
	index, err := next.buildIndexes()
	if err != nil {
		return err
	}
	for i := range next.Nodes {
		row := &next.Nodes[i]
		if nodeMatchesTarget(*row, next.Target) {
			row.Owner = ""
			continue
		}
		if operation.UpdatedAt.Before(row.CompletedAt) {
			return fault.ErrOperationAuthorization
		}
		row.Owner = pendingNodeOwner(*row, metadata)
		if !nodeApprovalReady(*row, next, index, metadata) {
			continue
		}
		declaration := (topology.NodeProgress{Converged: next.Target.Node(row.NodeName)}).Clone().Converged
		*row = nodeConvergence{NodeName: row.NodeName, Declaration: declaration,
			MemberID:      next.Target.MemberID(row.NodeName),
			SourceVersion: next.Target.Version, CompletedAt: operation.UpdatedAt}
	}
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

func nodeApprovalReady(row nodeConvergence, next *scopedState,
	index *scopedIndexes, operation *topology.Operation) bool {
	meta := index.groups[topology.MetaGroupID]
	if meta == nil || meta.Owner != "" || meta.SourceVersion == 0 ||
		!reflect.DeepEqual(meta.Declaration, next.Target.MetaGroup()) {
		return false
	}
	for _, group := range next.Groups {
		if group.GroupID == topology.MetaGroupID ||
			!groupNamesNode(group.Declaration, row.NodeName) &&
				!groupNamesNode(next.Target.Group(group.GroupID), row.NodeName) {
			continue
		}
		dependent := row.MemberID != next.Target.MemberID(row.NodeName) ||
			slices.ContainsFunc(operation.Plan.Blocks, func(block topology.PlanBlock) bool {
				return block.GroupID == group.GroupID
			})
		if dependent && !nodeGroupReady(group, row.NodeName, next.Target, index) {
			return false
		}
	}
	return true
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

func (s *scopedState) updateFullyConverged(at time.Time) {
	if s.Target.Version == 0 || s.FullyConverged.Version >= s.Target.Version {
		return
	}
	for _, row := range s.Groups {
		if row.Owner != "" || row.Deleting || row.SourceVersion == 0 && s.Target.Group(row.GroupID) != nil ||
			!reflect.DeepEqual(row.Declaration, s.Target.Group(row.GroupID)) {
			return
		}
	}
	for _, row := range s.Nodes {
		if !nodeMatchesTarget(row, s.Target) {
			return
		}
	}
	for _, op := range s.Operations {
		for _, block := range op.Status.Blocks {
			if !block.State.Terminal() {
				return
			}
		}
	}
	s.FullyConverged = topology.FullyConverged{Version: s.Target.Version, At: at}
}
