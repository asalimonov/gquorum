package metaengine

import "github.com/asalimonov/gquorum/topology"

type progressRecord struct {
	key       progressKey
	operation *topology.Operation
	block     int
	cleanup   *cleanupRecord
}

func (s *scopedState) progressRecords(q topology.ProgressQuery) []progressRecord {
	groups, nodes := s.progressNames()
	var rows []progressRecord
	for group := range groups {
		if s.progressGroupMatches(group, q) {
			rows = append(rows, progressRecord{key: progressKey{Kind: progressGroup, ID: group}})
		}
	}
	for node := range nodes {
		if s.progressNodeMatches(node, q) {
			rows = append(rows, progressRecord{key: progressKey{Kind: progressNode, ID: node}})
		}
	}
	for i := range s.Operations {
		op := &s.Operations[i]
		for j, block := range op.Status.Blocks {
			if s.progressOperationMatches(op, j, q) {
				rows = append(rows, progressRecord{key: progressKey{Kind: progressOperation,
					ID: string(op.Status.ID), Scope: block.GroupID}, operation: op, block: j})
			}
		}
	}
	for i := range s.Cleanup {
		debt := &s.Cleanup[i]
		if progressCleanupMatches(debt, q) {
			rows = append(rows, progressRecord{key: progressKey{Kind: progressCleanup,
				ID: debt.Identity.GroupID, Scope: debt.Identity.NodeName,
				ReplicaID: debt.Identity.ReplicaID}, cleanup: debt})
		}
	}
	return rows
}

func progressCleanupMatches(debt *cleanupRecord, q topology.ProgressQuery) bool {
	return (q.GroupID == "" || q.GroupID == debt.Identity.GroupID) &&
		(q.NodeName == "" || q.NodeName == debt.Identity.NodeName) &&
		(q.OperationID == "" || q.OperationID == debt.OperationID)
}

func (s *scopedState) progressNames() (groups, nodes map[string]bool) {
	groups, nodes = make(map[string]bool), make(map[string]bool)
	for _, doc := range []topology.Document{s.Latest, s.Target} {
		for _, group := range doc.Groups {
			groups[group.ID] = true
		}
		for _, node := range doc.Nodes {
			nodes[node.Name] = true
		}
	}
	for _, row := range s.Groups {
		groups[row.GroupID] = true
	}
	for _, row := range s.Nodes {
		nodes[row.NodeName] = true
	}
	for _, debt := range s.Cleanup {
		groups[debt.Identity.GroupID], nodes[debt.Identity.NodeName] = true, true
	}
	return groups, nodes
}

func (s *scopedState) progressGroupMatches(group string, q topology.ProgressQuery) bool {
	if q.GroupID != "" && q.GroupID != group {
		return false
	}
	row := s.indexes.groups[group]
	return (q.OperationID == "" || row != nil && q.OperationID == row.Owner) &&
		(q.NodeName == "" || s.progressGroupNamesNode(group, q.NodeName))
}

func (s *scopedState) progressNodeMatches(node string, q topology.ProgressQuery) bool {
	if q.NodeName != "" && q.NodeName != node {
		return false
	}
	row := s.indexes.nodes[node]
	return (q.OperationID == "" || row != nil && q.OperationID == row.Owner) &&
		(q.GroupID == "" || s.progressGroupNamesNode(q.GroupID, node))
}

// Scope follows current declarations and exact retained debt, never arbitrary
// old documents that happen to survive for an unrelated retention root.
func (s *scopedState) progressGroupNamesNode(group, node string) bool {
	// _meta owns the node catalog, including declared nodes without a replica.
	if group == topology.MetaGroupID &&
		(s.indexes.nodes[node] != nil || s.Latest.Node(node) != nil || s.Target.Node(node) != nil) {
		return true
	}
	p := s.groupProgress(group)
	if groupNamesNode(p.Desired, node) || groupNamesNode(p.Approved, node) || groupNamesNode(p.Converged, node) {
		return true
	}
	for _, debt := range s.Cleanup {
		if debt.Identity.GroupID == group && debt.Identity.NodeName == node {
			return true
		}
	}
	return false
}

func (s *scopedState) progressOperationMatches(op *topology.Operation, position int,
	q topology.ProgressQuery) bool {
	block := op.Plan.Blocks[position]
	if q.OperationID != "" && q.OperationID != op.Status.ID || q.GroupID != "" && q.GroupID != block.GroupID {
		return false
	}
	if q.NodeName == "" {
		return true
	}
	for _, version := range []uint64{block.SourceVersion, op.Status.Version} {
		doc := s.indexes.documents[version]
		if block.GroupID == topology.MetaGroupID && doc.Node(q.NodeName) != nil ||
			groupNamesNode(doc.Group(block.GroupID), q.NodeName) {
			return true
		}
	}
	return progressBlockNamesNode(op.Plan, block, q.NodeName)
}

func progressBlockNamesNode(plan topology.Plan, block topology.PlanBlock, node string) bool {
	for _, steps := range [][]int{block.Steps, block.Cleanup} {
		for _, seq := range steps {
			step := plan.Steps[seq-1]
			if step.NodeName == node || step.TransferNodeName == node {
				return true
			}
		}
	}
	return false
}
