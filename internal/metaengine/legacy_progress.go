package metaengine

import (
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// legacyProgress adapts the complete schema-3 document and operator records to
// the bounded progress API. It is removed with schema-3 persistence at Task78.
// The caller holds m.mu for reading.
func legacyProgress(m *Machine, q topology.ProgressQuery) (topology.ProgressPage, error) {
	if err := q.Validate(); err != nil {
		return topology.ProgressPage{}, err
	}
	views := topology.ReadView{AppliedIndex: m.appliedIndex, Latest: m.latest,
		Target: m.target, Active: m.active}
	rows := legacyGroupRows(m, q, views)
	rows = append(rows, legacyNodeRows(m, q)...)
	rows = append(rows, legacyOperationRows(m, q)...)
	// Schema 3 persists Active but no completion timestamp; leave At zero instead
	// of misrepresenting the document creation time as convergence evidence.
	fullyConverged := topology.FullyConverged{Version: m.active.Version}
	return pageProgress(progressPageHeader{cluster: m.latest.ClusterID, query: q, applied: m.appliedIndex,
		latest: m.latest.Version, target: m.target.Version, converged: fullyConverged}, rows, appendLegacyProgress)
}

func legacyGroupRows(m *Machine, q topology.ProgressQuery, views topology.ReadView) []progressRecord {
	rows := make([]progressRecord, 0)
	for _, row := range LegacyGroupProgress(views) {
		if !legacyGroupMatches(m, row.GroupID, q) {
			continue
		}
		owned := row.Clone()
		rows = append(rows, progressRecord{key: progressKey{Kind: progressGroup, ID: row.GroupID}, legacyGroup: &owned})
	}
	return rows
}

func legacyNodeRows(m *Machine, q topology.ProgressQuery) []progressRecord {
	rows := make([]progressRecord, 0)
	nodes := make(map[string]bool)
	for _, doc := range []topology.Document{m.latest, m.target, m.active} {
		for _, node := range doc.Nodes {
			nodes[node.Name] = true
		}
	}
	for _, name := range slices.Sorted(maps.Keys(nodes)) {
		if !legacyNodeMatches(m, name, q) {
			continue
		}
		row := topology.NodeProgress{NodeName: name, Desired: m.latest.Node(name),
			Approved: m.target.Node(name), Converged: m.active.Node(name),
			MemberID: m.active.MemberID(name)}
		if row.Converged != nil {
			row.ConvergedVersion = m.active.Version
		}
		row = row.Clone()
		rows = append(rows, progressRecord{key: progressKey{Kind: progressNode, ID: name}, legacyNode: &row})
	}
	return rows
}

func legacyOperationRows(m *Machine, q topology.ProgressQuery) []progressRecord {
	rows := make([]progressRecord, 0, len(m.operators))
	for _, id := range slices.Sorted(maps.Keys(m.operators)) {
		record := m.operators[id]
		if !legacyOperationMatches(record, q) {
			continue
		}
		status, err := legacyOperationStatus(m.operators, topology.OperationID(id))
		if err != nil || status.TotalBlocks == 0 {
			continue
		}
		operation := &topology.Operation{Status: status, Plan: topology.Plan{Steps: slices.Clone(record.Steps)}}
		rows = append(rows, progressRecord{key: progressKey{Kind: progressOperation,
			ID: string(id), Scope: record.Steps[0].GroupID}, operation: operation, block: 0})
	}
	return rows
}

func appendLegacyProgress(page *topology.ProgressPage, row progressRecord) {
	if row.legacyGroup != nil {
		page.Groups = append(page.Groups, row.legacyGroup.Clone())
	} else if row.legacyNode != nil {
		page.Nodes = append(page.Nodes, row.legacyNode.Clone())
	} else if row.operation != nil {
		page.Operations = append(page.Operations, row.operation.Status.Clone())
	}
}

func legacyGroupMatches(m *Machine, group string, q topology.ProgressQuery) bool {
	if q.GroupID != "" && q.GroupID != group {
		return false
	}
	if q.OperationID != "" && !legacyOperationTouches(
		m.operators[topology.OperatorID(q.OperationID)], group, q.NodeName) {
		return false
	}
	if q.NodeName == "" {
		return true
	}
	return groupNamesNode(m.latest.Group(group), q.NodeName) || groupNamesNode(m.target.Group(group), q.NodeName) ||
		groupNamesNode(m.active.Group(group), q.NodeName) || group == topology.MetaGroupID &&
		(m.latest.Node(q.NodeName) != nil || m.target.Node(q.NodeName) != nil || m.active.Node(q.NodeName) != nil)
}

func legacyNodeMatches(m *Machine, node string, q topology.ProgressQuery) bool {
	if q.NodeName != "" && q.NodeName != node {
		return false
	}
	if q.GroupID != "" && !legacyGroupMatches(m, q.GroupID,
		topology.ProgressQuery{GroupID: q.GroupID, NodeName: node}) {
		return false
	}
	return q.OperationID == "" || legacyOperationTouches(
		m.operators[topology.OperatorID(q.OperationID)], q.GroupID, node)
}

func legacyOperationMatches(record *operatorRecord, q topology.ProgressQuery) bool {
	if record == nil || q.OperationID != "" && topology.OperationID(record.ID) != q.OperationID {
		return false
	}
	return (q.GroupID == "" && q.NodeName == "") || legacyOperationTouches(record, q.GroupID, q.NodeName)
}

// Filters match every retained plan step, not just the operation's current cursor.
func legacyOperationTouches(record *operatorRecord, group, node string) bool {
	if record == nil {
		return false
	}
	for _, step := range record.Steps {
		if (group == "" || step.GroupID == group) &&
			(node == "" || step.NodeName == node || step.TransferNodeName == node) {
			return true
		}
	}
	return false
}
