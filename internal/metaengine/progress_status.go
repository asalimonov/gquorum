package metaengine

import (
	"cmp"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const statusOperationLimit = 100

// ProgressStatus is owned local diagnosis, without a quorum or freshness claim.
func (m *Machine) ProgressStatus() (topology.ClusterStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryScopedStatus(nil) // Task78 installs complete scoped state here.
}

func queryScopedStatus(s *scopedState) (topology.ClusterStatus, error) {
	if s == nil {
		return topology.ClusterStatus{}, fault.ErrUnsupported
	}
	views, _ := queryScopedTopology(s)
	status := topology.ClusterStatus{LatestVersion: views.Latest.Version, TargetVersion: views.Target.Version,
		FullyConverged: views.FullyConverged, Cleanup: s.cleanupSummary()}
	groups, _ := s.progressNames()
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		status.Groups = append(status.Groups, s.groupProgress(id).Clone())
	}
	for _, node := range s.Latest.Nodes {
		row := s.nodeProgress(node.Name).Clone()
		status.Nodes = append(status.Nodes, topology.NodeStatus{Node: *row.Desired,
			EffectiveState: s.progressNodeState(row), Health: topology.HealthUnknown})
	}
	status.Operations, status.OperationsTruncated = s.operationHeaders()
	return status, nil
}

func (s *scopedState) progressNodeState(row topology.NodeProgress) topology.NodeState {
	if row.Converged == nil {
		return row.Desired.State
	}
	if row.Converged.State != topology.NodeDraining {
		return row.Converged.State
	}
	for _, group := range s.Groups {
		if group.GroupID != topology.MetaGroupID &&
			(groupNamesNode(group.Declaration, row.NodeName) ||
				groupNamesNode(s.Target.Group(group.GroupID), row.NodeName)) {
			return topology.NodeDraining
		}
	}
	return topology.NodeDrained
}

func (s *scopedState) operationHeaders() ([]topology.OperationStatus, bool) {
	ops := slices.SortedFunc(maps.Values(s.indexes.operations), func(a, b *topology.Operation) int {
		return cmp.Or(cmp.Compare(b.Status.Version, a.Status.Version), cmp.Compare(b.Status.Attempt, a.Status.Attempt))
	})
	headers := make([]topology.OperationStatus, 0, min(len(ops), statusOperationLimit))
	for _, op := range ops[:min(len(ops), statusOperationLimit)] {
		header := op.Status
		header.Blocks, header.TotalBlocks = nil, len(op.Status.Blocks)
		header.BlocksTruncated = header.TotalBlocks != 0
		headers = append(headers, header)
	}
	return headers, len(ops) > statusOperationLimit
}

func (s *scopedState) cleanupSummary() topology.CleanupSummary {
	var summary topology.CleanupSummary
	for _, debt := range s.Cleanup {
		if cleanupComplete(debt) {
			continue
		}
		summary.Pending++
		if debt.Retired != nil {
			summary.Unconfirmed++
		}
		if summary.OldestPendingAt == nil || debt.CreatedAt.Before(*summary.OldestPendingAt) {
			summary.OldestPendingAt = copyCleanupValue(&debt.CreatedAt)
		}
	}
	return summary
}

// LegacyGroupProgress is the schema3 transition projection, removed at Task78.
// Active proves these declarations only as a whole document. No owner, deleting
// flag or blocker is inferred from the old global operation or missing Target.
func LegacyGroupProgress(views topology.ReadView) []topology.GroupProgress {
	groups := make(map[string]bool)
	for _, doc := range []topology.Document{views.Latest, views.Target, views.Active} {
		for _, group := range doc.Groups {
			groups[group.ID] = true
		}
	}
	rows := make([]topology.GroupProgress, 0, len(groups))
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		row := groupConvergence{GroupID: id, Declaration: views.Active.Group(id)}
		if row.Declaration != nil {
			row.SourceVersion = views.Active.Version
		}
		rows = append(rows, declaredGroupProgress(views.Latest, views.Target, row).Clone())
	}
	return rows
}

// Borrowed under the Machine read lock, like the legacy topology query. Query
// marshals the pair before releasing that lock; direct owned reads must clone.
func queryScopedTopology(s *scopedState) (topology.ReadView, error) {
	if s == nil {
		return topology.ReadView{}, fault.ErrUnsupported
	}
	return topology.ReadView{AppliedIndex: s.AppliedIndex, Latest: s.Latest, Target: s.Target,
		FullyConverged: s.FullyConverged}, nil
}
