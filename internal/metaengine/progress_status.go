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
	return queryScopedStatus(m.scoped)
}

func queryScopedStatus(s *scopedState) (topology.ClusterStatus, error) {
	if s == nil {
		return topology.ClusterStatus{}, fault.ErrUnsupported
	}
	views, _ := queryScopedTopology(s)
	status := topology.ClusterStatus{LatestVersion: views.Latest.Version, TargetVersion: views.Target.Version,
		FullyConverged: views.FullyConverged, Cleanup: s.cleanupSummary()}
	var err error
	status.Placement, err = s.statusPlacement()
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	status.Observation = topology.HealthObservation{Version: s.Target.Version,
		Groups: topology.EvaluateAllGroupHealth(s.Target, nil)}
	groups, nodes := s.progressNames()
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		status.Groups = append(status.Groups, s.groupProgress(id).Clone())
	}
	for _, name := range slices.Sorted(maps.Keys(nodes)) {
		row := s.nodeProgress(name).Clone()
		if row.Approved == nil && row.Converged == nil {
			continue // Staging or retained cleanup alone does not admit a node.
		}
		declaration := row.Desired
		if declaration == nil {
			declaration = row.Approved
		}
		if declaration == nil {
			declaration = row.Converged
		}
		status.Nodes = append(status.Nodes, topology.NodeStatus{Node: *declaration,
			EffectiveState: s.progressNodeState(row), Health: topology.HealthUnknown})
	}
	status.Operations, status.OperationsTruncated = s.operationHeaders()
	return status, nil
}

// Static placement uses each proven group's pinned declaration document. A
// newer approval must not relabel an older checkpoint with newer node labels.
func (s *scopedState) statusPlacement() ([]topology.PlacementReport, error) {
	groups := make(map[uint64][]string)
	for _, row := range s.Groups {
		if row.Declaration != nil && row.SourceVersion != s.Target.Version {
			groups[row.SourceVersion] = append(groups[row.SourceVersion], row.GroupID)
		}
	}
	for _, group := range s.Target.Groups {
		groups[s.Target.Version] = append(groups[s.Target.Version], group.ID)
	}
	var reports []topology.PlacementReport
	for _, version := range slices.Sorted(maps.Keys(groups)) {
		doc, found := s.indexes.documents[version]
		if !found {
			return nil, fault.ErrUnreadable
		}
		report := topology.PlacementReport{Version: version, ApprovedVersion: s.Target.Version, Source: "converged"}
		if version == s.Target.Version {
			report.Source = "approved"
		}
		slices.Sort(groups[version])
		for _, id := range groups[version] {
			group, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: doc, Desired: doc, GroupID: id})
			if err != nil {
				return nil, err
			}
			report.Groups = append(report.Groups, group)
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func (s *scopedState) progressNodeState(row topology.NodeProgress) topology.NodeState {
	if row.Converged == nil {
		return row.Approved.State
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

// Borrowed under the Machine read lock. Query
// marshals the pair before releasing that lock; direct owned reads must clone.
func queryScopedTopology(s *scopedState) (topology.ReadView, error) {
	if s == nil {
		return topology.ReadView{}, fault.ErrUnsupported
	}
	return topology.ReadView{AppliedIndex: s.AppliedIndex, Latest: s.Latest, Target: s.Target,
		FullyConverged: s.FullyConverged}, nil
}
