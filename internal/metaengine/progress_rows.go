package metaengine

import "github.com/asalimonov/gquorum/topology"

// Declarations here are borrowed under the FSM lock. Public projections clone
// them after selection; Status and ReadProgress share this complete row builder.
func (s *scopedState) groupProgress(id string) topology.GroupProgress {
	convergence := groupConvergence{GroupID: id}
	if row := s.indexes.groups[id]; row != nil {
		convergence = *row
	}
	p := declaredGroupProgress(s.Latest, s.Target, convergence)
	p.Blocked = s.progressBlocker(convergence.Owner, id)
	return p
}

func declaredGroupProgress(latest, target topology.Document, row groupConvergence) topology.GroupProgress {
	return topology.GroupProgress{GroupID: row.GroupID, Desired: latest.Group(row.GroupID),
		Approved: target.Group(row.GroupID), Converged: row.Declaration, ConvergedVersion: row.SourceVersion,
		OperationID: row.Owner, Deleting: row.Deleting}
}

func (s *scopedState) nodeProgress(name string) topology.NodeProgress {
	p := topology.NodeProgress{NodeName: name, Desired: s.Latest.Node(name), Approved: s.Target.Node(name)}
	if row := s.indexes.nodes[name]; row != nil {
		p.Converged, p.ConvergedVersion, p.OperationID = row.Declaration, row.SourceVersion, row.Owner
		p.MemberID, p.Blocked = row.MemberID, s.progressBlocker(row.Owner, topology.MetaGroupID)
	}
	return p
}

func (s *scopedState) progressBlocker(owner topology.OperationID, group string) *topology.BlockedReason {
	op := s.indexes.operations[owner]
	position := readyBlockPosition(op, group)
	if position < 0 {
		return nil
	}
	if reason := s.requiredBlocker(op, position); reason != nil {
		return reason
	}
	reason := op.Status.Blocks[position].Blocked
	if reason != nil && len(op.Plan.Blocks[position].Steps) != 0 &&
		(reason.Code == topology.BlockedDependency || reason.Code == topology.BlockedRevocationPending) {
		return nil // Derived prerequisites resolved; never preserve a stale poll cause.
	}
	return reason
}

func (r cleanupRecord) progress() topology.CleanupProgress {
	p := topology.CleanupProgress{Identity: r.Identity, Storage: r.Storage, MemberID: r.MemberID,
		OperationID: r.OperationID, RemovedAt: copyCleanupValue(r.RemovedAt), StoppedAt: copyCleanupValue(r.StoppedAt),
		RetiredAt: copyCleanupValue(r.RetiredAt), NotBefore: copyCleanupValue(r.NotBefore),
		Blocked: copyCleanupValue(r.Blocked), UpdatedAt: r.UpdatedAt}
	if r.Collection != nil {
		p.CollectedAt = copyCleanupValue(&r.Collection.CollectedAt)
	}
	return p
}
