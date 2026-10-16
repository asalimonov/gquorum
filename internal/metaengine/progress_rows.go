package metaengine

import "github.com/asalimonov/gquorum/topology"

// Declarations here are borrowed under the FSM lock. Public projections clone
// them after selection; Status and ReadProgress share this complete row builder.
func (s *scopedState) groupProgress(id string) topology.GroupProgress {
	p := topology.GroupProgress{GroupID: id, Desired: s.Latest.Group(id), Approved: s.Target.Group(id)}
	if row := s.indexes.groups[id]; row != nil {
		p.Converged, p.ConvergedVersion, p.OperationID = row.Declaration, row.SourceVersion, row.Owner
		p.Deleting = row.Deleting
		p.Blocked = s.progressBlocker(row.Owner, id)
	}
	return p
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
	if op := s.indexes.operations[owner]; op != nil {
		for _, block := range op.Status.Blocks {
			if block.GroupID == group {
				return block.Blocked
			}
		}
	}
	return nil
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
