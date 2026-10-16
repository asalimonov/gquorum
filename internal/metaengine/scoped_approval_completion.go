package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func prepareApprovalCompletion(source, next *scopedState) error {
	next.History = make([]topology.Document, 0, len(source.History)+1)
	for _, doc := range source.History {
		next.History = append(next.History, doc.Clone())
	}
	if source.Target.Version != 0 && !slices.ContainsFunc(next.History, func(doc topology.Document) bool {
		return doc.Version == source.Target.Version
	}) {
		next.History = append(next.History, source.Target.Clone())
	}
	proof, err := next.buildIndexes()
	if err != nil {
		return err
	}
	// Approval's prospective checkpoints never prove their own omitted work.
	proof.groups = source.indexes.groups
	catalog, err := restoreScopedAuthority(source, source.indexes)
	if err != nil {
		return err
	}
	op := &next.Operations[len(next.Operations)-1]
	for i, block := range op.Plan.Blocks {
		if len(block.Steps) != 0 || next.Target.Group(block.GroupID) == nil {
			continue
		}
		if !zeroStepMembership(source, next.Target, block.GroupID, catalog) {
			continue
		}
		if block.GroupID == topology.MetaGroupID && !completeApprovalAdmissions(next, op, proof, catalog) {
			continue
		}
		next.completeApprovalGroup(block.GroupID, op)
		op.Status.Blocks[i].State = topology.OperationSucceeded
	}
	op.Status.State = aggregateBlockStates(op.Status.Blocks)
	if err := prepareApprovalNodes(source, next, op); err != nil {
		return err
	}
	next.updateFullyConverged(op.UpdatedAt)
	return nil
}

func (s *scopedState) completeApprovalGroup(group string, op *topology.Operation) {
	for i := range s.Groups {
		if s.Groups[i].GroupID == group {
			declaration := (topology.GroupProgress{Converged: s.Target.Group(group)}).Clone().Converged
			s.Groups[i] = groupConvergence{GroupID: group, Declaration: declaration,
				SourceVersion: op.Status.Version, CompletedAt: op.UpdatedAt}
			return
		}
	}
}

func completeApprovalAdmissions(next *scopedState, op *topology.Operation,
	proof *scopedIndexes, catalog authorityCatalog) bool {
	for _, a := range next.Admissions {
		if a.State.terminal() || topology.OperationID(a.OperatorID) != op.Status.ID {
			continue
		}
		progress, valid := scopedAdmissionProgress(a, op, proof, catalog)
		if !valid || !progress.prepared || !a.State.isPrepared() ||
			!scopedAdmissionRetirement(a, next, proof, catalog) {
			return false
		}
	}
	next.Admissions = slices.Clone(next.Admissions)
	for i := range next.Admissions {
		a := &next.Admissions[i]
		if !a.State.terminal() && topology.OperationID(a.OperatorID) == op.Status.ID {
			a.State = AdmissionCompleted
		}
	}
	return true
}

// An empty plan cannot manufacture membership from approved declarations.
// Reuse proven membership and exact consumed effects after that checkpoint.
func zeroStepMembership(source *scopedState, desired topology.Document, group string, catalog authorityCatalog) bool {
	members := make(map[uint64]topology.Replica)
	prepared := make(map[uint64]bool)
	version := checkpointMembership(source.indexes.groups[group], members, prepared)
	for _, record := range catalog.snapshotRecords() {
		a := record.Grant.Action
		if a == nil || a.GroupID != group || a.Version <= version || !membershipProofKind(a.Kind) {
			continue
		}
		if slices.ContainsFunc(record.Grant.Resources, func(r topology.AuthorityResource) bool {
			return !resourceConsumed(&record, r)
		}) {
			return false
		}
		approved := source.indexes.documents[a.Version]
		applyMembershipProof(a, approved, members, prepared)
	}
	want := desired.Group(group)
	if want == nil || len(members) != len(want.Replicas) {
		return false
	}
	for _, r := range want.Replicas {
		actual := members[r.ReplicaID]
		if actual.NodeName != r.NodeName || actual.Role != r.Role || !prepared[r.ReplicaID] {
			return false
		}
	}
	return true
}

func checkpointMembership(row *groupConvergence, members map[uint64]topology.Replica,
	prepared map[uint64]bool) uint64 {
	if row == nil {
		return 0
	}
	if row.Declaration != nil {
		for _, r := range row.Declaration.Replicas {
			members[r.ReplicaID], prepared[r.ReplicaID] = r, true
		}
	}
	return row.SourceVersion
}

func membershipProofKind(kind topology.StepKind) bool {
	return kind == topology.StepRemoveReplica || kind == topology.StepCreateFoundingGroup || admissionPrefixKind(kind)
}

func applyMembershipProof(a *topology.AuthorizedStep, approved topology.Document,
	members map[uint64]topology.Replica, prepared map[uint64]bool) {
	r := members[a.ReplicaID]
	switch a.Kind {
	case topology.StepPrepareReplica:
		prepared[a.ReplicaID] = true
	case topology.StepCreateFoundingGroup:
		for _, founder := range approved.VotersOf(a.GroupID) {
			members[founder.ReplicaID], prepared[founder.ReplicaID] = founder, true
		}
	case topology.StepAddLearner:
		members[a.ReplicaID] = topology.Replica{
			NodeName: a.NodeName, ReplicaID: a.ReplicaID, Role: topology.ReplicaLearner}
	case topology.StepPromoteVoter:
		r.Role = topology.ReplicaVoter
		members[a.ReplicaID] = r
	case topology.StepDemoteVoter:
		r.Role = topology.ReplicaLearner
		members[a.ReplicaID] = r
	case topology.StepRemoveReplica:
		delete(members, a.ReplicaID)
	default: // The caller selects only membership/preparation effect kinds.
	}
}
