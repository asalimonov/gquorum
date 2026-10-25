package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyRecoveryGroupConsumed(index uint64,
	p model.GroupConsumedProposal) (proposalApplication, error) {
	if s.Recovery == nil || p.RecoveryID != s.Recovery.RecoveryID ||
		p.PlanDigest != s.Recovery.PlanDigest || p.ProposalID == "" {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	position := -1
	for i, group := range s.Recovery.Groups {
		if group.GroupID == p.GroupID {
			position = i
			break
		}
	}
	if position < 0 {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	group := s.Recovery.Groups[position]
	if group.Consumed != nil {
		if group.Consumed.Identity != p.Checkpoint.Identity || group.Consumed.Checkpoint != p.Checkpoint.Position ||
			group.Consumed.SnapshotDigest != p.Checkpoint.Digest {
			return proposalApplication{}, fault.ErrConflict
		}
		return proposalApplication{result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}}, nil
	}
	if s.Recovery.Phase != "importing" {
		return proposalApplication{}, fault.ErrConflict
	}
	evidence, err := s.recoveryEvidence(group, p.Checkpoint)
	if err != nil {
		return proposalApplication{}, err
	}
	owned := model.CloneImport(*s.Recovery)
	owned.Groups[position].Consumed = &evidence
	next := *s
	next.Recovery = &owned
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}}, nil
}

func (s *scopedState) applyCompleteRecoveryImport(index uint64,
	p model.CompleteImportProposal) (proposalApplication, error) {
	if s.Recovery == nil || p.RecoveryID != s.Recovery.RecoveryID ||
		p.PlanDigest != s.Recovery.PlanDigest || p.ProposalID == "" || p.At.IsZero() {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	if s.Recovery.Phase == "complete" {
		if s.Recovery.CompletedAt == nil || !s.Recovery.CompletedAt.Equal(p.At) {
			return proposalApplication{}, fault.ErrConflict
		}
		return proposalApplication{result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}}, nil
	}
	for _, group := range s.Recovery.Groups {
		if group.Consumed == nil {
			return proposalApplication{}, fault.ErrUnavailable
		}
	}
	op := s.indexes.operations[s.indexes.latest[s.Recovery.Version]]
	if op == nil || op.Status.State != topology.OperationSucceeded {
		return proposalApplication{}, fault.ErrUnavailable
	}
	owned := model.CloneImport(*s.Recovery)
	owned.Phase, owned.CompletedAt = "complete", &p.At
	next := *s
	next.Recovery = &owned
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}}, nil
}
