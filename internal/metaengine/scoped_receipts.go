package metaengine

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyRecordEffect(index uint64, p RecordEffectProposal) (proposalApplication, error) {
	return s.recordScopedReceipts(index, RecordReadmissionReceiptsProposal{
		Receipts: []topology.EffectReceipt{p.Receipt}, At: p.At, ProposalID: p.ProposalID})
}

func (s *scopedState) applyRecordReadmissionReceipts(index uint64,
	p RecordReadmissionReceiptsProposal) (proposalApplication, error) {
	position := slices.IndexFunc(s.Admissions, func(a AdmissionRecord) bool {
		return a.NodeName == p.Admission.NodeName && a.InstanceID == p.Admission.InstanceID &&
			a.MemberID == p.Admission.MemberID && a.OperatorID == p.Admission.OperatorID
	})
	if position < 0 || len(p.Receipts) == 0 {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	for _, receipt := range p.Receipts {
		record := s.indexes.grants[receipt.Key]
		if record == nil || record.Grant.Purpose != topology.AuthorityReadmit ||
			record.Grant.AdmissionID != ReadmissionID(s.Admissions[position]) {
			return proposalApplication{}, fault.ErrOperationAuthorization
		}
	}
	return s.recordScopedReceipts(index, p)
}

func (s *scopedState) recordScopedReceipts(index uint64,
	p RecordReadmissionReceiptsProposal) (proposalApplication, error) {
	if index <= s.AppliedIndex || p.At.IsZero() {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	catalog := s.authorityCatalog().clone()
	changed := false
	for _, receipt := range p.Receipts {
		result, updated := catalog.recordEffect(RecordEffectProposal{Receipt: receipt, ProposalID: p.ProposalID})
		if !result.Accepted {
			return proposalApplication{result: result}, nil
		}
		changed = changed || updated
	}
	result := proposalApplication{result: ApplyResult{Accepted: true, ProposalID: p.ProposalID}, changed: changed}
	if !changed {
		return result, nil
	}
	next := s.cloneExecutionState()
	next.AppliedIndex = index
	update := scopedRevocation{state: next, catalog: catalog, requested: make(map[blockRef]*scopedRevocationRecord)}
	update.copyRevocations()
	update.publish()
	var err error
	next.indexes, err = next.buildIndexes()
	if err != nil {
		return proposalApplication{}, err
	}
	if err := next.consumeCleanupReceipts(p); err != nil {
		return proposalApplication{}, err
	}
	next.indexes, err = prepareScopedGraph(next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = *next
	return result, nil
}

func (s *scopedState) consumeCleanupReceipts(p RecordReadmissionReceiptsProposal) error {
	for _, receipt := range p.Receipts {
		if err := s.recordCleanupEffect(receipt, p.At); err != nil {
			return err
		}
	}
	if err := s.settleReadmissionCleanup(p.Receipts, p.At); err != nil {
		return err
	}
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		return err
	}
	s.completeWaitingDeletions(p.At, s.receiptDeletionGroups(p.Receipts))
	return nil
}

// Only the exact touched debt/batch may trigger opportunistic completion. The
// final receipt of a readmission batch can unblock deletions in its other groups.
func (s *scopedState) receiptDeletionGroups(receipts []topology.EffectReceipt) []string {
	groups, batches := make(map[string]bool), make(map[string]bool)
	for _, receipt := range receipts {
		groups[receipt.Key.GroupID] = true
		if record := s.indexes.grants[receipt.Key]; record != nil && record.Grant.Purpose == topology.AuthorityReadmit {
			batches[record.Grant.AdmissionID] = true
		}
	}
	for _, record := range s.AuthorityRecords {
		if record.Grant.Purpose == topology.AuthorityReadmit && batches[record.Grant.AdmissionID] {
			groups[record.Grant.Key.GroupID] = true
		}
	}
	return slices.Sorted(maps.Keys(groups))
}

// The receipt is already validated on this owned candidate. A completion error
// is a scoped diagnostic, not permission to discard that durable fact. The caller
// still validates the entire resulting graph before it can publish anything.
func (s *scopedState) completeWaitingDeletions(at time.Time, groups []string) {
	for _, id := range groups {
		group := s.indexes.groups[id]
		if group == nil {
			continue
		}
		t, ready := s.waitingDeletionTransition(*group, at)
		if !ready {
			continue
		}
		next, err := s.completeGroupLocked(s.AppliedIndex, t)
		if err == nil {
			next.indexes, err = next.buildIndexes()
		}
		if err != nil {
			s.blockDeletionCompletion(t, err)
			continue
		}
		*s = *next
	}
}

func (s *scopedState) blockDeletionCompletion(t topology.OperationTransition, err error) {
	if errors.Is(err, fault.ErrUnavailable) || errors.Is(err, fault.ErrAuthorityPending) {
		return
	}
	op, position := s.transitionBlock(t)
	blocked := &topology.BlockedReason{Code: "cleanup_failed", Message: err.Error()}
	if op.Status.Blocks[position].State == topology.OperationWaitingForReplica &&
		reflect.DeepEqual(op.Status.Blocks[position].Blocked, blocked) {
		return
	}
	t.State, t.Blocked = topology.OperationWaitingForReplica, blocked
	op.Status.Blocks[position] = scopedTransitionStatus(op.Status.Blocks[position].TotalSteps, t)
	op.Status.State, op.UpdatedAt = aggregateBlockStates(op.Status.Blocks), t.At
}

func (s *scopedState) waitingDeletionTransition(group groupConvergence,
	at time.Time) (topology.OperationTransition, bool) {
	op := s.indexes.operations[group.Owner]
	if !group.Deleting || op == nil {
		return topology.OperationTransition{}, false
	}
	position := slices.IndexFunc(op.Plan.Blocks, func(b topology.PlanBlock) bool { return b.GroupID == group.GroupID })
	if position < 0 || len(op.Plan.Blocks[position].Steps) != 0 {
		return topology.OperationTransition{}, false
	}
	stamp := latestTimestamp(at, group.CompletedAt, op.UpdatedAt)
	for _, debt := range s.Cleanup {
		if debt.Identity.GroupID == group.GroupID {
			stamp = latestTimestamp(stamp, debt.UpdatedAt)
		}
	}
	return topology.OperationTransition{OperationID: op.Status.ID, Version: op.Status.Version,
		Attempt: op.Status.Attempt, GroupID: group.GroupID, State: topology.OperationSucceeded, At: stamp}, true
}
