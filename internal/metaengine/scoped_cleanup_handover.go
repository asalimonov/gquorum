package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type cleanupHandover struct {
	source    *scopedState
	desired   topology.Document
	collector planningCollector
	at        time.Time
	required  map[storage.Identity]topology.AuthorizedStep
}

func prepareCleanupHandovers(source, next *scopedState, collector planningCollector) error {
	operation := next.Operations[len(next.Operations)-1]
	h := cleanupHandover{source: source, desired: next.Target, collector: collector, at: operation.UpdatedAt,
		required: make(map[storage.Identity]topology.AuthorizedStep)}
	groups := make(map[string]bool, len(operation.Plan.Blocks))
	for _, block := range operation.Plan.Blocks {
		groups[block.GroupID] = true
		for _, seq := range block.Steps {
			step := operation.Plan.Steps[seq-1]
			if step.Kind != topology.StepStopReplica {
				continue
			}
			id := storage.Identity{ClusterID: next.Target.ClusterID, GroupID: step.GroupID,
				NodeName: step.NodeName, ReplicaID: step.ReplicaID}
			h.required[id] = topology.AuthorizedStep{OperatorID: topology.OperatorID(operation.Status.ID),
				Version: operation.Status.Version, Attempt: operation.Status.Attempt, StepSeq: seq,
				Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}
		}
	}
	debts := slices.Clone(source.Cleanup)
	for i, debt := range debts {
		if !groups[debt.Identity.GroupID] || debt.Stopped != nil || debt.StopAction == nil {
			continue
		}
		var err error
		debts[i], err = h.prepare(debt)
		if err != nil {
			return err
		}
	}
	next.Cleanup = debts
	return nil
}

func (h cleanupHandover) prepare(debt cleanupRecord) (cleanupRecord, error) {
	bound := h.source.indexes.operations[topology.OperationID(debt.StopAction.OperatorID)]
	if !requiredCleanupStop(bound, *debt.StopAction) {
		return debt, nil // Unconsumed detached work retains its original approval.
	}
	if action, found := h.required[debt.Identity]; found {
		if h.at.Before(debt.UpdatedAt) || h.desired.MemberID(debt.Identity.NodeName) > debt.MemberID {
			return cleanupRecord{}, fault.ErrOperationAuthorization
		}
		debt.StopAction, debt.UpdatedAt = &action, h.at
		return debt, nil
	}
	receipt := h.collector.readmissionStopReceipt(debt, h.desired)
	if receipt == nil || h.at.Before(debt.UpdatedAt) {
		return cleanupRecord{}, fault.ErrOperationAuthorization
	}
	// Readmission owns this exact former host. Keep the old actions as proof;
	// never send a successor Stop to the now-unroutable predecessor process.
	debt.ReadmissionKey, debt.Stopped, debt.Retired = &receipt.Key, receipt, receipt
	debt.StoppedAt, debt.RetiredAt, debt.UpdatedAt = &h.at, &h.at, h.at
	return debt, nil
}

func (c planningCollector) readmissionStopReceipt(debt cleanupRecord,
	desired topology.Document) *topology.EffectReceipt {
	member := desired.MemberID(debt.Identity.NodeName)
	if member <= debt.MemberID {
		return nil
	}
	want := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	var selected *topology.EffectReceipt
	for _, record := range c.evidence.authority.records {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityReadmit || grant.Key.GroupID != debt.Identity.GroupID ||
			!slices.ContainsFunc(c.evidence.admissions, func(a AdmissionRecord) bool {
				return a.MemberID == member && a.NodeName == debt.Identity.NodeName &&
					ReadmissionID(a) == grant.AdmissionID
			}) || !c.evidence.authority.readmissionBatchConsumed(grant.AdmissionID) {
			continue
		}
		for _, receipt := range record.Effects {
			if receipt.Target == want && receipt.Resource.Kind == topology.AuthorityHost &&
				(selected == nil || compareAuthorityKey(receipt.Key, selected.Key) < 0) {
				owned := receipt.Clone()
				selected = &owned
			}
		}
	}
	return selected
}
