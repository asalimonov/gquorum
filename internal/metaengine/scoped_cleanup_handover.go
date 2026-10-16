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
	detached  map[storage.Identity]topology.AuthorizedStep
}

func prepareCleanupHandovers(source, next *scopedState, collector planningCollector) error {
	operation := next.Operations[len(next.Operations)-1]
	h := cleanupHandover{source: source, desired: next.Target, collector: collector, at: operation.UpdatedAt,
		required: make(map[storage.Identity]topology.AuthorizedStep),
		detached: make(map[storage.Identity]topology.AuthorizedStep)}
	groups := make(map[string]bool, len(operation.Plan.Blocks))
	for _, block := range operation.Plan.Blocks {
		groups[block.GroupID] = true
		for _, seq := range append(slices.Clone(block.Steps), block.Cleanup...) {
			step := operation.Plan.Steps[seq-1]
			if step.Kind != topology.StepStopReplica {
				continue
			}
			id := storage.Identity{ClusterID: next.Target.ClusterID, GroupID: step.GroupID,
				NodeName: step.NodeName, ReplicaID: step.ReplicaID}
			if slices.Contains(block.Steps, seq) {
				h.required[id] = scopedPlanAction(&operation, step)
			} else {
				h.detached[id] = scopedPlanAction(&operation, step)
			}
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
	required := requiredCleanupStop(bound, *debt.StopAction)
	action, found := h.required[debt.Identity]
	if detached, ok := h.detached[debt.Identity]; ok {
		if !required || !detachedCleanupAllowed(h.desired, debt.Identity) {
			return cleanupRecord{}, fault.ErrOperationAuthorization
		}
		action, found = detached, true
	}
	if found {
		if h.at.Before(debt.UpdatedAt) || h.desired.MemberID(debt.Identity.NodeName) > debt.MemberID {
			return cleanupRecord{}, fault.ErrOperationAuthorization
		}
		debt.StopAction, debt.UpdatedAt = &action, h.at
		return debt, nil
	}
	if !required {
		return debt, nil // Unchanged detached work keeps its exact bound approval.
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

func detachedCleanupAllowed(desired topology.Document, id storage.Identity) bool {
	group := desired.Group(id.GroupID)
	return group != nil && !slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
		return r.NodeName == id.NodeName && r.ReplicaID != id.ReplicaID
	})
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
