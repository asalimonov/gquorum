package metaengine

import (
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type scopedCleanupValidation struct {
	state   *scopedState
	index   *scopedIndexes
	catalog authorityCatalog
}

func validateScopedCleanup(state *scopedState, index *scopedIndexes, catalog authorityCatalog) error {
	check := scopedCleanupValidation{state, index, catalog}
	for _, debt := range state.Cleanup {
		if !check.declaration(debt) || !check.actions(debt) || !check.milestones(debt) || !check.collection(debt) {
			return invalidScopedState("cleanup lacks exact declaration, action or milestone proof")
		}
	}
	return validateScopedCleanupEffects(index, catalog)
}

func (check scopedCleanupValidation) declaration(debt cleanupRecord) bool {
	if debt.Identity.ClusterID != check.state.Target.ClusterID || debt.MemberID == 0 ||
		check.index.operations[debt.OperationID] == nil ||
		topology.ValidateStorageClass(debt.Storage) != nil || debt.CreatedAt.IsZero() ||
		debt.UpdatedAt.Before(debt.CreatedAt) || debt.NotBefore != nil && debt.NotBefore.IsZero() {
		return false
	}
	for _, doc := range check.index.documents {
		if !scopedApprovedVersion(check.state, doc.Version) || doc.MemberID(debt.Identity.NodeName) != debt.MemberID {
			continue
		}
		group := doc.Group(debt.Identity.GroupID)
		replica := documentReplica(doc, debt.Identity.GroupID, debt.Identity.ReplicaID)
		if group != nil && replica != nil && replica.NodeName == debt.Identity.NodeName &&
			group.Storage == debt.Storage {
			return true
		}
	}
	return false
}

func (check scopedCleanupValidation) actions(debt cleanupRecord) bool {
	if debt.ReadmissionKey != nil && !check.readmission(debt, *debt.ReadmissionKey) {
		return false
	}
	if debt.StopAction == nil && debt.RetireAction == nil {
		return debt.ReadmissionKey != nil && debt.Removed == nil
	}
	if debt.StopAction == nil || debt.RetireAction == nil || debt.StopAction.Kind != topology.StepStopReplica {
		return false
	}
	kind := debt.RetireAction.Kind
	if kind != topology.StepCleanupReplicaState && kind != topology.StepDeleteGroupState &&
		!(kind == topology.StepStopReplica && debt.Identity.GroupID == topology.MetaGroupID) {
		return false
	}
	return check.stopAction(debt) && check.action(debt, *debt.RetireAction)
}

func (check scopedCleanupValidation) action(debt cleanupRecord, action topology.AuthorizedStep) bool {
	if action.GroupID != debt.Identity.GroupID || action.Version == 0 || action.Version > check.state.Target.Version ||
		action.Attempt == 0 || action.StepSeq < 1 ||
		action.TransferNodeName != "" || action.TransferReplicaID != 0 || action.Kind != topology.StepRemoveReplica &&
		(action.ExpectedVotersDigest != "" || action.ResultVotersDigest != "") ||
		action.OperatorID != topology.MakeOperatorID(action.Version, action.Attempt) {
		return false
	}
	if action.Kind == topology.StepDeleteGroupState {
		if action.NodeName != "" || action.ReplicaID != 0 {
			return false
		}
	} else if action.NodeName != debt.Identity.NodeName || action.ReplicaID != debt.Identity.ReplicaID {
		return false
	}
	operation := check.index.operations[topology.OperationID(action.OperatorID)]
	if operation == nil {
		return false
	}
	return action.StepSeq <= len(operation.Plan.Steps) && topology.OperationID(action.OperatorID) == debt.OperationID &&
		action.Matches(topology.OperatorID(operation.Status.ID), operation.Status.Attempt,
			operation.Status.Version, operation.Plan.Steps[action.StepSeq-1])
}

func (check scopedCleanupValidation) receipt(debt cleanupRecord, receipt *topology.EffectReceipt,
	kind topology.AuthorityResourceKind) bool {
	if receipt == nil {
		return true
	}
	record := check.catalog.records[receipt.Key]
	want := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	return record != nil && receipt.Resource.Kind == kind && receipt.Key.GroupID == debt.Identity.GroupID &&
		receipt.Target == want && slices.ContainsFunc(record.Effects,
		func(retained topology.EffectReceipt) bool { return reflect.DeepEqual(retained, *receipt) })
}

func (check scopedCleanupValidation) milestones(debt cleanupRecord) bool {
	if !cleanupTimestamp(debt.Removed, debt.RemovedAt, debt) || !cleanupTimestamp(debt.Stopped, debt.StoppedAt, debt) ||
		!cleanupTimestamp(debt.Retired, debt.RetiredAt, debt) ||
		!check.receipt(debt, debt.Removed, topology.AuthorityGroup) ||
		!check.receipt(debt, debt.Stopped, topology.AuthorityHost) ||
		!check.receipt(debt, debt.Retired, topology.AuthorityHost) {
		return false
	}
	if !check.removal(debt) {
		return false
	}
	if debt.ReadmissionKey != nil {
		return readmissionMilestones(debt)
	}
	return (debt.Retired == nil || debt.Stopped != nil && !debt.RetiredAt.Before(*debt.StoppedAt)) &&
		(debt.Removed == nil || debt.Stopped == nil || !debt.StoppedAt.Before(*debt.RemovedAt)) &&
		cleanupStopMatches(debt) && cleanupEffectMatches(debt.Retired, debt.RetireAction)
}

func cleanupStopMatches(debt cleanupRecord) bool {
	return debt.Stopped == nil || debt.Stopped.Action != nil &&
		debt.Stopped.Action.Kind == topology.StepStopReplica && cleanupRetainsAction(&debt, *debt.Stopped.Action)
}

func cleanupTimestamp(receipt *topology.EffectReceipt, at *time.Time, debt cleanupRecord) bool {
	if receipt == nil || at == nil {
		return receipt == nil && at == nil
	}
	return !at.IsZero() && !at.Before(debt.CreatedAt) && !at.After(debt.UpdatedAt)
}

func cleanupEffectMatches(receipt *topology.EffectReceipt, action *topology.AuthorizedStep) bool {
	return receipt == nil || receipt.Action != nil && *receipt.Action == *action
}

func validScopedCollection(debt cleanupRecord) bool {
	rc := debt.Collection
	if rc == nil {
		return true
	}
	// Collection has its own bucket sequence and host clock. Neither can be
	// ordered against effect-journal sequences or metadata milestone timestamps.
	return debt.Storage != topology.StorageMemory && debt.Retired != nil && rc.LocalSequence != 0 &&
		rc.Identity == debt.Identity && rc.MemberID == debt.MemberID && rc.Validate() == nil
}
