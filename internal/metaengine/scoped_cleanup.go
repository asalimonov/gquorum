package metaengine

import (
	"reflect"
	"slices"
	"strings"
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
		if !check.declaration(debt) || !check.actions(debt) || !check.milestones(debt) || !validScopedCollection(debt) {
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
	if debt.ReadmissionKey != nil {
		return check.readmission(debt)
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
	if debt.ReadmissionKey != nil {
		return debt.Removed == nil && debt.Stopped != nil && debt.Retired != nil &&
			debt.Stopped.Key == *debt.ReadmissionKey && reflect.DeepEqual(debt.Stopped, debt.Retired)
	}
	if debt.Removed == nil {
		if debt.RetireAction.Kind != topology.StepDeleteGroupState {
			return false
		}
	} else if debt.Removed.Action == nil || debt.Removed.Action.Kind != topology.StepRemoveReplica ||
		!check.action(debt, *debt.Removed.Action) {
		return false
	}
	return (debt.Retired == nil || debt.Stopped != nil && !debt.RetiredAt.Before(*debt.StoppedAt)) &&
		(debt.Removed == nil || debt.Stopped == nil || !debt.StoppedAt.Before(*debt.RemovedAt)) &&
		cleanupEffectMatches(debt.Stopped, debt.StopAction) && cleanupEffectMatches(debt.Retired, debt.RetireAction)
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

func (check scopedCleanupValidation) readmission(debt cleanupRecord) bool {
	record := check.catalog.records[*debt.ReadmissionKey]
	if debt.StopAction != nil || debt.RetireAction != nil || record == nil ||
		record.Grant.Purpose != topology.AuthorityReadmit {
		return false
	}
	admissionID := record.Grant.AdmissionID
	if !slices.ContainsFunc(check.state.Admissions, func(admission AdmissionRecord) bool {
		return ReadmissionID(admission) == admissionID && topology.OperationID(admission.OperatorID) == debt.OperationID
	}) {
		return false
	}
	meta := false
	for _, retained := range check.catalog.records {
		if retained.Grant.Purpose != topology.AuthorityReadmit || retained.Grant.AdmissionID != admissionID {
			continue
		}
		if !resourceConsumed(retained, retained.Grant.Resources[0]) {
			return false
		}
		meta = meta || retained.Grant.Key.GroupID == topology.MetaGroupID
	}
	return meta
}

func validScopedCollection(debt cleanupRecord) bool {
	rc := debt.Collection
	if rc == nil {
		return true
	}
	// Collection has its own bucket sequence and host clock. Neither can be
	// ordered against effect-journal sequences or metadata milestone timestamps.
	return debt.Storage != topology.StorageMemory && debt.Retired != nil && rc.LocalSequence != 0 &&
		rc.Identity == debt.Identity && rc.MemberID == debt.MemberID && rc.RetirementKey == debt.Retired.Key &&
		strings.TrimSpace(rc.Token) != "" && !strings.ContainsRune(rc.Token, 0) && !rc.CollectedAt.IsZero()
}
