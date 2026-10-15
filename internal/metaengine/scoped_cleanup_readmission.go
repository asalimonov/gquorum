package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func (check scopedCleanupValidation) readmission(debt cleanupRecord) bool {
	record := check.catalog.records[*debt.ReadmissionKey]
	if record == nil || record.Grant.Purpose != topology.AuthorityReadmit {
		return false
	}
	want := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	if !slices.Contains(record.Grant.Targets, want) {
		return false
	}
	admissionID := record.Grant.AdmissionID
	for _, admission := range check.state.Admissions {
		if admission.NodeName != debt.Identity.NodeName || ReadmissionID(admission) != admissionID {
			continue
		}
		id := topology.OperationID(admission.OperatorID)
		if check.index.operations[id] == nil || debt.StopAction == nil && id != debt.OperationID {
			return false
		}
		return check.catalog.readmissionBatchConsumed(admissionID)
	}
	return false
}

func (check scopedCleanupValidation) removal(debt cleanupRecord) bool {
	if debt.Removed == nil {
		return debt.StopAction == nil && debt.ReadmissionKey != nil ||
			debt.RetireAction != nil && debt.RetireAction.Kind == topology.StepDeleteGroupState
	}
	return debt.Removed.Action != nil && debt.Removed.Action.Kind == topology.StepRemoveReplica &&
		check.action(debt, *debt.Removed.Action)
}

func readmissionMilestone(debt *cleanupRecord, receipt *topology.EffectReceipt) bool {
	return debt.ReadmissionKey != nil && receipt != nil && receipt.Key == *debt.ReadmissionKey
}

func readmissionMilestones(debt cleanupRecord) bool {
	if debt.Stopped == nil || !readmissionMilestone(&debt, debt.Retired) ||
		debt.RetiredAt.Before(*debt.StoppedAt) {
		return false
	}
	if debt.Removed != nil && debt.StoppedAt.Before(*debt.RemovedAt) {
		return false
	}
	if readmissionMilestone(&debt, debt.Stopped) {
		return reflect.DeepEqual(debt.Stopped, debt.Retired)
	}
	return debt.StopAction != nil && cleanupEffectMatches(debt.Stopped, debt.StopAction)
}
