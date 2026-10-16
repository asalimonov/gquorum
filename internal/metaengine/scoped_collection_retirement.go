package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (check scopedCleanupValidation) collection(debt cleanupRecord) bool {
	return validScopedCollection(debt) && (debt.Collection == nil ||
		check.collectionRetirement(debt, debt.Collection.RetirementKey) == nil)
}

// The milestone keeps the first delivered revision; a host cannot know that
// delivery order. Any retained exact retirement revision may bind collection.
// Missing effect delivery is retryable, but never substitutes for consumption.
func (check scopedCleanupValidation) collectionRetirement(debt cleanupRecord, key topology.AuthorityKey) error {
	record := check.index.grants[key]
	target := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	if record == nil || key.ClusterID != debt.Identity.ClusterID || key.GroupID != debt.Identity.GroupID ||
		!slices.Contains(record.Grant.Targets, target) || !check.retirementRevision(debt, record) {
		return fault.ErrOperationAuthorization
	}
	for i := range record.Effects {
		if debt.Retired != nil && check.receipt(debt, &record.Effects[i], topology.AuthorityHost) {
			return nil
		}
	}
	return fault.ErrUnavailable
}

func (check scopedCleanupValidation) retirementRevision(debt cleanupRecord, record *scopedAuthorityRecord) bool {
	grant := record.Grant
	if grant.Purpose == topology.AuthorityReadmit {
		return debt.ReadmissionKey != nil && *debt.ReadmissionKey == grant.Key && check.readmission(debt)
	}
	return debt.Retired != nil && debt.Retired.Key == grant.Key ||
		record.Class == "cleanup" && grant.Action != nil && debt.RetireAction != nil &&
			*grant.Action == *debt.RetireAction
}
