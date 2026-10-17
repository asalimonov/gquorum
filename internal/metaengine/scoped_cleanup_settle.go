package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// A partial batch remains execution evidence only. Its last receipt retires
// every exact former target atomically, including targets omitted by deletion.
func (s *scopedState) settleReadmissionCleanup(receipts []topology.EffectReceipt, at time.Time) error {
	catalog := s.authorityCatalog()
	batches := make(map[string]bool)
	for _, receipt := range receipts {
		grant := catalog.records[receipt.Key].Grant
		if grant.Purpose == topology.AuthorityReadmit {
			batches[grant.AdmissionID] = true
		}
	}
	for _, record := range catalog.snapshotRecords() {
		if !batches[record.Grant.AdmissionID] || record.Grant.Purpose != topology.AuthorityReadmit ||
			!catalog.readmissionBatchConsumed(record.Grant.AdmissionID) {
			continue
		}
		origin := s.readmissionOwner(record.Grant.AdmissionID)
		if s.indexes.operations[origin] == nil {
			return fault.ErrOperationAuthorization
		}
		for _, receipt := range record.Effects {
			if err := s.retireReadmissionTarget(origin, receipt, at); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *scopedState) retireReadmissionTarget(origin topology.OperationID,
	receipt topology.EffectReceipt, at time.Time) error {
	id := storage.Identity{ClusterID: receipt.Key.ClusterID, GroupID: receipt.Key.GroupID,
		NodeName: receipt.Target.NodeName, ReplicaID: receipt.Target.ReplicaID}
	position := slices.IndexFunc(s.Cleanup, func(row cleanupRecord) bool { return row.Identity == id })
	created := latestTimestamp(at, s.indexes.operations[origin].UpdatedAt)
	debt := cleanupRecord{Identity: id, MemberID: receipt.Target.MemberID, OperationID: origin,
		CreatedAt: created, UpdatedAt: created}
	if position >= 0 {
		debt = s.Cleanup[position].clone()
		if debt.MemberID != receipt.Target.MemberID {
			return fault.ErrIdentityMismatch
		}
		if debt.Retired != nil {
			return nil // Repeated batches cannot replace earlier exact retirement/collection proof.
		}
	}
	class, err := s.formerReplicaStorage(id, debt.MemberID)
	if err != nil {
		return err
	}
	if position >= 0 && debt.Storage != class {
		return fault.ErrIdentityMismatch
	}
	debt.Storage = class
	stamp := latestTimestamp(at, debt.CreatedAt, debt.UpdatedAt)
	debt.ReadmissionKey, debt.Retired, debt.RetiredAt = &receipt.Key, cloneCleanupReceipt(&receipt), &stamp
	if debt.Stopped == nil {
		debt.Stopped, debt.StoppedAt = cloneCleanupReceipt(&receipt), &stamp
	}
	debt.UpdatedAt = stamp
	return s.upsertCleanupLocked(debt)
}
