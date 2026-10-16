package metaengine

import (
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Upserts an owned preparation candidate. Its caller rebuilds indexes and
// validates the complete grant/debt graph before publishing the enclosing Apply.
// Action rebinding belongs to approval/retry, never to a receipt upsert.
func (s *scopedState) upsertCleanupLocked(incoming cleanupRecord) error {
	if !validCleanupRecordShape(incoming) {
		return fault.ErrOperationAuthorization
	}
	position := slices.IndexFunc(s.Cleanup, func(row cleanupRecord) bool { return row.Identity == incoming.Identity })
	next := incoming.clone()
	if position >= 0 {
		var err error
		next, err = mergeCleanupRecord(s.Cleanup[position], incoming)
		if err != nil {
			return err
		}
	}
	rows := slices.Clone(s.Cleanup)
	if position < 0 {
		rows = append(rows, next)
	} else {
		rows[position] = next
	}
	s.Cleanup = rows
	return nil
}

func validCleanupRecordShape(row cleanupRecord) bool {
	return row.Identity.ClusterID != "" && row.Identity.NodeName != "" && row.Identity.ReplicaID != 0 &&
		row.MemberID != 0 && row.OperationID != "" && !row.CreatedAt.IsZero() && !row.UpdatedAt.Before(row.CreatedAt) &&
		topology.ValidateID(row.Identity.GroupID) == nil && topology.ValidateStorageClass(row.Storage) == nil &&
		cleanupTimestamp(row.Removed, row.RemovedAt, row) && cleanupTimestamp(row.Stopped, row.StoppedAt, row) &&
		cleanupTimestamp(row.Retired, row.RetiredAt, row) && validScopedCollection(row)
}

func mergeCleanupRecord(old, incoming cleanupRecord) (cleanupRecord, error) {
	if old.Identity != incoming.Identity || old.Storage != incoming.Storage || old.MemberID != incoming.MemberID ||
		old.OperationID != incoming.OperationID || !compatibleCleanupValue(old.StopAction, incoming.StopAction) ||
		!compatibleCleanupValue(old.RetireAction, incoming.RetireAction) ||
		old.ReadmissionKey != nil && !compatibleCleanupValue(old.ReadmissionKey, incoming.ReadmissionKey) ||
		old.Collection != nil && !compatibleCleanupValue(old.Collection, incoming.Collection) {
		return cleanupRecord{}, fault.ErrIdentityMismatch
	}
	next := old.clone()
	if err := mergeCleanupMilestone(&next.Removed, &next.RemovedAt, incoming.Removed, incoming.RemovedAt); err != nil {
		return cleanupRecord{}, err
	}
	if err := mergeCleanupMilestone(&next.Stopped, &next.StoppedAt, incoming.Stopped, incoming.StoppedAt); err != nil {
		return cleanupRecord{}, err
	}
	if err := mergeCleanupMilestone(&next.Retired, &next.RetiredAt, incoming.Retired, incoming.RetiredAt); err != nil {
		return cleanupRecord{}, err
	}
	if next.ReadmissionKey == nil {
		next.ReadmissionKey = copyCleanupValue(incoming.ReadmissionKey)
	}
	if next.Collection == nil {
		next.Collection = copyCleanupValue(incoming.Collection)
	}
	if reflect.DeepEqual(next, old) {
		return next, nil
	}
	next.UpdatedAt = latestTimestamp(old.UpdatedAt, incoming.UpdatedAt)
	if !validCleanupRecordShape(next) {
		return cleanupRecord{}, fault.ErrOperationAuthorization
	}
	return next, nil
}

func compatibleCleanupValue[T any](old, incoming *T) bool {
	return incoming == nil || reflect.DeepEqual(old, incoming)
}

func mergeCleanupMilestone(into **topology.EffectReceipt, stamp **time.Time,
	receipt *topology.EffectReceipt, at *time.Time) error {
	if receipt == nil {
		return nil
	}
	if *into != nil {
		if !reflect.DeepEqual(*into, receipt) {
			return fault.ErrOperationAuthorization
		}
		return nil // Receipt replay cannot move the original milestone timestamp.
	}
	*into, *stamp = cloneCleanupReceipt(receipt), copyCleanupValue(at)
	return nil
}
