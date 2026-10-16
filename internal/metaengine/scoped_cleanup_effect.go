package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) recordCleanupEffect(receipt topology.EffectReceipt, at time.Time) error {
	action := receipt.Action
	if action == nil {
		return nil // Readmission is settled only after its complete batch.
	}
	id := storage.Identity{ClusterID: receipt.Key.ClusterID, GroupID: receipt.Key.GroupID,
		NodeName: receipt.Target.NodeName, ReplicaID: receipt.Target.ReplicaID}
	position := slices.IndexFunc(s.Cleanup, func(row cleanupRecord) bool { return row.Identity == id })
	if action.Kind == topology.StepRemoveReplica {
		return s.recordRemovalCleanup(receipt, at, position)
	}
	if action.Kind != topology.StepStopReplica && action.Kind != topology.StepCleanupReplicaState &&
		action.Kind != topology.StepDeleteGroupState {
		return nil
	}
	if position < 0 || s.Cleanup[position].MemberID != receipt.Target.MemberID {
		return fault.ErrOperationAuthorization
	}
	debt := s.Cleanup[position].clone()
	if !cleanupRetainsAction(&debt, *action) {
		return fault.ErrOperationAuthorization
	}
	stamp := latestTimestamp(at, debt.CreatedAt, debt.UpdatedAt)
	if action.Kind == topology.StepStopReplica && debt.Stopped == nil {
		debt.Stopped, debt.StoppedAt = cloneCleanupReceipt(&receipt), &stamp
	}
	if debt.RetireAction != nil && *debt.RetireAction == *action && debt.Retired == nil {
		if debt.Stopped == nil {
			return fault.ErrAuthorityPending
		}
		debt.Retired, debt.RetiredAt = cloneCleanupReceipt(&receipt), &stamp
	}
	debt.UpdatedAt = stamp
	return s.upsertCleanupLocked(debt)
}

func (s *scopedState) recordRemovalCleanup(receipt topology.EffectReceipt, at time.Time, position int) error {
	if position < 0 {
		return s.createRemovalDebt(receipt, at)
	}
	if s.Cleanup[position].MemberID != receipt.Target.MemberID {
		return fault.ErrIdentityMismatch
	}
	return nil // The first debt owns attribution; late native proof stays in its grant.
}

func (s *scopedState) createRemovalDebt(receipt topology.EffectReceipt, at time.Time) error {
	action := receipt.Action
	op := s.indexes.operations[topology.OperationID(action.OperatorID)]
	if op == nil || receipt.Resource.Kind != topology.AuthorityGroup {
		return fault.ErrOperationAuthorization
	}
	stamp := latestTimestamp(at, op.UpdatedAt)
	debt := cleanupRecord{Identity: storage.Identity{ClusterID: receipt.Key.ClusterID, GroupID: action.GroupID,
		NodeName: action.NodeName, ReplicaID: action.ReplicaID}, MemberID: receipt.Target.MemberID,
		OperationID: op.Status.ID, CreatedAt: stamp, UpdatedAt: stamp,
		Removed: cloneCleanupReceipt(&receipt), RemovedAt: &stamp}
	var err error
	debt.Storage, err = s.formerReplicaStorage(debt.Identity, debt.MemberID)
	if err != nil {
		return err
	}
	bindCleanupActions(&debt, op)
	if debt.StopAction == nil || debt.RetireAction == nil {
		return fault.ErrOperationAuthorization
	}
	return s.upsertCleanupLocked(debt)
}

func bindCleanupActions(debt *cleanupRecord, op *topology.Operation) {
	for _, step := range op.Plan.Steps {
		if step.GroupID != debt.Identity.GroupID || step.NodeName != debt.Identity.NodeName ||
			step.ReplicaID != debt.Identity.ReplicaID {
			continue
		}
		bound := scopedPlanAction(op, step)
		switch step.Kind {
		case topology.StepStopReplica:
			debt.StopAction = &bound
			if step.GroupID == topology.MetaGroupID {
				debt.RetireAction = &bound
			}
		case topology.StepCleanupReplicaState:
			debt.RetireAction = &bound
		default:
		}
	}
}
