package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (check scopedCleanupValidation) stopAction(debt cleanupRecord) bool {
	origin := check.index.operations[debt.OperationID]
	bound := check.index.operations[topology.OperationID(debt.StopAction.OperationID)]
	if origin == nil || bound == nil || bound.Status.Version < origin.Status.Version ||
		bound.Status.Version == origin.Status.Version && bound.Status.Attempt < origin.Status.Attempt {
		return false
	}
	candidate := debt
	candidate.OperationID = bound.Status.ID
	if !check.action(candidate, *debt.StopAction) {
		return false
	}
	if requiredCleanupStop(bound, *debt.StopAction) {
		if debt.Stopped != nil || debt.ReadmissionKey != nil {
			return true // Consumed predecessor receipts keep their original tuple.
		}
		return check.pendingStop(debt, bound)
	}
	// Only Stop may change binding; retirement and removal retain their origin.
	return detachedCleanupStop(bound, debt.StopAction) &&
		(debt.RetireAction.Kind != topology.StepDeleteGroupState ||
			check.state.Target.Group(debt.Identity.GroupID) == nil &&
				slices.ContainsFunc(check.state.RetiredGroups, func(row retiredGroup) bool {
					return row.ID == debt.Identity.GroupID
				}))
}

func detachedCleanupStop(operation *topology.Operation, action *topology.AuthorizedStep) bool {
	return operation != nil && action != nil && action.Kind == topology.StepStopReplica &&
		slices.ContainsFunc(operation.Plan.Blocks, func(block topology.PlanBlock) bool {
			return block.GroupID == action.GroupID && slices.Contains(block.Cleanup, action.StepSeq)
		})
}

func requiredCleanupStop(operation *topology.Operation, action topology.AuthorizedStep) bool {
	return operation != nil && action.Kind == topology.StepStopReplica &&
		slices.ContainsFunc(operation.Plan.Blocks, func(block topology.PlanBlock) bool {
			return block.GroupID == action.GroupID && slices.Contains(block.Steps, action.StepSeq)
		})
}

func (check scopedCleanupValidation) pendingStop(debt cleanupRecord, tip *topology.Operation) bool {
	row := check.index.groups[debt.Identity.GroupID]
	if row == nil || debt.RetireAction.Kind == topology.StepDeleteGroupState &&
		(!row.Deleting || row.SourceVersion != 0 && row.Declaration == nil) {
		return false
	}
	for _, block := range tip.Status.Blocks {
		if block.GroupID != debt.Identity.GroupID {
			continue
		}
		if block.State == topology.OperationFailed {
			return row.Owner == "" && row.SourceVersion < tip.Status.Version &&
				check.index.latest[tip.Status.Version] == tip.Status.ID
		}
		return row.Owner == tip.Status.ID && !block.State.Terminal()
	}
	return false
}

// Traverse retained grants once; exact debt lookups avoid a grants × debts scan.
func validateScopedCleanupEffects(index *scopedIndexes, catalog authorityCatalog) error {
	for _, record := range catalog.records {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityEffect {
			continue
		}
		for _, target := range grant.Targets {
			identity := storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
				NodeName: target.NodeName, ReplicaID: target.ReplicaID}
			debt := index.cleanup[identity]
			if debt == nil {
				continue
			}
			if !cleanupGrantProgress(debt, record, target) {
				return invalidScopedState("cleanup grant contradicts its stop boundary or exact milestone")
			}
		}
	}
	return nil
}

func cleanupGrantProgress(debt *cleanupRecord, record *authorityRecord, target topology.AuthorityTarget) bool {
	action := record.Grant.Action
	if debt.RetireAction != nil && *action == *debt.RetireAction && action.Kind != topology.StepStopReplica &&
		debt.Stopped == nil && debt.ReadmissionKey == nil {
		return false
	}
	milestones := cleanupActionMilestones(debt, action)
	if len(milestones) != 0 && target.MemberID != debt.MemberID {
		return false
	}
	for _, receipt := range record.Effects {
		if receipt.Target != target {
			continue
		}
		for _, milestone := range milestones {
			if milestone == nil || receipt.Key == milestone.Key && !reflect.DeepEqual(&receipt, milestone) {
				return false
			}
		}
	}
	return true
}

func cleanupActionMilestones(debt *cleanupRecord, action *topology.AuthorizedStep) []*topology.EffectReceipt {
	var milestones []*topology.EffectReceipt
	if action.Kind == topology.StepStopReplica && cleanupRetainsAction(debt, *action) &&
		!readmissionMilestone(debt, debt.Stopped) &&
		(debt.Stopped == nil || reflect.DeepEqual(action, debt.Stopped.Action)) {
		milestones = append(milestones, debt.Stopped)
	}
	if debt.RetireAction != nil && *action == *debt.RetireAction && !readmissionMilestone(debt, debt.Retired) {
		milestones = append(milestones, debt.Retired)
	}
	if debt.Removed != nil && reflect.DeepEqual(action, debt.Removed.Action) {
		milestones = append(milestones, debt.Removed)
	}
	// Later stop bindings cannot rewrite the first exact identity-level milestone.
	return milestones
}
