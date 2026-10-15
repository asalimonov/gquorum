package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (check scopedCleanupValidation) stopAction(debt cleanupRecord) bool {
	origin := check.index.operations[debt.OperationID]
	for _, block := range origin.Plan.Blocks {
		if !slices.Contains(block.Steps, debt.StopAction.StepSeq) {
			continue
		}
		bound := check.index.operations[topology.OperationID(debt.StopAction.OperatorID)]
		if bound == nil || bound.Status.Version != origin.Status.Version ||
			bound.Status.Attempt < origin.Status.Attempt {
			return false
		}
		// Graph validation has already proved this version's one complete retry chain.
		candidate := debt
		candidate.OperationID = bound.Status.ID
		if !check.action(candidate, *debt.StopAction) {
			return false
		}
		if debt.Stopped != nil || debt.ReadmissionKey != nil {
			return true // Consumed predecessor receipts keep their original tuple.
		}
		tip := check.index.operations[check.index.latest[origin.Status.Version]]
		return bound == tip && check.pendingStop(debt, tip)
	}
	// Detached work stays bound to its original action, independently of retries.
	return debt.RetireAction.Kind != topology.StepDeleteGroupState && check.action(debt, *debt.StopAction)
}

func (check scopedCleanupValidation) pendingStop(debt cleanupRecord, tip *topology.Operation) bool {
	row := check.index.groups[debt.Identity.GroupID]
	if row == nil || debt.RetireAction.Kind == topology.StepDeleteGroupState &&
		(!row.Deleting || row.Declaration == nil) {
		return false
	}
	for _, block := range tip.Status.Blocks {
		if block.GroupID != debt.Identity.GroupID {
			continue
		}
		if block.State == topology.OperationFailed {
			return row.Owner == ""
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
			if !reflect.DeepEqual(&receipt, milestone) {
				return false
			}
		}
	}
	return true
}

func cleanupActionMilestones(debt *cleanupRecord, action *topology.AuthorizedStep) []*topology.EffectReceipt {
	var milestones []*topology.EffectReceipt
	if debt.StopAction != nil && *action == *debt.StopAction && !readmissionMilestone(debt, debt.Stopped) {
		milestones = append(milestones, debt.Stopped)
	}
	if debt.RetireAction != nil && *action == *debt.RetireAction && !readmissionMilestone(debt, debt.Retired) {
		milestones = append(milestones, debt.Retired)
	}
	if debt.Removed != nil && reflect.DeepEqual(action, debt.Removed.Action) {
		milestones = append(milestones, debt.Removed)
	}
	// An older retry action is retained evidence, not the currently bound milestone.
	return milestones
}
