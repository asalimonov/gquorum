package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// New ownership is stricter than immutable-plan shape validation: completed
// actions stay in historical plans, but another approval cannot reissue them.
func (c planningCollector) validateNewRetirement(input topology.PlanInput,
	desired topology.Document, plan topology.Plan, semantics DeletionSemantics) error {
	c.markConvergedOwners(input.Groups)
	if err := validateDeletionPartition(desired, plan, semantics); err != nil {
		return err
	}
	hosted := make(map[string]bool)
	for key, owner := range c.owners {
		if owner.progress == topology.RetainedHosted &&
			(semantics == DeletionSemanticsDetached && desired.Group(key.group) == nil ||
				!topology.ReadmissionCovers(input, desired, plan, owner.target.NodeName)) {
			hosted[key.group] = true
		}
	}
	allowedStops := c.retainedStopBindings(desired, semantics)
	for _, step := range plan.Steps {
		owner := c.owners[planningReplicaKey{step.GroupID, step.ReplicaID}]
		reusable := owner != nil && owner.progress == topology.RetainedCleanup &&
			step.Kind == topology.StepStopReplica &&
			allowedStops[planningReplicaKey{step.GroupID, step.ReplicaID}] == step.NodeName
		if step.Kind == topology.StepDeleteGroupState && !hosted[step.GroupID] ||
			owner != nil && !reusable && duplicateRetirement(*owner, desired.Group(step.GroupID), step, semantics) {
			return fault.New(fault.OperationAuthorization, "retirement already belongs to retained cleanup", nil)
		}
	}
	return nil
}

// PlanInput can conservatively include a handover Stop that is already detached.
// Index exact reusable bindings once; the original debt retains dispatch authority.
func (c planningCollector) retainedStopBindings(desired topology.Document,
	semantics DeletionSemantics) map[planningReplicaKey]string {
	allowed := make(map[planningReplicaKey]string)
	for _, debt := range c.evidence.cleanup {
		if debt.Stopped != nil || debt.StopAction == nil {
			continue
		}
		bound := c.evidence.operations[debt.StopAction.OperationID]
		reusable := requiredCleanupStop(bound, *debt.StopAction) ||
			semantics == DeletionSemanticsDetached && desired.Group(debt.Identity.GroupID) == nil &&
				detachedCleanupStop(bound, debt.StopAction)
		if reusable {
			allowed[planningReplicaKey{debt.Identity.GroupID, debt.Identity.ReplicaID}] = debt.Identity.NodeName
		}
	}
	return allowed
}

func validateDeletionPartition(desired topology.Document, plan topology.Plan, semantics DeletionSemantics) error {
	if semantics != DeletionSemanticsDetached {
		return nil
	}
	for _, block := range plan.Blocks {
		if len(block.Steps) != 0 && desired.Group(block.GroupID) == nil {
			return fault.New(fault.OperationAuthorization, "deletion stop work must be detached", nil)
		}
	}
	return nil
}

func (c planningCollector) markConvergedOwners(groups map[string]topology.GroupProgress) {
	for id, progress := range groups {
		if progress.Converged == nil {
			continue
		}
		for _, replica := range progress.Converged.Replicas {
			if owner := c.owners[planningReplicaKey{id, replica.ReplicaID}]; owner != nil {
				owner.hosted() // A checkpoint supplies possible ownership, never removal.
			}
		}
	}
}

func duplicateRetirement(owner planningReplica, desired *topology.Group, step topology.Step,
	semantics DeletionSemantics) bool {
	switch step.Kind {
	case topology.StepRemoveReplica:
		return owner.removed != 0
	case topology.StepCleanupReplicaState:
		return owner.progress == topology.RetainedCleanup || owner.progress == topology.RetainedStopped
	case topology.StepStopReplica:
		if owner.progress == topology.RetainedStopped {
			return true
		}
		if owner.progress != topology.RetainedCleanup {
			return false
		}
		if desired == nil {
			return semantics == DeletionSemanticsDetached
		}
		return !slices.ContainsFunc(desired.Replicas, func(r topology.Replica) bool {
			return r.NodeName == step.NodeName && r.ReplicaID != step.ReplicaID
		})
	default:
		return false
	}
}
