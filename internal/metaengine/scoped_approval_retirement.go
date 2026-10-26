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
	for _, step := range plan.Steps {
		owner := c.owners[planningReplicaKey{step.GroupID, step.ReplicaID}]
		if step.Kind == topology.StepDeleteGroupState && !hosted[step.GroupID] ||
			owner != nil && duplicateRetirement(*owner, desired.Group(step.GroupID), step, semantics) &&
				!c.retainedStopAllowed(*owner, desired.Group(step.GroupID), step, semantics) {
			return fault.New(fault.OperationAuthorization, "retirement already belongs to retained cleanup", nil)
		}
	}
	return nil
}

func (c planningCollector) retainedStopAllowed(owner planningReplica, desired *topology.Group,
	step topology.Step, semantics DeletionSemantics) bool {
	return owner.progress == topology.RetainedCleanup && (c.detachingRequiredStop(step) ||
		semantics == DeletionSemanticsDetached && desired == nil && c.reusingDetachedStop(step))
}

// PlanInput describes possible host overlap, not an action's binding. If its
// conservative handover Stop is already detached, keep the original debt action;
// the extra planned step has no independent cleanup grant or dispatch authority.
func (c planningCollector) reusingDetachedStop(step topology.Step) bool {
	if step.Kind != topology.StepStopReplica {
		return false
	}
	for _, debt := range c.evidence.cleanup {
		if debt.Identity.GroupID == step.GroupID && debt.Identity.NodeName == step.NodeName &&
			debt.Identity.ReplicaID == step.ReplicaID && debt.Stopped == nil && debt.StopAction != nil {
			return detachedCleanupStop(c.evidence.operations[debt.StopAction.OperationID], debt.StopAction)
		}
	}
	return false
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

func (c planningCollector) detachingRequiredStop(step topology.Step) bool {
	if step.Kind != topology.StepStopReplica {
		return false
	}
	for _, debt := range c.evidence.cleanup {
		if debt.Identity.GroupID != step.GroupID || debt.Identity.NodeName != step.NodeName ||
			debt.Identity.ReplicaID != step.ReplicaID || debt.Stopped != nil || debt.StopAction == nil {
			continue
		}
		bound := c.evidence.operations[topology.OperationID(debt.StopAction.OperationID)]
		return requiredCleanupStop(bound, *debt.StopAction)
	}
	return false
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
