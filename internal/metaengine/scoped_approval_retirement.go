package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// New ownership is stricter than immutable-plan shape validation: completed
// actions stay in historical plans, but another approval cannot reissue them.
func validateNewRetirement(c planningCollector, input topology.PlanInput,
	desired topology.Document, plan topology.Plan) error {
	c.markConvergedOwners(input.Groups)
	hosted := make(map[string]bool)
	for key, owner := range c.owners {
		if owner.progress == topology.RetainedHosted &&
			!topology.ReadmissionCovers(input, desired, plan, owner.target.NodeName) {
			hosted[key.group] = true
		}
	}
	for _, step := range plan.Steps {
		owner := c.owners[planningReplicaKey{step.GroupID, step.ReplicaID}]
		if step.Kind == topology.StepDeleteGroupState && !hosted[step.GroupID] ||
			owner != nil && duplicateRetirement(*owner, desired.Group(step.GroupID), step) &&
				!(owner.progress == topology.RetainedCleanup && c.detachingRequiredStop(step)) {
			return fault.New(fault.OperationAuthorization, "retirement already belongs to retained cleanup", nil)
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
		bound := c.evidence.operations[topology.OperationID(debt.StopAction.OperatorID)]
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

func duplicateRetirement(owner planningReplica, desired *topology.Group, step topology.Step) bool {
	switch step.Kind {
	case topology.StepRemoveReplica:
		return owner.removed != 0
	case topology.StepCleanupReplicaState:
		return owner.progress == topology.RetainedCleanup || owner.progress == topology.RetainedStopped
	case topology.StepStopReplica:
		if owner.progress == topology.RetainedStopped {
			return true
		}
		if owner.progress != topology.RetainedCleanup || desired == nil {
			return false
		}
		return !slices.ContainsFunc(desired.Replicas, func(r topology.Replica) bool {
			return r.NodeName == step.NodeName && r.ReplicaID != step.ReplicaID
		})
	default:
		return false
	}
}
