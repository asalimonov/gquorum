package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func requiredPlanStop(operation *topology.Operation, block topology.PlanBlock, id storage.Identity) bool {
	for _, seq := range block.Steps {
		step := operation.Plan.Steps[seq-1]
		if step.Kind == topology.StepStopReplica && step.GroupID == id.GroupID &&
			step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID {
			return true
		}
	}
	return false
}

// The current deletion attempt supplies the stop, independently of whether its
// debt originally belonged to detached removal. Failed tips retain that binding.
func (s *scopedState) deletionStop(id storage.Identity) bool {
	var row *groupConvergence
	for i := range s.Groups {
		if s.Groups[i].GroupID == id.GroupID {
			row = &s.Groups[i]
		}
	}
	if row == nil || !row.Deleting {
		return false
	}
	tip, position := s.latestGroupOperation(id.GroupID)
	if tip == nil {
		return false
	}
	state := tip.Status.Blocks[position].State
	current := row.Owner == tip.Status.ID && !state.Terminal()
	failed := row.Owner == "" && state == topology.OperationFailed && row.SourceVersion < tip.Status.Version
	return (current || failed) && requiredPlanStop(tip, tip.Plan.Blocks[position], id)
}

func (s *scopedState) latestGroupOperation(group string) (*topology.Operation, int) {
	var tip *topology.Operation
	var position int
	for i := range s.Operations {
		op := &s.Operations[i]
		for j, block := range op.Plan.Blocks {
			if block.GroupID == group && (tip == nil || compareAdmissionOperations(op, tip) > 0) {
				tip, position = op, j
			}
		}
	}
	return tip, position
}

func (p deletionPreparation) deletionDebt(id storage.Identity, block topology.PlanBlock,
	existing *cleanupRecord) (cleanupRecord, error) {
	if existing == nil {
		return p.ordinaryDebt(id, block)
	}
	if p.semantics == DeletionSemanticsDetached {
		owner := p.collector.owners[planningReplicaKey{id.GroupID, id.ReplicaID}]
		if existing.Identity != id || owner == nil || existing.MemberID != owner.target.MemberID {
			return cleanupRecord{}, fault.ErrIdentityMismatch
		}
		class, err := p.source.formerReplicaStorage(id, existing.MemberID)
		if err != nil {
			return cleanupRecord{}, err
		}
		if existing.Storage != class {
			return cleanupRecord{}, fault.ErrIdentityMismatch
		}
		return cleanupRecord{}, nil // Existing exact debt keeps its origin and proof.
	}
	op := &p.next.Operations[len(p.next.Operations)-1]
	if existing.Stopped != nil || requiredPlanStop(op, block, id) {
		return cleanupRecord{}, nil
	}
	owner := p.collector.owners[planningReplicaKey{id.GroupID, id.ReplicaID}]
	_, err := p.collector.deletionReadmission(id.GroupID, owner.target, p.next.Target)
	return cleanupRecord{}, err
}
