package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Validate coverage against the source's exact owners before publishing any
// logical checkpoint. Pending actions are obligations, never stop receipts.
// Restore reuses this check for already-completed logical checkpoints.
func (p deletionPreparation) validateDetachedCoverage(blocks []topology.PlanBlock) error {
	if p.source.indexes == nil {
		return fault.ErrOperationAuthorization
	}
	deleting, err := p.detachedDeletionScopes(blocks)
	if err != nil || len(deleting) == 0 {
		return err
	}
	check, err := p.detachedCoverageCheck()
	if err != nil {
		return err
	}
	for _, key := range p.collector.sortedOwners() {
		owner := p.collector.owners[key]
		if deleting[key.group] && owner.progress != "" {
			if err := p.validateDetachedOwner(key, owner, check); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p deletionPreparation) detachedDeletionScopes(blocks []topology.PlanBlock) (map[string]bool, error) {
	retired := make(map[string]uint64, len(p.next.RetiredGroups))
	for _, row := range p.next.RetiredGroups {
		retired[row.ID] = row.Version
	}
	deleting := make(map[string]bool)
	for _, block := range blocks {
		if p.next.Target.Group(block.GroupID) != nil {
			continue
		}
		if block.GroupID == topology.MetaGroupID || len(block.Steps) != 0 || deleting[block.GroupID] ||
			retired[block.GroupID] == 0 || retired[block.GroupID] > p.next.Target.Version {
			return nil, fault.ErrOperationAuthorization
		}
		deleting[block.GroupID] = true
	}
	return deleting, nil
}

func (p deletionPreparation) detachedCoverageCheck() (scopedCleanupValidation, error) {
	// Reuse validated source context, replacing only the candidate row lookups.
	// Do not build the prospective graph/catalog to prove its own completion.
	index := *p.source.indexes
	err := indexRows(p.next.Cleanup, &index.cleanup, func(row *cleanupRecord) storage.Identity {
		return row.Identity
	})
	if err != nil {
		return scopedCleanupValidation{}, err
	}
	err = indexRows(p.next.Operations, &index.operations, func(op *topology.Operation) topology.OperationID {
		return op.Status.ID
	})
	if err != nil {
		return scopedCleanupValidation{}, err
	}
	return scopedCleanupValidation{state: p.next, index: &index, catalog: p.collector.evidence.authority}, nil
}

func (p deletionPreparation) validateDetachedOwner(key planningReplicaKey, owner *planningReplica,
	check scopedCleanupValidation) error {
	id := storage.Identity{ClusterID: p.next.Target.ClusterID, GroupID: key.group,
		NodeName: owner.target.NodeName, ReplicaID: key.id}
	debt := check.index.cleanup[id]
	if debt == nil || debt.MemberID != owner.target.MemberID || check.index.operations[debt.OperationID] == nil {
		return fault.ErrOperationAuthorization
	}
	class, err := p.source.formerReplicaStorage(id, debt.MemberID)
	if err != nil {
		return err
	}
	if debt.Storage != class {
		return fault.ErrIdentityMismatch
	}
	if !validCleanupRecordShape(*debt) || !check.actions(*debt) || !check.milestones(*debt) {
		return fault.ErrOperationAuthorization
	}
	if debt.Stopped == nil && (debt.StopAction == nil ||
		!detachedCleanupStop(check.index.operations[debt.StopAction.OperationID], debt.StopAction)) {
		return fault.ErrOperationAuthorization
	}
	return nil
}
