package metaengine

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func prepareDeletionCompletions(source, next *scopedState, collector planningCollector) error {
	p := deletionPreparation{source, next, collector}
	if err := p.protectReadmission(); err != nil {
		return err
	}
	operation := &next.Operations[len(next.Operations)-1]
	for i, block := range operation.Plan.Blocks {
		if len(block.Steps) != 0 || next.Target.Group(block.GroupID) != nil {
			continue
		}
		if err := p.complete(block.GroupID, operation.UpdatedAt); err != nil {
			if errors.Is(err, fault.ErrUnavailable) {
				continue // A zero-step block still waits for its exact retirement proof.
			}
			return err
		}
		for j := range next.Groups {
			row := &next.Groups[j]
			if row.GroupID == block.GroupID {
				*row = groupConvergence{GroupID: block.GroupID, SourceVersion: operation.Status.Version,
					CompletedAt: operation.UpdatedAt}
			}
		}
		operation.Status.Blocks[i].State = topology.OperationSucceeded
	}
	operation.Status.State = aggregateBlockStates(operation.Status.Blocks)
	return nil
}

// No debt or milestone is published until every former host is accounted for.
func (p deletionPreparation) complete(group string, at time.Time) error {
	debts := slices.Clone(p.next.Cleanup)
	positions := make(map[storage.Identity]int, len(debts))
	for i, debt := range debts {
		positions[debt.Identity] = i
	}
	for key, owner := range p.collector.owners {
		if key.group != group || owner.progress == "" {
			continue
		}
		id := storage.Identity{ClusterID: p.next.Target.ClusterID, GroupID: group,
			NodeName: owner.target.NodeName, ReplicaID: key.id}
		position, exists := positions[id]
		debt := cleanupRecord{Identity: id, MemberID: owner.target.MemberID, CreatedAt: at, UpdatedAt: at}
		if exists {
			debt = debts[position]
			if debt.Stopped != nil {
				continue
			}
		}
		completed, err := p.readmissionDebt(debt, at)
		if err != nil {
			return err
		}
		if exists {
			debts[position] = completed
		} else {
			debts = append(debts, completed)
		}
	}
	slices.SortFunc(debts, func(a, b cleanupRecord) int { return compareReplicaIdentity(a.Identity, b.Identity) })
	p.next.Cleanup = debts
	return nil
}

func (p deletionPreparation) readmissionDebt(debt cleanupRecord, at time.Time) (cleanupRecord, error) {
	target := topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
		MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}
	record, err := p.collector.deletionReadmission(debt.Identity.GroupID, target, p.next.Target)
	if err != nil {
		return cleanupRecord{}, err
	}
	if !p.collector.evidence.authority.readmissionBatchConsumed(record.Grant.AdmissionID) {
		return cleanupRecord{}, fault.New(fault.Unavailable, "awaiting whole-batch readmission retirement", nil)
	}
	if at.Before(debt.UpdatedAt) {
		return cleanupRecord{}, fault.ErrOperationAuthorization
	}
	class, err := p.source.formerReplicaStorage(debt.Identity, debt.MemberID)
	if err != nil {
		return cleanupRecord{}, err
	}
	if debt.OperationID != "" && debt.Storage != class {
		return cleanupRecord{}, fault.ErrIdentityMismatch
	}
	debt.Storage = class
	if debt.OperationID == "" {
		debt.OperationID = p.collector.readmissionOrigin(record.Grant.AdmissionID)
		if p.source.indexes.operations[debt.OperationID] == nil {
			return cleanupRecord{}, fault.ErrOperationAuthorization
		}
	}
	for _, effect := range record.Effects {
		if effect.Resource.Kind == topology.AuthorityHost && effect.Target == target {
			receipt := effect.Clone()
			debt.ReadmissionKey, debt.Stopped, debt.Retired = &receipt.Key, &receipt, &receipt
			debt.StoppedAt, debt.RetiredAt, debt.UpdatedAt = &at, &at, at
			return debt, nil
		}
	}
	return cleanupRecord{}, fault.ErrOperationAuthorization
}

// An immutable pending deletion cannot acquire a new owner after Target omits
// it. Keep its concrete retirement source usable until consumption is durable.
func (p deletionPreparation) protectReadmission() error {
	for key, owner := range p.collector.owners {
		if owner.progress == "" || p.next.Target.MemberID(owner.target.NodeName) ==
			p.source.Target.MemberID(owner.target.NodeName) || !p.dependsOnReadmission(key, owner.target) {
			continue
		}
		record, err := p.collector.deletionReadmission(key.group, owner.target, p.source.Target)
		if errors.Is(err, fault.ErrUnavailable) {
			continue
		} // No issued batch is being cancelled.
		if err != nil {
			return err
		}
		if p.collector.evidence.authority.readmissionBatchConsumed(record.Grant.AdmissionID) {
			continue
		}
		member := p.collector.readmissionMembers(owner.target.NodeName)[record.Grant.AdmissionID]
		if p.next.Target.MemberID(owner.target.NodeName) != member {
			message := fmt.Sprintf("group %s former %s/%d requires pending readmission retirement for member %d",
				key.group, owner.target.NodeName, owner.target.MemberID, member)
			return fault.New(fault.OperationAuthorization, message, nil)
		}
	}
	return nil
}

func (p deletionPreparation) dependsOnReadmission(key planningReplicaKey, target topology.AuthorityTarget) bool {
	id := storage.Identity{ClusterID: p.next.Target.ClusterID, GroupID: key.group,
		NodeName: target.NodeName, ReplicaID: key.id}
	for _, debt := range p.next.Cleanup {
		if debt.Identity != id {
			continue
		}
		if debt.Stopped != nil || debt.StopAction == nil {
			return false
		}
		bound := p.source.indexes.operations[topology.OperationID(debt.StopAction.OperatorID)]
		return requiredCleanupStop(bound, *debt.StopAction) &&
			p.source.Target.MemberID(target.NodeName) > target.MemberID
	}
	return slices.ContainsFunc(p.next.Groups, func(row groupConvergence) bool {
		return row.GroupID == key.group && row.Deleting
	})
}

func (c planningCollector) readmissionOrigin(id string) topology.OperationID {
	for _, a := range c.evidence.admissions {
		if ReadmissionID(a) == id {
			return a.originOperator()
		}
	}
	return ""
}
