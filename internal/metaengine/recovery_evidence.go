package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

// recoveryEvidence derives a complete same-key pair from committed state only.
// Non-creation fences/transfers cannot erase proof of B's actual creation.
func (s *scopedState) recoveryEvidence(group model.ImportGroup,
	proof model.CheckpointProof) (model.Consumed, error) {
	record, err := s.currentRecoveryCreation(group)
	if err != nil {
		return model.Consumed{}, err
	}
	evidence := recoveryRecordedPair(record, group)
	evidence.Identity, evidence.Checkpoint, evidence.SnapshotDigest = proof.Identity, proof.Position, proof.Digest
	if err := validateRecoveryConsumed(s, s.indexes, group, evidence); err != nil {
		return model.Consumed{}, err
	}
	return evidence, nil
}
func recoveryRecordedPair(record *authorityRecord, group model.ImportGroup) model.Consumed {
	var evidence model.Consumed
	for _, receipt := range record.Effects {
		if receipt.Key != record.Grant.Key {
			continue
		}
		switch receipt.Resource.Kind {
		case topology.AuthorityHost:
			if receipt.Target.NodeName == group.Destination.NodeName &&
				receipt.Target.ReplicaID == group.Destination.ReplicaID {
				evidence.HostReceipt = receipt.Clone()
			}
		case topology.AuthorityGroup:
			evidence.GroupReceipt = receipt.Clone()
		default:
		}
	}
	return evidence
}
func completeRecoveryPair(record *authorityRecord, group model.ImportGroup) bool {
	pair := recoveryRecordedPair(record, group)
	return pair.HostReceipt.Key.Revision != 0 && pair.GroupReceipt.Key.Revision != 0
}
func (s *scopedState) currentRecoveryCreation(group model.ImportGroup) (*authorityRecord, error) {
	catalog := s.authorityCatalog()
	attempts := s.recoveryCreationAttempts()
	var selected *authorityRecord
	for _, record := range catalog.records {
		if !s.isRecoveryCreation(record.Grant, group, attempts) || !completeRecoveryPair(record, group) {
			continue
		}
		if selected == nil || record.Grant.Key.Revision > selected.Grant.Key.Revision {
			selected = record
		}
	}
	if selected == nil {
		return nil, fault.ErrAuthorityPending
	}
	for _, resource := range selected.Grant.Resources {
		if compareAuthorityKey(catalog.floors[resource], selected.Grant.Key) < 0 {
			return nil, fault.ErrAuthorityPending
		}
	}
	for _, record := range catalog.records {
		if record.Grant.Key.Revision > selected.Grant.Key.Revision &&
			s.isRecoveryCreation(record.Grant, group, attempts) &&
			!catalog.recordSettled(record) {
			return nil, fault.ErrAuthorityPending
		}
	}
	return selected, nil
}
func (s *scopedState) recoveryCreationAttempts() map[topology.OperationID]bool {
	result := make(map[topology.OperationID]bool)
	if s.Recovery == nil {
		return result
	}
	op := s.indexes.operations[s.indexes.latest[s.Recovery.Version]]
	for op != nil && !result[op.Status.ID] {
		if op.Status.Version != s.Recovery.Version {
			break
		}
		result[op.Status.ID] = true
		op = s.indexes.operations[op.Previous]
	}
	return result
}
func (s *scopedState) isRecoveryCreation(grant topology.AuthorityGrant, group model.ImportGroup,
	attempts map[topology.OperationID]bool) bool {
	action := grant.Action
	if s.Recovery == nil || action == nil || action.Kind != topology.StepCreateFoundingGroup ||
		action.Version != s.Recovery.Version || grant.Key.ClusterID != group.Destination.ClusterID ||
		grant.Key.GroupID != group.GroupID || !attempts[grant.OperationID] {
		return false
	}
	target := topology.AuthorityTarget{NodeName: group.Destination.NodeName, MemberID: s.Recovery.Survivor.ReplicaID,
		ReplicaID: group.Destination.ReplicaID}
	return len(grant.Targets) == 1 && grant.Targets[0] == target
}
