package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryEvidence selects the same-key pair already committed by ordinary
// creation. This read does not manufacture receipts or change command payloads.
func (m *Machine) RecoveryEvidence(ctx context.Context, proof model.CheckpointProof) (model.Consumed, error) {
	if err := ctx.Err(); err != nil {
		return model.Consumed{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state := m.scoped
	if state.Recovery == nil {
		return model.Consumed{}, fault.ErrOperationAuthorization
	}
	for _, group := range state.Recovery.Groups {
		if group.Destination == proof.Identity {
			return state.recoveryEvidence(group, proof)
		}
	}
	return model.Consumed{}, fault.ErrIdentityMismatch
}

func (s *scopedState) recoveryEvidence(group model.ImportGroup,
	proof model.CheckpointProof) (model.Consumed, error) {
	record, err := s.currentRecoveryCreation(group)
	if err != nil {
		return model.Consumed{}, err
	}
	evidence := model.Consumed{Identity: proof.Identity, Checkpoint: proof.Position, SnapshotDigest: proof.Digest}
	for _, receipt := range record.Effects {
		switch receipt.Resource.Kind {
		case topology.AuthorityHost:
			if receipt.Target.NodeName == group.Destination.NodeName &&
				receipt.Target.ReplicaID == group.Destination.ReplicaID {
				evidence.HostReceipt = receipt.Clone()
			}
		case topology.AuthorityGroup:
			evidence.GroupReceipt = receipt.Clone()
		default: // Other resources do not supply the exact creation pair.
		}
	}
	if evidence.HostReceipt.Key.Revision == 0 || evidence.GroupReceipt.Key.Revision == 0 {
		return model.Consumed{}, fault.ErrAuthorityPending
	}
	if err := validateRecoveryConsumed(s, s.indexes, group, evidence); err != nil {
		return model.Consumed{}, err
	}
	return evidence, nil
}

func (s *scopedState) currentRecoveryCreation(group model.ImportGroup) (*authorityRecord, error) {
	catalog := s.authorityCatalog()
	groupResource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: group.GroupID}
	hostResource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group.GroupID,
		NodeName: group.Destination.NodeName}
	key := catalog.floors[groupResource]
	if key.Revision == 0 || catalog.floors[hostResource] != key {
		return nil, fault.ErrAuthorityPending
	}
	record := catalog.records[key]
	if record == nil || record.Grant.Action == nil ||
		record.Grant.Action.Kind != topology.StepCreateFoundingGroup ||
		record.Grant.Action.Version != s.Recovery.Version {
		return nil, fault.ErrOperationAuthorization
	}
	return record, nil
}
