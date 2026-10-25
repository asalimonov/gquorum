package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/topology"
)

func validateRecoveryState(state *scopedState, index *scopedIndexes) error {
	recovery := state.Recovery
	if recovery == nil {
		return nil
	}
	doc, err := validateRecoveryProvenance(state, index)
	if err != nil {
		return err
	}
	previous := ""
	for _, group := range recovery.Groups {
		if group.GroupID <= previous || group.Source.GroupID != group.GroupID ||
			model.ValidateSeedSource(group.Source, recovery.Source) != nil {
			return invalidScopedState("invalid recovery source inventory")
		}
		previous = group.GroupID
		if err := validateRecoveryDestination(doc, group); err != nil {
			return err
		}
		if group.Consumed != nil {
			if err := validateRecoveryConsumed(state, index, group, *group.Consumed); err != nil {
				return err
			}
		}
		if recovery.Phase == "complete" && group.Consumed == nil {
			return invalidScopedState("recovery completed without every consumed checkpoint")
		}
	}
	return validateRecoveryCompletion(recovery, index)
}

func validateRecoveryProvenance(state *scopedState, index *scopedIndexes) (topology.Document, error) {
	recovery := state.Recovery
	if recovery.AcceptedAt.IsZero() || !validRecoveryDigest(recovery.RequestDigest) {
		return topology.Document{}, invalidScopedState("recovery import lacks accepted request proof")
	}
	if _, err := recoveryplacement.FromImport(recovery, state.Target); err != nil {
		return topology.Document{}, err
	}
	doc := index.documents[recovery.Version]
	op := index.operations[index.first[recovery.Version]]
	if doc.Version == 0 || op == nil || op.Status.Version != 2 || doc.ClusterID != recovery.Survivor.ClusterID ||
		doc.Origin != topology.OriginOperator {
		return topology.Document{}, invalidScopedState("recovery import lacks its ordinary approved operation")
	}
	if !doc.CreatedAt.Equal(recovery.AcceptedAt) {
		return topology.Document{}, invalidScopedState("recovery acceptance timestamp differs")
	}
	if len(recovery.Policies) != len(doc.Groups) || len(recovery.Groups) != len(doc.LoadGroups()) {
		return topology.Document{}, invalidScopedState("recovery import group/policy coverage differs")
	}
	for _, policy := range recovery.Policies {
		digest, err := recoveryplacement.PolicyDigest(doc, policy.GroupID)
		if err != nil || digest != policy.PolicyDigest {
			return topology.Document{}, invalidScopedState("recovery policy differs from its original import")
		}
	}
	return doc, nil
}

func validateRecoveryDestination(doc topology.Document, group model.ImportGroup) error {
	declaration := doc.Group(group.GroupID)
	if declaration == nil || len(declaration.Replicas) != 1 || group.Destination.ClusterID != doc.ClusterID ||
		group.Destination.NodeName != declaration.Replicas[0].NodeName ||
		group.Destination.ReplicaID != declaration.Replicas[0].ReplicaID ||
		group.Destination.ReplicaID <= group.Source.Identity.ReplicaID {
		return invalidScopedState("recovery destination differs from the imported exact replica")
	}
	return nil
}

func validateRecoveryCompletion(recovery *model.ImportState, index *scopedIndexes) error {
	if recovery.Phase != "complete" {
		if recovery.CompletedAt != nil {
			return invalidScopedState("pending recovery carries completion")
		}
		return nil
	}
	if recovery.CompletedAt == nil || recovery.CompletedAt.Before(recovery.AcceptedAt) {
		return invalidScopedState("recovery completion timestamp is absent or regresses")
	}
	latest := index.operations[index.latest[recovery.Version]]
	if latest == nil || latest.Status.State != topology.OperationSucceeded {
		return invalidScopedState("recovery completed before its ordinary operation")
	}
	return nil
}

func validateRecoveryConsumed(state *scopedState, index *scopedIndexes, group model.ImportGroup,
	evidence model.Consumed) error {
	if evidence.Checkpoint.Index < group.Source.Commit.Index ||
		evidence.Checkpoint.Term < group.Source.Commit.Term ||
		evidence.Identity != group.Destination || !validRecoveryDigest(evidence.SnapshotDigest) {
		return fault.ErrIdentityMismatch
	}
	if evidence.GroupReceipt.AppliedIndex != group.Source.Snapshot.Index ||
		evidence.GroupReceipt.AppliedTerm != group.Source.Snapshot.Term ||
		evidence.GroupReceipt.AppliedIndex > evidence.Checkpoint.Index ||
		evidence.GroupReceipt.AppliedTerm > evidence.Checkpoint.Term {
		return fault.ErrIdentityMismatch
	}
	for _, receipt := range []topology.EffectReceipt{evidence.HostReceipt, evidence.GroupReceipt} {
		if err := validateRecoveryReceipt(state, index, group, receipt); err != nil {
			return err
		}
	}
	if evidence.HostReceipt.Resource.Kind != topology.AuthorityHost ||
		evidence.GroupReceipt.Resource.Kind != topology.AuthorityGroup ||
		evidence.HostReceipt.Key != evidence.GroupReceipt.Key {
		return fault.ErrIdentityMismatch
	}
	target := topology.AuthorityTarget{NodeName: group.Destination.NodeName,
		MemberID: state.Recovery.Survivor.ReplicaID, ReplicaID: group.Destination.ReplicaID}
	grant := index.grants[evidence.HostReceipt.Key].Grant
	if len(grant.Targets) != 1 || grant.Targets[0] != target || evidence.HostReceipt.Target != target ||
		(evidence.GroupReceipt.Target != (topology.AuthorityTarget{}) && evidence.GroupReceipt.Target != target) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (s *scopedState) refreshRecoveryPolicies() {
	if s.Recovery == nil {
		return
	}
	record := model.CloneImport(*s.Recovery)
	context, err := recoveryplacement.FromImport(&record, s.Target)
	if err != nil {
		return
	} // Complete graph validation reports malformed provenance.
	for i := range record.Policies {
		policy := &record.Policies[i]
		if policy.PolicyReleased {
			continue
		}
		if row := s.indexes.groups[policy.GroupID]; row != nil {
			progress := topology.GroupProgress{GroupID: row.GroupID, Converged: row.Declaration,
				ConvergedVersion: row.SourceVersion, Deleting: row.Deleting}
			if recoveryplacement.CanRelease(context, progress, s.Target) {
				policy.PolicyReleased = true
			}
		}
	}
	s.Recovery = &record
}

func validateRecoveryReceipt(state *scopedState, index *scopedIndexes, group model.ImportGroup,
	receipt topology.EffectReceipt) error {
	record := index.grants[receipt.Key]
	if record == nil || receipt.Validate(record.Grant) != nil ||
		!slices.ContainsFunc(record.Effects, func(saved topology.EffectReceipt) bool {
			return reflect.DeepEqual(saved, receipt)
		}) {
		return fault.ErrOperationAuthorization
	}
	action := record.Grant.Action
	if action == nil || action.Kind != topology.StepCreateFoundingGroup ||
		action.Version != state.Recovery.Version ||
		receipt.Key.ClusterID != group.Destination.ClusterID || receipt.Key.GroupID != group.GroupID {
		return fault.ErrIdentityMismatch
	}
	return nil
}
