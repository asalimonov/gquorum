package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Plan edges explain ordering, but immutable predecessor operation IDs cannot
// strand unchanged dependent blocks. The same state predicates gate issuance,
// execution authorization and (later) completion.
func (s *scopedState) validateActionBarriers(op *topology.Operation, action topology.AuthorizedStep) error {
	catalog := s.authorityCatalog()
	if action.GroupID != topology.MetaGroupID {
		if action.Kind == topology.StepPrepareReplica || action.Kind == topology.StepCreateFoundingGroup {
			return s.validateLoadAdmission(op, action, catalog)
		}
		return nil
	}
	if action.Kind == topology.StepPrepareReplica {
		return s.validateMetaPreparation(action, catalog)
	}
	if action.Kind == topology.StepRemoveReplica {
		return s.validateMetaEvacuation(action, catalog)
	}
	return nil
}

func (s *scopedState) validateMetaPreparation(action topology.AuthorizedStep, catalog authorityCatalog) error {
	for _, admission := range s.Admissions {
		if admission.NodeName == action.NodeName && admission.MemberID == action.ReplicaID &&
			admission.OperationID == action.OperationID && !admission.State.terminal() {
			if scopedAdmissionRetirement(admission, s, s.indexes, catalog) {
				return nil
			}
			return fault.ErrUnavailable
		}
	}
	if s.Target.MemberID(action.NodeName) != action.ReplicaID {
		return fault.ErrOperationAuthorization
	}
	// Approval may precede the joining process. Its exact binding arrives
	// through Join; absence is a dependency wait, not a failed operation.
	return fault.New(fault.Unavailable, "metadata admission binding pending", nil)
}

func (s *scopedState) validateLoadAdmission(op *topology.Operation, action topology.AuthorizedStep,
	catalog authorityCatalog) error {
	approved := s.indexes.documents[op.Status.Version]
	var targets []topology.Replica
	if action.Kind == topology.StepCreateFoundingGroup {
		targets = approved.VotersOf(action.GroupID)
	} else if replica := documentReplica(approved, action.GroupID, action.ReplicaID); replica != nil {
		targets = []topology.Replica{*replica}
	}
	if len(targets) == 0 {
		return fault.ErrOperationAuthorization
	}
	report, err := topology.EvaluatePlacement(topology.PlacementInput{
		Approved: approved, Desired: s.Target, GroupID: action.GroupID})
	if err != nil {
		return fault.ErrOperationAuthorization
	}
	for _, replica := range targets {
		current := documentReplica(s.Target, action.GroupID, replica.ReplicaID)
		member := approved.MemberID(replica.NodeName)
		if current == nil || *current != replica || member == 0 || s.Target.MemberID(replica.NodeName) != member ||
			!slices.ContainsFunc(report.Candidates, func(candidate topology.PlacementCandidate) bool {
				return candidate.NodeName == replica.NodeName && candidate.CanRetain &&
					(replica.Role != topology.ReplicaVoter || candidate.CanVote)
			}) {
			return fault.ErrOperationAuthorization
		}
		if !s.metadataPrepared(replica.NodeName, member, catalog) {
			return fault.ErrUnavailable
		}
		if !s.loadHostRetired(action.GroupID, replica) {
			return fault.New(fault.Unavailable, "same-host predecessor retirement pending", nil)
		}
	}
	return nil
}

// FI_HOST_REUSE: Stop joins the old owner but does not quarantine its storage.
// Retire must consume the shared host floor before a successor Prepare takes it;
// otherwise late cleanup fences the successor's next restart. Collection stays
// detached. Use the existing sorted debt index, not a scan of unrelated hosts.
func (s *scopedState) loadHostRetired(group string, replica topology.Replica) bool {
	key := storage.Identity{ClusterID: s.Target.ClusterID, GroupID: group, NodeName: replica.NodeName}
	rows := s.indexes.cleanupOrder
	start, _ := slices.BinarySearchFunc(rows, key, func(row *cleanupRecord, key storage.Identity) int {
		return compareReplicaIdentity(row.Identity, key)
	})
	for _, debt := range rows[start:] {
		if debt.Identity.GroupID != group || debt.Identity.NodeName != replica.NodeName {
			break
		}
		if debt.Identity.ReplicaID != replica.ReplicaID && debt.Retired == nil {
			return false
		}
	}
	return true
}

func (s *scopedState) metadataPrepared(node string, member uint64, catalog authorityCatalog) bool {
	if row := s.indexes.groups[topology.MetaGroupID]; row != nil && row.SourceVersion != 0 &&
		groupHasMember(row.Declaration, node, member) {
		return true
	}
	for _, admission := range s.Admissions {
		if admission.NodeName == node && admission.MemberID == member && !admission.State.terminal() &&
			(!admission.State.isPrepared() || !scopedAdmissionRetirement(admission, s, s.indexes, catalog)) {
			return false
		}
	}
	for _, record := range catalog.records {
		if metadataPreparationConsumed(record, node, member) {
			return true
		}
	}
	return false
}

func metadataPreparationConsumed(record *authorityRecord, node string, member uint64) bool {
	action := record.Grant.Action
	if action == nil || action.GroupID != topology.MetaGroupID || action.Kind != topology.StepPrepareReplica ||
		action.NodeName != node || action.ReplicaID != member {
		return false
	}
	return slices.ContainsFunc(record.Effects, func(receipt topology.EffectReceipt) bool {
		return receipt.Resource.Kind == topology.AuthorityHost && receipt.Target.NodeName == node &&
			receipt.Target.MemberID == member && receipt.Target.ReplicaID == member
	}) && slices.ContainsFunc(record.Grant.Resources, func(resource topology.AuthorityResource) bool {
		return resource.Kind == topology.AuthorityHost && resource.NodeName == node &&
			resourceConsumed(record, resource)
	})
}

func groupHasMember(group *topology.Group, node string, replica uint64) bool {
	return group != nil && slices.ContainsFunc(group.Replicas, func(candidate topology.Replica) bool {
		return candidate.NodeName == node && candidate.ReplicaID == replica
	})
}

func (s *scopedState) validateMetaEvacuation(action topology.AuthorizedStep, catalog authorityCatalog) error {
	if current := s.Target.MemberID(action.NodeName); current != 0 {
		if current <= action.ReplicaID {
			return fault.ErrOperationAuthorization
		}
		collector := s.planningCollector()
		proof, err := collector.deletionReadmission(topology.MetaGroupID, topology.AuthorityTarget{
			NodeName: action.NodeName, MemberID: action.ReplicaID, ReplicaID: action.ReplicaID}, s.Target)
		if err != nil || !catalog.readmissionBatchConsumed(proof.Grant.AdmissionID) {
			return fault.ErrUnavailable
		}
		return nil
	}
	return s.validateLoadEvacuation(action)
}

func (s *scopedState) validateLoadEvacuation(action topology.AuthorizedStep) error {
	for _, row := range s.Groups {
		if row.GroupID == topology.MetaGroupID {
			continue
		}
		if groupNamesNode(row.Declaration, action.NodeName) ||
			groupNamesNode(s.Target.Group(row.GroupID), action.NodeName) {
			return fault.ErrUnavailable
		}
		if row.SourceVersion == 0 && s.groupReferencedMember(row.GroupID, action.NodeName, action.ReplicaID) {
			return fault.ErrUnavailable
		}
	}
	collector := s.planningCollector()
	if _, err := collector.collect(s.planningDeclarations(), s.Target); err != nil {
		return err
	}
	for key, owner := range collector.owners {
		if key.group != topology.MetaGroupID && owner.target.NodeName == action.NodeName &&
			owner.target.MemberID == action.ReplicaID && owner.progress == topology.RetainedHosted {
			return fault.ErrUnavailable
		}
	}
	return nil
}

func (s *scopedState) groupReferencedMember(group, node string, member uint64) bool {
	for _, doc := range s.indexes.documents {
		if doc.MemberID(node) == member && scopedApprovedVersion(s, doc.Version) &&
			groupNamesNode(doc.Group(group), node) {
			return true
		}
	}
	return false
}
