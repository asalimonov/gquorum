package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (r *scopedRetention) closeAuthority() {
	for _, record := range r.state.AuthorityRecords {
		if !r.grants[record.Grant.Key] {
			continue
		}
		r.closeGrant(record)
	}
	r.closeRevocations()
}

func (r *scopedRetention) closeGrant(record scopedAuthorityRecord) {
	grant := record.Grant
	if record.Class == cleanupAuthorityClass || !r.catalog.recordSettled(&record.authorityRecord) {
		r.operation(topology.OperationID(grant.OperatorID))
	}
	if record.Class == cleanupAuthorityClass {
		for _, target := range grant.Targets {
			r.cleanup[storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
				NodeName: target.NodeName, ReplicaID: target.ReplicaID}] = true
		}
	}
	if grant.Purpose == topology.AuthorityReadmit {
		r.closeReadmission(grant.AdmissionID)
	}
	for _, resource := range grant.Resources {
		if r.catalog.deletedResource(resource) {
			continue
		}
		r.settlementWitness(record.authorityRecord, resource)
		if grant.Purpose == topology.AuthorityFence && !resourceConsumed(&record.authorityRecord, resource) {
			r.fencePredecessor(grant.Key, resource)
		}
	}
}

func (r *scopedRetention) closeReadmission(id string) {
	r.readmissionBatch(id)
	for _, admission := range r.state.Admissions {
		if ReadmissionID(admission) == id {
			r.admissions[admissionKey(admission.NodeName, admission.InstanceID)] = true
			r.operation(topology.OperationID(admission.OperatorID))
		}
	}
}

func (r *scopedRetention) settlementWitness(record authorityRecord, resource topology.AuthorityResource) {
	transfer := record.Grant.Action != nil && record.Grant.Action.Kind == topology.StepTransferLeader
	if !transfer && resourceConsumed(&record, resource) {
		return
	}
	var witness topology.AuthorityKey
	for _, candidate := range r.state.AuthorityRecords {
		key := candidate.Grant.Key
		if key.GroupID != record.Grant.Key.GroupID || key.Revision <= record.Grant.Key.Revision {
			continue
		}
		if key.Revision > witness.Revision && slices.ContainsFunc(candidate.Fences,
			func(e topology.FenceEvidence) bool { return e.Resource == resource }) {
			witness = key
		}
	}
	if witness.Revision != 0 {
		r.grants[witness] = true
	}
}

func (r *scopedRetention) fencePredecessor(key topology.AuthorityKey, resource topology.AuthorityResource) {
	var witness topology.AuthorityKey
	for _, record := range r.state.AuthorityRecords {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityFence && grant.Key.GroupID == key.GroupID &&
			grant.Key.Revision < key.Revision && grant.Key.Revision > witness.Revision &&
			slices.Contains(grant.Resources, resource) {
			witness = grant.Key
		}
	}
	if witness.Revision != 0 {
		r.grants[witness] = true
	}
}

func (r *scopedRetention) closeRevocations() {
	for i, record := range r.state.Revocations {
		record.Obligations = r.catalog.liveObligations(record.Obligations)
		op := r.state.indexes.operations[record.OperationID]
		if op != nil && r.versions[op.Status.Version] || slices.ContainsFunc(record.Obligations,
			func(ob topology.FenceObligation) bool { return ob.Evidence == nil || r.grants[ob.Key] }) {
			r.revocations[i] = true
		}
		if !r.revocations[i] {
			continue
		}
		for _, obligation := range record.Obligations {
			r.grants[obligation.Key] = true
			if obligation.Evidence != nil {
				r.grants[obligation.Evidence.Key] = true
			}
		}
	}
}
