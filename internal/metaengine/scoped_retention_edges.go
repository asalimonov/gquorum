package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func (r *scopedRetention) closeDocumentsAndOperations() {
	for version, doc := range r.state.indexes.documents {
		if r.documents[version] && doc.RebasedFrom != 0 {
			r.document(doc.RebasedFrom)
			r.document(doc.ParentVersion)
		}
	}
	for _, operation := range r.state.Operations {
		if !r.versions[operation.Status.Version] {
			continue
		}
		r.document(operation.Status.Version)
		for _, block := range operation.Plan.Blocks {
			r.document(block.SourceVersion)
		}
		// A pinned plan includes every successful sibling, Previous receipt and
		// issued effect that may have completed before its cursor acknowledgement.
		for _, record := range r.state.AuthorityRecords {
			if record.Grant.OperationID == topology.OperationID(operation.Status.ID) {
				r.grants[record.Grant.Key] = true
			}
		}
	}
	r.closeCheckpointContexts()
}

func (r *scopedRetention) closeCheckpointContexts() {
	selected := r.selected()
	for _, row := range r.state.Groups {
		source, exists := r.state.indexes.documents[row.SourceVersion]
		if !exists || row.Declaration == nil || r.documents[row.SourceVersion] {
			continue
		}
		for version, doc := range r.state.indexes.documents {
			if r.documents[version] && scopedApprovedVersion(&selected, version) &&
				reflect.DeepEqual(doc.Group(row.GroupID), row.Declaration) &&
				!sameGroupMembers(source, doc, row.Declaration) {
				r.document(row.SourceVersion)
			}
		}
	}
}

func (r *scopedRetention) closeCleanup() {
	for _, debt := range r.state.Cleanup {
		if !r.cleanup[debt.Identity] {
			continue
		}
		r.operation(debt.OperationID)
		for _, action := range []*topology.AuthorizedStep{debt.StopAction, debt.RetireAction} {
			if action != nil {
				r.operation(topology.OperationID(action.OperationID))
			}
		}
		r.cleanupReceipts(debt)
		if debt.ReadmissionKey != nil {
			r.grants[*debt.ReadmissionKey] = true
		}
		r.cleanupDeclaration(debt)
		r.closeCollectionLineage(debt)
	}
}

func (r *scopedRetention) cleanupReceipts(debt cleanupRecord) {
	for _, receipt := range []*topology.EffectReceipt{debt.Removed, debt.Stopped, debt.Retired} {
		if receipt != nil {
			r.grants[receipt.Key] = true
			if receipt.Action != nil {
				r.operation(topology.OperationID(receipt.Action.OperationID))
			}
		}
	}
}

func (r *scopedRetention) cleanupDeclaration(debt cleanupRecord) {
	var version uint64
	for _, doc := range r.state.indexes.documents {
		if doc.MemberID(debt.Identity.NodeName) != debt.MemberID || !scopedApprovedVersion(r.state, doc.Version) {
			continue
		}
		group := doc.Group(debt.Identity.GroupID)
		if doc.Version > version && group != nil && group.Storage == debt.Storage &&
			documentMentionsIdentity(doc, debt.Identity) {
			version = doc.Version
		}
	}
	r.document(version)
	// If neither a checkpoint nor a retained plan proves approval, retain its
	// original approval. Never turn an unapproved historical declaration into proof.
	selected := r.selected()
	if scopedApprovedVersion(&selected, version) {
		return
	}
	var witness topology.OperationID
	for _, operation := range r.state.Operations {
		if (witness == "" || operation.Status.ID < witness) &&
			(operation.Status.Version == version || slices.ContainsFunc(operation.Plan.Blocks,
				func(block topology.PlanBlock) bool { return block.SourceVersion == version })) {
			witness = operation.Status.ID
		}
	}
	r.operation(witness)
}

func (r *scopedRetention) closeAdmissions() {
	for _, admission := range r.state.Admissions {
		key := admissionKey(admission.NodeName, admission.InstanceID)
		// A retained founding operation must remember that its seed was used.
		if r.versions[1] && admission.OperationID == topology.MakeOperationID(1, 1) {
			r.admissions[key] = true
		}
		if !r.admissions[key] {
			continue
		}
		if admission.ReadmissionOperatorID != "" || !admission.State.terminal() {
			r.operation(topology.OperationID(admission.OperationID))
			r.operation(admission.ReadmissionOperatorID)
		}
		if admission.ReadmissionInstanceID != "" {
			r.admissions[admissionKey(admission.NodeName, admission.ReadmissionInstanceID)] = true
		}
		r.readmissionBatch(ReadmissionID(admission))
		r.admissionRetirementWitness(admission)
		r.admissionPrefix(admission)
	}
}

func (r *scopedRetention) readmissionBatch(id string) {
	for _, record := range r.state.AuthorityRecords {
		if record.Grant.Purpose == topology.AuthorityReadmit && record.Grant.AdmissionID == id {
			r.grants[record.Grant.Key] = true
		}
	}
}

func (r *scopedRetention) closeRetiredMembers() {
	candidate := r.selected()
	catalog := candidate.authorityCatalog()
	admissions := append([]AdmissionRecord(nil), candidate.Admissions...)
	for _, node := range candidate.Target.Nodes {
		admissions = append(admissions, AdmissionRecord{NodeName: node.Name,
			MemberID: candidate.Target.MemberID(node.Name)})
	}
	for _, admission := range admissions {
		members := unretiredFormerMembers(&candidate, r.state.indexes, catalog, admission)
		for _, record := range r.state.AuthorityRecords {
			grant := record.Grant
			if grant.Purpose != topology.AuthorityReadmit || grant.Key.GroupID != topology.MetaGroupID ||
				!r.catalog.readmissionBatchConsumed(grant.AdmissionID) {
				continue
			}
			for _, target := range grant.Targets {
				if target.NodeName == admission.NodeName && members[target.MemberID] {
					r.readmissionBatch(grant.AdmissionID)
				}
			}
		}
	}
}

// A completed batch may justify Ready even if a later batch for this same
// member was fenced. No remaining add-source is necessary for this edge.
func (r *scopedRetention) admissionRetirementWitness(admission AdmissionRecord) {
	if !admission.State.isPrepared() {
		return
	}
	id := ReadmissionID(admission)
	if !slices.ContainsFunc(r.state.AuthorityRecords, func(record scopedAuthorityRecord) bool {
		return record.Grant.Purpose == topology.AuthorityReadmit && record.Grant.AdmissionID == id
	}) {
		return
	}
	var witness string
	for _, previous := range r.state.Admissions {
		candidate := ReadmissionID(previous)
		if (witness == "" || candidate < witness) &&
			r.catalog.admissionRetirementConsumed(previous, admission.NodeName, admission.MemberID) {
			witness = candidate
		}
	}
	if witness != "" {
		r.readmissionBatch(witness)
	}
}

func (r *scopedRetention) admissionPrefix(admission AdmissionRecord) {
	current := r.state.indexes.operations[topology.OperationID(admission.OperationID)]
	if admission.ReadmissionOperatorID == "" || current == nil {
		return
	}
	for _, record := range r.state.AuthorityRecords {
		action := record.Grant.Action
		if action == nil || !r.grants[record.Grant.Key] || action.GroupID != topology.MetaGroupID ||
			action.NodeName != admission.NodeName || action.ReplicaID != admission.MemberID ||
			!admissionPrefixKind(action.Kind) {
			continue
		}
		if action.Version < current.Status.Version ||
			action.Version == current.Status.Version && action.Attempt < current.Status.Attempt {
			r.operation(topology.OperationID(action.OperationID))
		}
	}
}
