package metaengine

import (
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type admissionHandover struct {
	source  *scopedState
	proof   *scopedIndexes
	catalog authorityCatalog
}

// Prepare on owned rows only. The candidate still needs atomic revocation,
// required-stop handover and complete graph validation before installation.
func prepareAdmissionHandovers(source, next *scopedState) error {
	successor := &next.Operations[len(next.Operations)-1]
	if !slices.ContainsFunc(successor.Status.Blocks, func(block topology.BlockStatus) bool {
		return block.GroupID == topology.MetaGroupID && !block.State.Terminal()
	}) {
		return nil
	}
	catalog, err := restoreScopedAuthority(source, source.indexes)
	if err != nil {
		return err
	}
	proof := *source.indexes
	proof.operations = maps.Clone(proof.operations)
	for i := range next.Operations {
		operation := &next.Operations[i]
		proof.operations[operation.Status.ID] = operation
	}
	proof.documents = maps.Clone(proof.documents)
	proof.documents[next.Target.Version] = next.Target
	// Keep source convergence: a prospective zero-step checkpoint cannot prove
	// the omitted admission work that would authorize its own installation.
	handover := admissionHandover{source: source, proof: &proof, catalog: catalog}
	admissions := slices.Clone(source.Admissions)
	for i, admission := range admissions {
		if admission.State.terminal() || next.Target.MemberID(admission.NodeName) != admission.MemberID {
			continue // Different-member cancellation belongs to scoped revocation (Task 38).
		}
		admissions[i], err = handover.prepare(admission, successor)
		if err != nil {
			return err
		}
	}
	next.Admissions = admissions
	return nil
}

func (h admissionHandover) prepare(previous AdmissionRecord, successor *topology.Operation) (AdmissionRecord, error) {
	approved := h.proof.documents[successor.Status.Version]
	original := h.source.indexes.operations[topology.OperationID(previous.OperationID)]
	if original == nil || previous.State.terminal() || !h.currentOwner(previous, original) ||
		approved.MemberID(previous.NodeName) != previous.MemberID ||
		compareAdmissionOperations(original, successor) >= 0 ||
		!slices.ContainsFunc(successor.Status.Blocks, func(block topology.BlockStatus) bool {
			return block.GroupID == topology.MetaGroupID && !block.State.Terminal()
		}) {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	if err := validateScopedRetry(*successor, h.proof); err != nil {
		return AdmissionRecord{}, err
	}
	if !h.catalog.readmissionUsable(ReadmissionID(previous)) {
		return AdmissionRecord{}, fault.ErrAuthorityFenced
	}
	next := previous
	if next.ReadmissionOperatorID == "" {
		next.ReadmissionOperatorID = topology.OperationID(previous.OperationID)
	}
	next.OperationID = topology.OperationID(successor.Status.ID)
	if !validAdmissionOrigin(next, h.proof) {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	progress, valid := scopedAdmissionProgress(next, successor, h.proof, h.catalog)
	if !valid || previous.State.isPrepared() &&
		!scopedAdmissionRetirement(previous, h.source, h.source.indexes, h.catalog) {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	next.State = progress.handoverState(previous.State)
	return next, nil
}

func (h admissionHandover) currentOwner(a AdmissionRecord, original *topology.Operation) bool {
	row := h.source.indexes.groups[topology.MetaGroupID]
	approved := h.source.indexes.documents[original.Status.Version]
	if row == nil || h.source.Target.MemberID(a.NodeName) != a.MemberID ||
		approved.MemberID(a.NodeName) != a.MemberID {
		return false
	}
	for _, block := range original.Status.Blocks {
		if block.GroupID == topology.MetaGroupID {
			return row.Owner == "" && block.State == topology.OperationFailed ||
				row.Owner == original.Status.ID && !block.State.Terminal()
		}
	}
	return false
}

func (a authorityCatalog) readmissionUsable(id string) bool {
	for _, record := range a.records {
		if record.Grant.Purpose != topology.AuthorityReadmit || record.Grant.AdmissionID != id {
			continue
		}
		for _, resource := range record.Grant.Resources {
			if (record.Closed || a.floors[resource] != record.Grant.Key) &&
				!resourceConsumed(record, resource) {
				return false
			}
		}
	}
	return true // Consumed retirement remains a fact even after its grant closes.
}

func (p admissionProgress) handoverState(previous AdmissionState) AdmissionState {
	if !previous.isPrepared() {
		return AdmissionPending // Membership facts cannot authenticate a replacement process.
	}
	if p.promoted {
		return AdmissionPromoted
	}
	if p.learner {
		return AdmissionLearner
	}
	return AdmissionReady
}
