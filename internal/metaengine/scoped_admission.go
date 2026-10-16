package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func validateScopedAdmissions(state *scopedState, index *scopedIndexes, catalog authorityCatalog) error {
	seen := make(map[topology.OperatorID]bool, len(state.Operations))
	for _, operation := range state.Operations {
		seen[topology.OperatorID(operation.Status.ID)] = true
	}
	if err := validateRestoredAdmissions(smState{Admissions: state.Admissions}, seen); err != nil {
		return err
	}
	for _, admission := range state.Admissions {
		if !validAdmissionOrigin(admission, index) || !validScopedAdmission(admission, state, index, catalog) {
			return invalidScopedState("admission contradicts its retained approved sequence or progress")
		}
	}
	return nil
}

// FI_ADMISSION_ORIGIN: the field is a reference to retained approved proof, not
// proof by itself. Resumed successor prefixes are validated separately.
func validAdmissionOrigin(admission AdmissionRecord, index *scopedIndexes) bool {
	if admission.ReadmissionOperatorID == "" {
		return true
	}
	origin := index.operations[admission.ReadmissionOperatorID]
	current := index.operations[topology.OperationID(admission.OperatorID)]
	if origin == nil || current == nil || origin.Status.Version > current.Status.Version ||
		origin.Status.Version == current.Status.Version && origin.Status.Attempt >= current.Status.Attempt {
		return false
	}
	approved := index.documents[origin.Status.Version]
	if approved.MemberID(admission.NodeName) != admission.MemberID ||
		!hasAdmissionSequence(origin.Plan.Steps, approved, admission.NodeName, admission.MemberID) {
		return false
	}
	return slices.ContainsFunc(origin.Status.Blocks, func(block topology.BlockStatus) bool {
		return block.GroupID == topology.MetaGroupID &&
			(block.State == topology.OperationFailed || block.State == topology.OperationSuperseded)
	})
}

func validScopedAdmission(admission AdmissionRecord, state *scopedState,
	index *scopedIndexes, catalog authorityCatalog) bool {
	operation := index.operations[topology.OperationID(admission.OperatorID)]
	if operation == nil {
		return admission.OperatorID != "" && admission.State.terminal()
	}
	approved := index.documents[operation.Status.Version]
	if approved.MemberID(admission.NodeName) != admission.MemberID {
		return false
	}
	position := slices.IndexFunc(operation.Plan.Blocks,
		func(block topology.PlanBlock) bool { return block.GroupID == topology.MetaGroupID })
	if position < 0 {
		return false
	}
	block := operation.Status.Blocks[position]
	if approved.Version == 1 && approved.Origin == topology.OriginFounding {
		return block.State == topology.OperationSucceeded && admission.State.terminal()
	}
	if !hasAdmissionSequence(operation.Plan.Steps, approved, admission.NodeName, admission.MemberID) {
		return false
	}
	if !block.State.Terminal() && !admission.State.terminal() &&
		state.Target.MemberID(admission.NodeName) != admission.MemberID {
		return false
	}
	if admission.State == AdmissionSuperseded || admission.State == AdmissionRetired ||
		admission.State == AdmissionPending {
		return true // Failed attempts retain evidence; only live owner checks authorize work.
	}
	if !scopedAdmissionRetirement(admission, state, index, catalog) {
		return false
	}
	if admission.State == AdmissionCompleted {
		return block.State == topology.OperationSucceeded
	}
	return catalog.scopedAdmissionMilestone(admission, operation, block, index)
}

func (catalog authorityCatalog) scopedAdmissionMilestone(admission AdmissionRecord, operation *topology.Operation,
	block topology.BlockStatus, index *scopedIndexes) bool {
	if admission.State == AdmissionReady {
		return true // Readiness may be recorded before the preparation receipt arrives.
	}
	kind := topology.StepAddLearner
	if admission.State == AdmissionPromoted {
		kind = topology.StepPromoteVoter
	}
	for _, step := range operation.Plan.Steps {
		if step.Kind == kind && step.GroupID == topology.MetaGroupID && step.NodeName == admission.NodeName &&
			step.ReplicaID == admission.MemberID {
			return scopedStepCompleted(block, step.Seq) && scopedStepConsumed(operation, step, index, catalog)
		}
	}
	return false
}

func scopedAdmissionRetirement(admission AdmissionRecord, state *scopedState,
	index *scopedIndexes, catalog authorityCatalog) bool {
	required := false
	for _, doc := range index.documents {
		member := doc.MemberID(admission.NodeName)
		if member != 0 && member < admission.MemberID && scopedApprovedVersion(state, doc.Version) {
			required = true
		}
	}
	id := ReadmissionID(admission)
	for _, record := range catalog.records {
		if record.Grant.Purpose == topology.AuthorityReadmit && record.Grant.AdmissionID == id {
			required = true
		}
	}
	if !required {
		return true
	}
	for _, previous := range state.Admissions {
		if catalog.admissionRetirementConsumed(previous, admission.NodeName, admission.MemberID) {
			return true
		}
	}
	return false
}
