package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

type admissionProgress struct {
	prepared, learner, promoted bool
	nativeRole                  topology.ReplicaRole
	checkpoint                  uint64
}

func compareAdmissionOperations(a, b *topology.Operation) int {
	return cmp.Or(cmp.Compare(a.Status.Version, b.Status.Version), cmp.Compare(a.Status.Attempt, b.Status.Attempt))
}

func admissionStep(a AdmissionRecord, step topology.Step) bool {
	return step.GroupID == topology.MetaGroupID && step.NodeName == a.NodeName && step.ReplicaID == a.MemberID
}

func (p *admissionProgress) consume(kind topology.StepKind) {
	switch kind {
	case topology.StepPrepareReplica:
		p.prepared = true
	case topology.StepAddLearner:
		p.learner = true
		p.nativeRole = topology.ReplicaLearner
	case topology.StepPromoteVoter:
		p.promoted = true
		p.nativeRole = topology.ReplicaVoter
	case topology.StepDemoteVoter:
		p.nativeRole = topology.ReplicaLearner
	default: // Other effects do not change admission milestones or role.
	}
}

// FI_ADMISSION_ORIGIN: prefix operations keep their own immutable action tuples.
// Issuance selects provenance; only consumption supplies progress. Neither a
// Pending row nor authenticated Ready substitutes for these facts.
func scopedAdmissionProgress(a AdmissionRecord, current *topology.Operation,
	index *scopedIndexes, catalog authorityCatalog) (admissionProgress, bool) {
	progress := admissionProgress{}
	chain, valid := admissionPrefixOperations(a, current, index, catalog)
	if !valid {
		return progress, false
	}
	progress.includeCheckpoint(a, index)
	if progress.checkpoint >= chain[0].Status.Version && !a.State.terminal() {
		return admissionProgress{}, false // Metadata completion must complete its live admissions atomically.
	}
	var version uint64
	beforeVersion := progress
	for _, operation := range chain {
		if version != operation.Status.Version {
			version, beforeVersion = operation.Status.Version, progress
		}
		approved := index.documents[operation.Status.Version]
		// Immutable same-version retries share the pre-plan role. Covered terminal
		// plans already passed structural validation; convergence subsumes their effects.
		if approved.MemberID(a.NodeName) != a.MemberID || operation.Status.Version > progress.checkpoint &&
			!beforeVersion.validSuffix(a, operation.Plan, approved) {
			return admissionProgress{}, false
		}
		progress.includeEffects(a, operation, index, catalog)
	}
	return progress, true
}

func admissionPrefixOperations(a AdmissionRecord, current *topology.Operation,
	index *scopedIndexes, catalog authorityCatalog) ([]*topology.Operation, bool) {
	if a.ReadmissionOperatorID == "" {
		return []*topology.Operation{current}, true
	}
	origin := index.operations[a.ReadmissionOperatorID]
	if origin == nil {
		return nil, false
	}
	selected := map[topology.OperationID]*topology.Operation{origin.Status.ID: origin, current.Status.ID: current}
	for _, record := range catalog.records {
		action := record.Grant.Action
		if action == nil || action.GroupID != topology.MetaGroupID || action.NodeName != a.NodeName ||
			action.ReplicaID != a.MemberID || !admissionPrefixKind(action.Kind) {
			continue
		}
		if action.Version > current.Status.Version ||
			action.Version == current.Status.Version && action.Attempt >= current.Status.Attempt {
			continue // Current-owner suffix and later historical facts cannot select the origin.
		}
		operation := index.operations[topology.OperationID(action.OperatorID)]
		if operation == nil {
			return nil, false // Live handover must retain its issued provenance.
		}
		if compareAdmissionOperations(operation, origin) < 0 {
			return nil, false
		}
		selected[operation.Status.ID] = operation
	}
	chain := make([]*topology.Operation, 0, len(selected))
	for _, operation := range selected {
		chain = append(chain, operation)
	}
	slices.SortFunc(chain, compareAdmissionOperations)
	return chain, true
}

func admissionPrefixKind(kind topology.StepKind) bool {
	return kind == topology.StepPrepareReplica || kind == topology.StepAddLearner ||
		kind == topology.StepPromoteVoter || kind == topology.StepDemoteVoter
}

func (p *admissionProgress) includeCheckpoint(a AdmissionRecord, index *scopedIndexes) {
	checkpoint := index.groups[topology.MetaGroupID]
	if checkpoint == nil || checkpoint.Declaration == nil {
		return
	}
	// The graph validates this durable checkpoint before admissions. Approval
	// preparation must use the existing graph, never a prospective checkpoint.
	for _, replica := range checkpoint.Declaration.Replicas {
		if replica.NodeName == a.NodeName && replica.ReplicaID == a.MemberID {
			p.prepared, p.learner = true, true
			p.promoted = replica.Role == topology.ReplicaVoter
			p.nativeRole, p.checkpoint = replica.Role, checkpoint.SourceVersion
		}
	}
}

func (p *admissionProgress) includeEffects(a AdmissionRecord, operation *topology.Operation,
	index *scopedIndexes, catalog authorityCatalog) {
	for _, step := range operation.Plan.Steps {
		if !admissionStep(a, step) || !admissionPrefixKind(step.Kind) ||
			!scopedStepConsumed(operation, step, index, catalog) {
			continue
		}
		// A historical receipt can precede its cursor acknowledgement. Current
		// milestones retain the normal execution cursor requirement.
		if operation.Status.ID == topology.OperationID(a.OperatorID) &&
			!slices.ContainsFunc(operation.Status.Blocks, func(block topology.BlockStatus) bool {
				return block.GroupID == topology.MetaGroupID && scopedStepCompleted(block, step.Seq)
			}) {
			continue
		}
		role := p.nativeRole
		p.consume(step.Kind)
		if operation.Status.Version <= p.checkpoint {
			p.nativeRole = role // Historical milestones cannot overwrite newer convergence.
		}
	}
}

func (p admissionProgress) validSuffix(a AdmissionRecord, plan topology.Plan, approved topology.Document) bool {
	role := topology.ReplicaLearner
	for _, replica := range approved.MetaGroup().Replicas {
		if replica.NodeName == a.NodeName && replica.ReplicaID == a.MemberID {
			role = replica.Role
		}
	}
	steps := plan.Steps
	if p.nativeRole == topology.ReplicaVoter {
		return validAdmittedVoterSuffix(a, steps, role)
	}
	prefix, valid := p.suffixPrefix(a, steps)
	if !valid {
		return false
	}
	// Grammar-only prefix: no sequence numbers, grants or receipts are created.
	// Insert before this member's suffix, preserving unrelated membership work
	// and the existing learner-first/paired-promotion adjacency checks.
	position := slices.IndexFunc(steps, func(step topology.Step) bool {
		return step.Kind != topology.StepTransferLeader && admissionStep(a, step)
	})
	if position < 0 {
		position = len(steps)
	}
	grammar := append(slices.Clone(steps[:position]), prefix...)
	grammar = append(grammar, steps[position:]...)
	return topology.ValidAdmissionSequence(grammar, a.NodeName, a.MemberID, role)
}

func validAdmittedVoterSuffix(a AdmissionRecord, steps []topology.Step, role topology.ReplicaRole) bool {
	demoted := false
	for _, step := range steps {
		if step.Kind == topology.StepTransferLeader || !admissionStep(a, step) {
			continue
		}
		if role != topology.ReplicaLearner || demoted || step.Kind != topology.StepDemoteVoter {
			return false
		}
		demoted = true
	}
	return demoted == (role == topology.ReplicaLearner)
}

func containsAdmissionStep(a AdmissionRecord, steps []topology.Step, kind topology.StepKind) bool {
	return slices.ContainsFunc(steps, func(step topology.Step) bool {
		return step.Kind == kind && admissionStep(a, step)
	})
}

func (p admissionProgress) suffixPrefix(a AdmissionRecord, steps []topology.Step) ([]topology.Step, bool) {
	var prefix []topology.Step
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner} {
		if containsAdmissionStep(a, steps, kind) {
			continue
		}
		if kind == topology.StepPrepareReplica && !p.prepared || kind == topology.StepAddLearner && !p.learner {
			return nil, false
		}
		prefix = append(prefix, topology.Step{Kind: kind, GroupID: topology.MetaGroupID,
			NodeName: a.NodeName, ReplicaID: a.MemberID})
	}
	return prefix, true
}
