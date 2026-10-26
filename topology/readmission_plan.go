package topology

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
)

// ReadmissionCovers selects readmission as this former host's retirement owner
// from approved identity, planned admission or convergence. It is not consumption
// proof: the FSM requires exact retirement receipts before preparation/completion.
func ReadmissionCovers(input PlanInput, desired Document, plan Plan, node string) bool {
	member := desired.MemberID(node)
	previous, known := input.loadOwnerMember(node)
	metaMember := input.convergedMetaMember(node)
	if !known || member == 0 || member <= previous || metaMember > member {
		return false
	}
	if metaMember == member {
		return true // Exact convergence includes the receipt-gated admission.
	}
	if plan.Blocks != nil && approvedReadmission(input, desired, node) {
		return true // Another approved operation owns this exact pending readmission.
	}
	replica := exactPlanReplica(desired, Step{GroupID: MetaGroupID, NodeName: node, ReplicaID: member})
	return replica != nil && ValidAdmissionSequence(plan.Steps, node, member, replica.Role)
}

func approvedReadmission(input PlanInput, desired Document, node string) bool {
	return input.Approved.ClusterID == desired.ClusterID && input.Approved.MemberID(node) == desired.MemberID(node)
}

func validatePlanMembers(input PlanInput, desired Document) error {
	meta := input.Groups[MetaGroupID]
	if meta.ConvergedVersion == 0 || meta.Converged == nil {
		return nil
	}
	for _, replica := range meta.Converged.Replicas {
		member := desired.MemberID(replica.NodeName)
		if member != 0 && member < replica.ReplicaID {
			return fmt.Errorf("%w: desired member %s/%d precedes converged member %d; stage fresh intent",
				fault.ErrVersionConflict,
				replica.NodeName, member, replica.ReplicaID)
		}
	}
	return nil
}

// bindReadmissionRetirement constructs the final plan before approval. A fresh
// metadata owner retires its predecessor through the independently granted
// readmission protocol before PrepareReplica can be consumed. Later host calls
// to the retired member would be both redundant and unroutable.
func bindReadmissionRetirement(input PlanInput, desired Document, plan Plan, groups []string) Plan {
	meta := desired.MetaGroup()
	if meta == nil {
		return plan
	}
	var replacements []Replica
	for _, r := range sortedReplicas(meta.Replicas) {
		previous, known := input.loadOwnerMember(r.NodeName)
		// Unchanged metadata can still own pending readmission retirement.
		// The FSM verifies consumption, including deletion without a Prepare.
		if known && r.ReplicaID > previous && (slices.Contains(groups, MetaGroupID) ||
			input.convergedMetaMember(r.NodeName) == r.ReplicaID ||
			approvedReadmission(input, desired, r.NodeName)) {
			replacements = append(replacements, r)
		}
	}
	if len(replacements) == 0 {
		return plan
	}
	builder := readmissionPlanBuilder{input: input, desired: desired, replacements: replacements,
		metadataOwned:     slices.Contains(groups, MetaGroupID),
		preserveAdmission: readmissionPreparations(input, plan, replacements), emitted: make(map[string]bool)}
	for _, step := range plan.Steps {
		builder.appendStep(step)
	}
	builder.flush()
	for i := range builder.steps {
		builder.steps[i].Seq = i + 1
	}
	plan.Steps = builder.steps
	return plan
}

func readmissionPreparations(input PlanInput, plan Plan, replacements []Replica) map[string]bool {
	plannedPreparation := make(map[string]bool, len(replacements))
	native := input.Native[MetaGroupID].ConfState
	for _, replacement := range replacements {
		// Native membership has already passed exact identity validation.
		// Scoped admission handover separately proves the consumed prefix.
		plannedPreparation[replacement.NodeName] = slices.Contains(native.Voters, replacement.ReplicaID) ||
			slices.Contains(native.Learners, replacement.ReplicaID)
	}
	for _, step := range plan.Steps {
		if step.GroupID != MetaGroupID || step.Kind != StepPrepareReplica {
			continue
		}
		for _, replacement := range replacements {
			if replacement.NodeName == step.NodeName && replacement.ReplicaID == step.ReplicaID {
				plannedPreparation[step.NodeName] = true
			}
		}
	}
	return plannedPreparation
}

type readmissionPlanBuilder struct {
	input             PlanInput
	desired           Document
	replacements      []Replica
	preserveAdmission map[string]bool
	emitted           map[string]bool
	steps             []Step
	metadataOwned     bool
}

func (p *readmissionPlanBuilder) replacement(node string) *Replica {
	for i := range p.replacements {
		if p.replacements[i].NodeName == node {
			return &p.replacements[i]
		}
	}
	return nil
}

func (p *readmissionPlanBuilder) emit(r Replica) {
	if p.emitted[r.NodeName] {
		return
	}
	p.emitted[r.NodeName] = true
	if !p.metadataOwned || p.preserveAdmission[r.NodeName] ||
		p.input.convergedMetaMember(r.NodeName) == r.ReplicaID {
		return // preserve the planner's paired waits and destructive ordering
	}
	kinds := []StepKind{StepPrepareReplica, StepAddLearner}
	if r.Role == ReplicaVoter {
		kinds = append(kinds, StepWaitCaughtUp, StepPromoteVoter)
	}
	for _, kind := range kinds {
		p.steps = append(p.steps, Step{GroupID: MetaGroupID, NodeName: r.NodeName, ReplicaID: r.ReplicaID, Kind: kind})
	}
}

func (p *readmissionPlanBuilder) flush() {
	for _, r := range p.replacements {
		p.emit(r)
	}
}

func (p *readmissionPlanBuilder) appendStep(step Step) {
	if step.GroupID != MetaGroupID {
		p.flush()
	}
	r := p.replacement(step.NodeName)
	if step.GroupID == MetaGroupID && r != nil && step.ReplicaID == r.ReplicaID {
		p.emit(*r)
		if !p.preserveAdmission[r.NodeName] && p.input.convergedMetaMember(r.NodeName) != r.ReplicaID &&
			slices.Contains([]StepKind{StepPrepareReplica, StepAddLearner, StepWaitCaughtUp,
				StepPromoteVoter}, step.Kind) {
			return
		}
	}
	deletingLoad := step.GroupID != MetaGroupID && p.desired.Group(step.GroupID) == nil
	if r != nil && !deletingLoad && (step.Kind == StepStopReplica || step.Kind == StepCleanupReplicaState) &&
		exactPlanReplica(p.desired, step) == nil {
		return
	}
	if step.Kind == StepDeleteGroupState && !p.requiresDeletion(step) {
		return
	}
	p.steps = append(p.steps, step)
}

func (p *readmissionPlanBuilder) requiresDeletion(step Step) bool {
	owners := p.input.formerReplicas(step.GroupID)
	return len(owners) == 0 || slices.ContainsFunc(owners, func(r RetainedReplica) bool {
		return r.Progress == RetainedHosted &&
			(p.desired.Group(step.GroupID) == nil || p.replacement(r.NodeName) == nil)
	})
}
