package topology

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
)

// ReadmissionCovers reports whether the plan's exact replacement metadata
// preparation is the retirement boundary for this former host. The metadata FSM
// must require all readmission receipts before granting that preparation.
func ReadmissionCovers(active, desired Document, plan Plan, node string) bool {
	return hasReadmissionBoundary(convergedPlanInput(active), desired, plan, node)
}

func hasReadmissionBoundary(input PlanInput, desired Document, plan Plan, node string) bool {
	member := desired.MemberID(node)
	previous, known := input.loadOwnerMember(node)
	metaMember := input.convergedMetaMember(node)
	if !known || member == 0 || member <= previous || metaMember > member {
		return false
	}
	if metaMember == member {
		return true // Exact convergence includes the receipt-gated admission.
	}
	replica := exactPlanReplica(desired, Step{GroupID: MetaGroupID, NodeName: node, ReplicaID: member})
	return replica != nil && ValidAdmissionSequence(plan.Steps, node, member, replica.Role)
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
func bindReadmissionRetirement(input PlanInput, desired Document, plan Plan) Plan {
	meta := desired.MetaGroup()
	if meta == nil {
		return plan
	}
	var replacements []Replica
	for _, r := range sortedReplicas(meta.Replicas) {
		previous, known := input.loadOwnerMember(r.NodeName)
		if known && r.ReplicaID > previous {
			replacements = append(replacements, r)
		}
	}
	if len(replacements) == 0 {
		return plan
	}
	builder := readmissionPlanBuilder{input: input, desired: desired, replacements: replacements,
		plannedPreparation: readmissionPreparations(plan, replacements), emitted: make(map[string]bool)}
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

func readmissionPreparations(plan Plan, replacements []Replica) map[string]bool {
	plannedPreparation := make(map[string]bool, len(replacements))
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
	input              PlanInput
	desired            Document
	replacements       []Replica
	plannedPreparation map[string]bool
	emitted            map[string]bool
	steps              []Step
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
	if p.plannedPreparation[r.NodeName] || p.input.convergedMetaMember(r.NodeName) == r.ReplicaID {
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
		if !p.plannedPreparation[r.NodeName] && p.input.convergedMetaMember(r.NodeName) != r.ReplicaID &&
			slices.Contains([]StepKind{StepPrepareReplica, StepAddLearner, StepWaitCaughtUp,
				StepPromoteVoter}, step.Kind) {
			return
		}
	}
	if r != nil && (step.Kind == StepStopReplica || step.Kind == StepCleanupReplicaState) &&
		exactPlanReplica(p.desired, step) == nil {
		return
	}
	if step.Kind == StepDeleteGroupState && !p.requiresDeletion(step) {
		return
	}
	p.steps = append(p.steps, step)
}

func (p *readmissionPlanBuilder) requiresDeletion(step Step) bool {
	if step.Kind == StepDeleteGroupState {
		old := p.input.provenGroup(step.GroupID)
		if old != nil &&
			!slices.ContainsFunc(old.Replicas, func(r Replica) bool { return p.replacement(r.NodeName) == nil }) {
			return false
		}
	}
	return true
}
