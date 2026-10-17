package topology

import "slices"

func validatePlanVoterProof(base PlanInput, desired Document, plan Plan) error {
	required, err := requiredProofGroups(plan)
	if err != nil {
		return err
	}
	if len(required) != len(plan.VoterBases) {
		return planUnsafef("plan has incomplete or extraneous voter bases")
	}
	p := planVoterProof{base: base, desired: desired, sets: make(map[string]map[uint64]struct{}, len(required))}
	last := ""
	for _, entry := range plan.VoterBases {
		if !required[entry.GroupID] || last != "" && comparePlanGroups(entry.GroupID, last) <= 0 {
			return planUnsafef("plan voter bases are duplicate or out of order")
		}
		last = entry.GroupID
		set, err := p.validateBase(entry)
		if err != nil {
			return err
		}
		p.sets[entry.GroupID] = set
	}
	for _, step := range plan.Steps {
		if err := p.validateStep(step); err != nil {
			return err
		}
	}
	for id, set := range p.sets {
		if err := p.validateFinalGroup(id, set); err != nil {
			return err
		}
	}

	return nil
}

type planVoterProof struct {
	base    PlanInput
	desired Document
	sets    map[string]map[uint64]struct{}
}

func (p *planVoterProof) validateBase(entry PlanVoterBase) (map[uint64]struct{}, error) {
	if !slices.IsSorted(entry.Voters) || len(slices.Compact(slices.Clone(entry.Voters))) != len(entry.Voters) {
		return nil, planUnsafef("group %q has a noncanonical voter base", entry.GroupID)
	}
	set := make(map[uint64]struct{}, len(entry.Voters))
	for _, id := range entry.Voters {
		if id == 0 {
			return nil, planUnsafef("group %q voter base contains zero ID", entry.GroupID)
		}
		if _, ok := p.base.planReplicaKey(entry.GroupID, p.desired.Group(entry.GroupID), id); !ok {
			return nil, planUnsafef("group %q voter base has unknown replica %d", entry.GroupID, id)
		}
		set[id] = struct{}{}
	}
	if p.base.foundingGroup(entry.GroupID) {
		if len(set) != 0 {
			return nil, planUnsafef("founding group %q has a nonempty voter base", entry.GroupID)
		}
	} else if len(set) == 0 {
		return nil, planUnsafef("established group %q has an empty voter base", entry.GroupID)
	}
	state := voterPlanState{base: p.base, desired: p.desired, groupID: entry.GroupID}
	if err := validateOldDomains(p.base, p.desired, entry.GroupID, voterKeysForSet(state, set)); err != nil {
		return nil, err
	}
	return set, nil
}

func (p *planVoterProof) validateTransfer(step Step) error {
	if step.Kind == StepTransferLeader {
		if set := p.sets[step.GroupID]; set != nil {
			if _, voter := set[step.ReplicaID]; !voter {
				return planUnsafef("step %d transfers to a nonvoter", step.Seq)
			}
		}
	}

	return nil
}

func (p *planVoterProof) validateStep(step Step) error {
	if !isMembershipStep(step.Kind) {
		return p.validateTransfer(step)
	}

	set := p.sets[step.GroupID]
	if set == nil || step.ExpectedVotersDigest == "" || step.ResultVotersDigest == "" ||
		step.ExpectedVotersDigest != digestIDSet(set) {
		return planUnsafef("step %d has missing or mismatched predecessor voter proof", step.Seq)
	}
	before := cloneIDSet(set)
	if err := p.applyStep(step, set); err != nil {
		return err
	}
	if step.ResultVotersDigest != digestIDSet(set) {
		return planUnsafef("step %d has a mismatched result voter proof", step.Seq)
	}
	state := voterPlanState{base: p.base, desired: p.desired, groupID: step.GroupID}
	view := voterPlanTopology{base: p.base, desired: p.desired, groupID: step.GroupID}
	checkErr := validateVoterTransition(view, voterKeysForSet(state, before),
		voterKeysForSet(state, set), state)
	if checkErr != nil {
		return checkErr
	}
	return nil
}

func (p *planVoterProof) applyStep(step Step, set map[uint64]struct{}) error {
	switch step.Kind {
	case StepCreateFoundingGroup:
		if len(set) != 0 || p.desired.Group(step.GroupID) == nil {
			return planUnsafef("founding step %d has an invalid voter base", step.Seq)
		}
		for _, replica := range p.desired.VotersOf(step.GroupID) {
			set[replica.ReplicaID] = struct{}{}
		}
	case StepPromoteVoter:
		// The same rule applies to a previously completed promotion.
		set[step.ReplicaID] = struct{}{}
	case StepDemoteVoter:
		if _, voter := set[step.ReplicaID]; !voter {
			return planUnsafef("step %d demotes a nonvoter", step.Seq)
		}
		if _, voter := set[step.TransferReplicaID]; !voter || step.TransferReplicaID == step.ReplicaID {
			return planUnsafef("step %d has no current retained demotion guard", step.Seq)
		}
		delete(set, step.ReplicaID)
	case StepRemoveReplica:
		delete(set, step.ReplicaID)
	default: // Adding learners and other steps preserve the simulated voter set.
	}
	return nil
}

func (p *planVoterProof) validateFinalGroup(id string, set map[uint64]struct{}) error {
	group := p.desired.Group(id)
	if group == nil {
		if len(set) != 0 {
			return planUnsafef("deleted group %q retains voters", id)
		}
		return nil
	}
	if !sameVoterIDs(set, p.desired.VotersOf(id)) {
		return planUnsafef("group %q plan does not end at desired voters", id)
	}
	state := voterPlanState{base: p.base, desired: p.desired, groupID: id}
	violations, err := targetVoterViolations(p.base, p.desired, id, voterKeysForSet(state, set))
	if err != nil {
		return err
	}
	if len(violations) != 0 {
		return planUnsafef("group %q final placement remains unsatisfied", id)
	}
	return nil
}

func requiredProofGroups(plan Plan) (map[string]bool, error) {
	required := make(map[string]bool)
	for _, step := range plan.Steps {
		if isMembershipStep(step.Kind) {
			required[step.GroupID] = true
		} else if step.ExpectedVotersDigest != "" || step.ResultVotersDigest != "" {
			return nil, planUnsafef("nonmembership step %d carries a voter guard", step.Seq)
		}
	}

	return required, nil
}
