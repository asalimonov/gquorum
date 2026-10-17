package topology

import "slices"

// Clone copies every immutable plan component, including nested sequence/voter lists.
func (p Plan) Clone() Plan {
	owned := p
	owned.Steps = slices.Clone(p.Steps)
	owned.VoterBases = slices.Clone(p.VoterBases)
	for i := range owned.VoterBases {
		owned.VoterBases[i].Voters = slices.Clone(p.VoterBases[i].Voters)
	}
	owned.Blocks = slices.Clone(p.Blocks)
	for i := range owned.Blocks {
		owned.Blocks[i].Steps = slices.Clone(p.Blocks[i].Steps)
		owned.Blocks[i].Cleanup = slices.Clone(p.Blocks[i].Cleanup)
	}
	owned.Dependencies = slices.Clone(p.Dependencies)
	for i := range owned.Dependencies {
		owned.Dependencies[i].Requires = slices.Clone(p.Dependencies[i].Requires)
	}
	return owned
}

// Equal compares version, ordered steps, voter bases, blocks and dependencies.
func (p Plan) Equal(other Plan) bool {
	return p.Version == other.Version && slices.Equal(p.Steps, other.Steps) &&
		slices.EqualFunc(p.VoterBases, other.VoterBases, equalPlanVoterBase) &&
		slices.EqualFunc(p.Blocks, other.Blocks, equalPlanBlock) &&
		slices.EqualFunc(p.Dependencies, other.Dependencies, equalStepDependency)
}

func equalPlanVoterBase(a, b PlanVoterBase) bool {
	return a.GroupID == b.GroupID && slices.Equal(a.Voters, b.Voters)
}

func equalPlanBlock(a, b PlanBlock) bool {
	return a.GroupID == b.GroupID && a.SourceVersion == b.SourceVersion &&
		slices.Equal(a.Steps, b.Steps) && slices.Equal(a.Cleanup, b.Cleanup)
}

func equalStepDependency(a, b StepDependency) bool {
	return a.StepSeq == b.StepSeq && slices.Equal(a.Requires, b.Requires)
}

// IsEmpty reports whether the plan contains neither actions nor declaration blocks.
func (p Plan) IsEmpty() bool { return len(p.Steps) == 0 && len(p.Blocks) == 0 }
