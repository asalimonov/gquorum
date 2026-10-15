//go:build !functional

package topology

import (
	"strings"
	"testing"
)

func blockPlan() Plan {
	return Plan{Version: 2, Steps: []Step{
		{Seq: 1, GroupID: MetaGroupID, Kind: StepPrepareReplica, NodeName: "a", ReplicaID: 1},
		{Seq: 2, GroupID: MetaGroupID, Kind: StepRemoveReplica, NodeName: "b", ReplicaID: 2},
		{Seq: 3, GroupID: "work", Kind: StepPrepareReplica, NodeName: "a", ReplicaID: 3},
		{Seq: 4, GroupID: "work", Kind: StepRemoveReplica, NodeName: "b", ReplicaID: 4},
		{Seq: 5, GroupID: "work", Kind: StepStopReplica, NodeName: "b", ReplicaID: 4},
	}, Blocks: []PlanBlock{
		{GroupID: MetaGroupID, SourceVersion: 1, Steps: []int{1, 2}},
		{GroupID: "work", SourceVersion: 1, Steps: []int{3, 4}, Cleanup: []int{5}},
	}, Dependencies: []StepDependency{{StepSeq: 2, Requires: []int{4}}, {StepSeq: 3, Requires: []int{1}}}}
}

func TestPlanBlockValidation(t *testing.T) {
	t.Parallel()
	if err := validatePlanBlocks(blockPlan()); err != nil {
		t.Fatal(err)
	}
	declaration := Plan{Version: 2, Blocks: []PlanBlock{{GroupID: "work", SourceVersion: 1}}}
	if err := validatePlanBlocks(declaration); err != nil || declaration.IsEmpty() {
		t.Fatal("zero-step declaration lost its scope", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Plan)
	}{
		{"missing block", func(p *Plan) { p.Blocks = nil }},
		{"duplicate group", func(p *Plan) { p.Blocks[1].GroupID = MetaGroupID }},
		{"wrong group", func(p *Plan) { p.Steps[2].GroupID = "elsewhere" }},
		{"future source", func(p *Plan) { p.Blocks[0].SourceVersion = 3 }},
		{"missing step", func(p *Plan) { p.Blocks[1].Steps = []int{3} }},
		{"duplicate ownership", func(p *Plan) { p.Blocks[1].Cleanup = []int{4, 5} }},
		{"invalid reference", func(p *Plan) { p.Blocks[1].Steps[0] = 0 }},
		{"out of order", func(p *Plan) { p.Blocks[1].Steps = []int{4, 3} }},
		{"native cleanup", func(p *Plan) { p.Blocks[1].Steps, p.Blocks[1].Cleanup = []int{3}, []int{4, 5} }},
		{"cleanup prerequisite", func(p *Plan) { p.Dependencies[0].Requires = []int{5} }},
		{"cleanup dependent", func(p *Plan) { p.Dependencies[1].StepSeq = 5 }},
		{"missing prerequisite", func(p *Plan) { p.Dependencies[0].Requires = nil }},
		{"duplicate prerequisite", func(p *Plan) { p.Dependencies[0].Requires = []int{4, 4} }},
		{"unknown prerequisite", func(p *Plan) { p.Dependencies[0].Requires = []int{6} }},
		{"same-group edge", func(p *Plan) { p.Dependencies[0].Requires = []int{1} }},
		{"wrong admission node", func(p *Plan) { p.Steps[2].NodeName = "b" }},
		{"wrong removal node", func(p *Plan) { p.Steps[3].NodeName = "a" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan := blockPlan()
			tc.mutate(&plan)
			if err := validatePlanBlocks(plan); err == nil {
				t.Fatal("invalid partition or barrier accepted")
			}
		})
	}
}

func TestPlanBarrierCycleIncludesWithinBlockOrder(t *testing.T) {
	t.Parallel()
	plan := blockPlan()
	plan.Steps[0], plan.Steps[1] = plan.Steps[1], plan.Steps[0]
	plan.Steps[0].Seq, plan.Steps[1].Seq = 1, 2
	plan.Dependencies = []StepDependency{{StepSeq: 1, Requires: []int{4}}, {StepSeq: 3, Requires: []int{2}}}
	if err := validatePlanBlocks(plan); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatal("cyclic admission/removal ordering accepted", err)
	}
	if err := ValidatePlanGuardChain(plan); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatal("document-independent restore validator accepted the cycle", err)
	}
}

func TestPlanBlocksCloneAndEquality(t *testing.T) {
	t.Parallel()
	original := blockPlan()
	for _, change := range []func(*Plan){
		func(p *Plan) { p.Blocks[0].GroupID = "other" },
		func(p *Plan) { p.Blocks[0].SourceVersion++ },
		func(p *Plan) { p.Blocks[0].Steps[0]++ },
		func(p *Plan) { p.Blocks[1].Cleanup[0]++ },
		func(p *Plan) { p.Dependencies[0].StepSeq++ },
		func(p *Plan) { p.Dependencies[0].Requires[0]++ },
	} {
		owned := original.Clone()
		if !original.Equal(owned) {
			t.Fatal("clone changed immutable plan")
		}
		change(&owned)
		if original.Equal(owned) || !original.Equal(blockPlan()) {
			t.Fatal("equality omitted a field or clone aliased evidence")
		}
	}
}

func TestPlanBlocksKeepDeletionAndReplacementStopsRequired(t *testing.T) {
	t.Parallel()
	for _, replacement := range []bool{false, true} {
		plan := Plan{Version: 2, Steps: []Step{{Seq: 1, GroupID: "work", NodeName: "n1", ReplicaID: 1, Kind: StepStopReplica}},
			Blocks: []PlanBlock{{GroupID: "work", SourceVersion: 1, Cleanup: []int{1}}}}
		desired := Document{Version: 2}
		if replacement {
			desired.Groups = []Group{{ID: "work", Replicas: []Replica{{NodeName: "n1", ReplicaID: 2}}}}
			plan.Steps = append(plan.Steps, Step{Seq: 2, GroupID: "work", NodeName: "n1", ReplicaID: 2, Kind: StepPrepareReplica})
			plan.Blocks[0].Steps = []int{2}
		}
		if err := ValidatePlanShape(PlanInput{}, desired, plan); err == nil || !strings.Contains(err.Error(), "requires") {
			t.Fatalf("replacement=%v accepted detached required stop: %v", replacement, err)
		}
		plan.Blocks[0].Cleanup = nil
		plan.Blocks[0].Steps = append([]int{1}, plan.Blocks[0].Steps...)
		if err := validateBlockSafety(desired, plan); err != nil {
			t.Fatal("required stop refused", err)
		}
	}
}

func TestFoundingBarrierNamesAnActualFounder(t *testing.T) {
	t.Parallel()
	plan := Plan{Version: 2, Steps: []Step{
		{Seq: 1, GroupID: MetaGroupID, Kind: StepPrepareReplica, NodeName: "n1", ReplicaID: 1},
		{Seq: 2, GroupID: "work", Kind: StepCreateFoundingGroup},
	}, Blocks: []PlanBlock{{GroupID: MetaGroupID, SourceVersion: 1, Steps: []int{1}}, {GroupID: "work", Steps: []int{2}}},
		Dependencies: []StepDependency{{StepSeq: 2, Requires: []int{1}}}}
	desired := Document{Version: 2, Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "n2", ReplicaID: 2}}}}}
	if err := ValidatePlanShape(PlanInput{}, desired, plan); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatal("unrelated admission became a founding prerequisite", err)
	}
	desired.Groups[0].Replicas[0].NodeName = "n1"
	if err := validateBlockSafety(desired, plan); err != nil {
		t.Fatal("actual founder refused", err)
	}
	plan.Blocks[0].SourceVersion = plan.Version
	if err := validatePlanBlocks(plan); err == nil {
		t.Fatal("block claimed its own output as source")
	}
}
