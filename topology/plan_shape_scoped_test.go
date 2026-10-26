//go:build !functional

package topology

import "testing"

func TestValidatePlanShapeRequiresEveryDeletionObligation(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"a"}, shardOf("work", rep("a", 1)))
	desired := base.Clone()
	desired.Version++
	desired.Groups = desired.Groups[:1]
	input := convergedPlanInput(base)
	input.Approved = desired.Clone() // Approval cannot replace the older proof.
	stop := Step{Seq: 1, GroupID: "work", NodeName: "a", ReplicaID: 1, Kind: StepStopReplica}
	retire := Step{Seq: 2, GroupID: "work", Kind: StepDeleteGroupState}
	plan := Plan{Version: desired.Version, Steps: []Step{stop, retire}}
	bindPlanBlocks(input, desired, &plan, []string{"work"})
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("complete deletion lost its independently proven source", err)
	}
	for _, steps := range [][]Step{nil, {stop}, {{Seq: 1, GroupID: "work", Kind: StepDeleteGroupState}}} {
		plan.Steps = steps
		bindPlanBlocks(input, desired, &plan, []string{"work"})
		if err := ValidatePlanShape(input, desired, plan); err == nil {
			t.Fatal("shape accepted deletion without every stop and retirement obligation", steps)
		}
	}
	input.Retained = map[string]RetainedGroup{"work": {Replicas: []RetainedReplica{
		{ReplicaKey: ReplicaKey{NodeName: "a", ReplicaID: 1}, Progress: RetainedStopped}}}}
	if err := ValidatePlanShape(input, desired, Plan{Version: desired.Version, Blocks: []PlanBlock{{GroupID: "work", SourceVersion: base.Version}}}); err != nil {
		t.Fatal("proven stopped debt required a second retirement action", err)
	}
}

func TestValidatePlanShapeRejectsUnpartitionedEffects(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"a"}, shardOf("work", rep("a", 1)))
	desired := base.Clone()
	desired.Version++
	desired.Groups = desired.Groups[:1]
	input := convergedPlanInput(base)
	plan := Plan{Version: desired.Version, Steps: []Step{
		{Seq: 1, GroupID: "work", NodeName: "a", ReplicaID: 1, Kind: StepStopReplica},
		{Seq: 2, GroupID: "work", Kind: StepDeleteGroupState},
	}}
	if err := ValidatePlanShape(input, desired, plan); err == nil {
		t.Fatal("accepted effects without execution/cleanup ownership")
	}
	plan.Blocks = []PlanBlock{{GroupID: "work", SourceVersion: 1, Steps: []int{1}, Cleanup: []int{2}}}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("complete scoped deletion rejected", err)
	}
}

func TestValidatePlanShapeDeletionPartitionsPreserveExactCoverage(t *testing.T) {
	t.Parallel()
	for _, detached := range []bool{false, true} {
		name := "required"
		if detached {
			name = "cleanup"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base := planDoc(1, 3, []string{"a", "b"}, shardOf("work", rep("a", 1), rep("b", 2)))
			desired := base.Clone()
			desired.Version++
			desired.Groups = desired.Groups[:1]
			input := convergedPlanInput(base)
			// The metadata FSM separately validates the actual debt behind this
			// planning declaration. This owner needs no duplicate retirement.
			input.Retained = map[string]RetainedGroup{"work": {Replicas: []RetainedReplica{
				{ReplicaKey: ReplicaKey{NodeName: "b", ReplicaID: 2}, Progress: RetainedCleanup},
			}}}
			plan := Plan{Version: desired.Version, Steps: []Step{
				{Seq: 1, GroupID: "work", NodeName: "a", ReplicaID: 1, Kind: StepStopReplica},
				{Seq: 2, GroupID: "work", Kind: StepDeleteGroupState},
			}, Blocks: []PlanBlock{{GroupID: "work", SourceVersion: base.Version, Steps: []int{1}, Cleanup: []int{2}}}}
			if detached {
				plan.Blocks[0].Steps, plan.Blocks[0].Cleanup = nil, []int{1, 2}
			}
			if err := ValidatePlanShape(input, desired, plan); err != nil {
				t.Fatal("exact deletion coverage refused", err)
			}
			for _, mode := range []string{"missing hosted stop", "foreign stop", "retirement first"} {
				broken := plan.Clone()
				switch mode {
				case "missing hosted stop":
					broken.Steps = []Step{{Seq: 1, GroupID: "work", Kind: StepDeleteGroupState}}
					broken.Blocks[0].Steps, broken.Blocks[0].Cleanup = nil, []int{1}
				case "foreign stop":
					broken.Steps[0].ReplicaID = 99
				case "retirement first":
					broken.Steps[0], broken.Steps[1] = broken.Steps[1], broken.Steps[0]
					broken.Steps[0].Seq, broken.Steps[1].Seq = 1, 2
					broken.Blocks[0].Steps, broken.Blocks[0].Cleanup = nil, []int{1, 2}
				}
				if err := ValidatePlanShape(input, desired, broken); err == nil {
					t.Fatal("accepted deletion without exact ordered coverage", mode)
				}
			}
		})
	}
}
