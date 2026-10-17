//go:build !functional

package topology

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

func pendingReadmissionInput() (PlanInput, Document) {
	input, desired, _ := scopedReadmissionPlanFixture()
	status := statusForDocument(input.Approved)
	work := status["work"]
	work.HealthKnown, work.LiveVoters = true, map[uint64]bool{1: true, 2: true, 3: true}
	status["work"] = work
	input = testPlanInput(input.Approved, status)
	input.Approved = desired.Clone()
	delete(input.Native, MetaGroupID) // Another operation owns metadata; no native read required.
	desired.Version++
	desired.Group("work").Labels = map[string]string{"edit": "new load approval"}
	return input, desired
}

func TestLoadOnlyPlanKeepsPendingReadmissionRetirement(t *testing.T) {
	t.Parallel()
	for _, stopped := range []bool{false, true} {
		t.Run(fmt.Sprint(stopped), func(t *testing.T) {
			t.Parallel()
			input, desired := pendingReadmissionInput()
			if stopped {
				input.Retained = map[string]RetainedGroup{"work": {Replicas: []RetainedReplica{{
					ReplicaKey: ReplicaKey{NodeName: "a", ReplicaID: 1}, Progress: RetainedStopped}}}}
			}
			before, _ := json.Marshal(input)
			plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
			if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != "work" {
				t.Fatal("load-only approval captured pending metadata admission", plan, err)
			}
			if !ReadmissionCovers(input, desired, plan, "a") || len(plan.Dependencies) != 0 {
				t.Fatal("pending exact admission ownership was lost or became an invented edge", plan)
			}
			remove, prepare := false, false
			for _, step := range plan.Steps {
				if step.GroupID != "work" || step.NodeName == "a" &&
					(step.Kind == StepStopReplica || step.Kind == StepCleanupReplicaState) {
					t.Fatal("successor called the retired predecessor or synthesized metadata work", step)
				}
				remove = remove || step.Kind == StepRemoveReplica && step.ReplicaID == 1
				prepare = prepare || step.Kind == StepPrepareReplica && step.ReplicaID == 4
			}
			if !remove || !prepare {
				t.Fatal("readmission retirement hid necessary native or successor work", plan)
			}
			if err := ValidatePlanShape(input, desired, plan); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(input)
			if !bytes.Equal(before, after) {
				t.Fatal("binding mutated the planning input")
			}
		})
	}
}

func TestPendingReadmissionDeletionKeepsOtherHostsAndNoSyntheticAdmission(t *testing.T) {
	t.Parallel()
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprint(all), func(t *testing.T) {
			t.Parallel()
			input, desired := pendingReadmissionInput()
			desired.Groups = desired.Groups[:1]
			if all {
				input.Groups["work"].Converged.Replicas = input.Groups["work"].Converged.Replicas[:1]
				native := input.Native["work"]
				native.ConfState.Voters = []uint64{1}
				input.Native["work"] = native
				input.Groups["work"].Observation.ConfState = native.ConfState
			}
			plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
			if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != "work" {
				t.Fatal("pending retirement prevented scoped deletion", plan, err)
			}
			if all {
				// This is ownership selection only. FSM completion must still wait
				// for the exact readmission receipt even though no step can call a/1.
				if len(plan.Steps) != 0 || plan.IsEmpty() {
					t.Fatal("readmission-owned deletion lost its zero-step block", plan)
				}
				return
			}
			if len(plan.Steps) != 3 || plan.Steps[0].NodeName != "b" || plan.Steps[1].NodeName != "c" ||
				plan.Steps[2].Kind != StepDeleteGroupState || !slices.Equal(plan.Blocks[0].Steps, []int{1, 2}) {
				t.Fatal("readmission waived another host's retirement", plan)
			}
		})
	}
}

func TestPendingReadmissionNeedsExactApprovedReplacement(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unapproved", "another replacement", "foreign cluster", "same owner"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			input, desired := pendingReadmissionInput()
			switch mode {
			case "unapproved":
				input.Approved.MetaGroup().Replicas[0].ReplicaID = 1
			case "another replacement":
				input.Approved.MetaGroup().Replicas[0].ReplicaID = 5
			case "foreign cluster":
				input.Approved.ClusterID = "other"
			case "same owner":
				node := input.Nodes["a"]
				node.MemberID = 4
				input.Nodes["a"] = node
			}
			if ReadmissionCovers(input, desired, Plan{Blocks: []PlanBlock{}}, "a") {
				t.Fatal("unrelated intent supplied a retirement owner")
			}
		})
	}
}
