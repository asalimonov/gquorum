//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func nativePlanningFixture() (PlanInput, Document) {
	input, desired := retainedPlanFixture()
	confirmed := testPlanInput(desired, statusForDocument(desired))
	input.Native = confirmed.Native
	status := groupStatus("work", 1, []uint64{1, 2, 6}, nil)
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 4: true}
	row := input.Groups["work"]
	row.Observation = &status
	input.Groups["work"] = row
	native := input.Native["work"]
	native.ConfState.Voters = []uint64{1, 2, 4}
	input.Native["work"] = native
	// Metadata and load declarations have independent convergence versions.
	meta := input.Groups[MetaGroupID]
	meta.ConvergedVersion = desired.Version
	input.Groups[MetaGroupID] = meta
	return input, desired
}

func TestBuildPlanSeparatesApprovedProvenAndNative(t *testing.T) {
	t.Parallel()
	input, desired := nativePlanningFixture()
	before, _ := json.Marshal(input)
	plan, err := BuildPlan(input, desired, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 5, MaxReplicas: 5}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{3, 4, 5} {
		want := []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState}
		if !slices.Equal(retainedStepKinds(plan, id), want) {
			t.Fatal("lost actual or retained former owner", id, plan)
		}
	}
	if !slices.Contains(retainedStepKinds(plan, 6), StepPrepareReplica) {
		t.Fatal("approved/cached membership was mistaken for native presence", plan)
	}
	after, _ := json.Marshal(input)
	if !slices.Equal(before, after) {
		t.Fatal("planner mutated caller evidence")
	}
}

func TestBuildPlanRequiresExactNativeEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"foreign group", "unknown reporter", "zero reporter", "zero applied", "missing"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			input, desired := nativePlanningFixture()
			state := input.Native["work"]
			want := fault.ErrUnsafeMembership
			switch mode {
			case "foreign group":
				state.GroupID = "foreign"
			case "unknown reporter":
				state.ReplicaID = 99
			case "zero reporter":
				state.ReplicaID = 0
			case "zero applied":
				state.Applied = 0
			case "missing":
				want = fault.ErrUnavailable
			}
			input.Native["work"] = state
			if mode == "missing" {
				delete(input.Native, "work")
			}
			_, err := BuildPlan(input, desired, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 5, MaxReplicas: 5}})
			if !errors.Is(err, want) {
				t.Fatal("native evidence was not checked", err)
			}
		})
	}
}
