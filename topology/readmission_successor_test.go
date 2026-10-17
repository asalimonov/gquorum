//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func TestPlanKeepsPendingReadmissionWithItsMetadataOwner(t *testing.T) {
	t.Parallel()
	input, desired := pendingReadmissionInput()
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil {
		t.Fatal(err)
	}
	if !ReadmissionCovers(input, desired, plan, "a") {
		t.Fatal("scoped plan lost its independently owned admission")
	}
	broken := plan.Clone()
	broken.Blocks, broken.Dependencies = nil, nil
	if ReadmissionCovers(input, desired, broken, "a") {
		t.Fatal("flat plan borrowed an operation it supersedes")
	}
	if err := ValidatePlanShape(input, desired, broken); err == nil {
		t.Fatal("flat shape omitted both admission and the former host stop")
	}
	meta := input.Groups[MetaGroupID].Observation
	meta.HealthKnown, meta.LiveVoters = true, map[uint64]bool{1: true, 2: true, 3: true}
	input.Native[MetaGroupID] = NativeState{GroupID: MetaGroupID, ReplicaID: 1, Applied: 1,
		ConfState: meta.ConfState}
	ordinary, err := BuildPlan(input, desired, plannerRules())
	if err != nil || !ordinary.Equal(plan) {
		t.Fatal("ordinary planner took over unchanged metadata admission", ordinary, err)
	}
	if err := ValidatePlanShape(input, desired, ordinary); err != nil {
		t.Fatal(err)
	}
}

func TestScopedReadmissionPreservesNativeAdmissionSuffix(t *testing.T) {
	t.Parallel()
	for _, role := range []ReplicaRole{ReplicaLearner, ReplicaVoter} {
		t.Run(string(role), func(t *testing.T) {
			t.Parallel()
			input, desired, _ := scopedReadmissionPlanFixture()
			statuses := statusForDocument(input.Approved)
			meta := statuses[MetaGroupID]
			meta.HealthKnown, meta.LeaderID = true, 1
			meta.LiveVoters = map[uint64]bool{1: true, 2: true, 3: true, 4: true}
			if role == ReplicaLearner {
				meta.ConfState.Learners = []uint64{4}
			} else {
				meta.ConfState.Voters = append(meta.ConfState.Voters, 4)
			}
			statuses[MetaGroupID] = meta
			input = testPlanInput(input.Approved, statuses)
			input.Approved = desired.Clone()
			desired.Version++
			desired.MetaGroup().Labels = map[string]string{"edit": "metadata successor"}
			rules := Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 4, MaxReplicas: 5}}
			plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: rules})
			if err != nil {
				t.Fatal(err)
			}
			var kinds []StepKind
			for _, step := range plan.Steps {
				if step.GroupID != MetaGroupID {
					t.Fatal("metadata edit took load ownership", step)
				}
				kinds = append(kinds, step.Kind)
			}
			want := []StepKind{StepTransferLeader, StepRemoveReplica}
			if role == ReplicaLearner {
				want = append([]StepKind{StepWaitCaughtUp, StepPromoteVoter}, want...)
			}
			if !slices.Equal(kinds, want) {
				t.Fatalf("native %s replanned consumed admission: got %v, want %v", role, kinds, want)
			}
			if err := ValidatePlanShape(input, desired, plan); err != nil {
				t.Fatal(err)
			}
		})
	}
}
