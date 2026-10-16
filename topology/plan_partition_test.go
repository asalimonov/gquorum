//go:build !functional

package topology

import (
	"reflect"
	"slices"
	"testing"
)

func TestScopedPartitionsDetachRemovalButKeepDeletionStops(t *testing.T) {
	t.Parallel()
	for _, deletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "replica removal", true: "whole group"}[deletion], func(t *testing.T) {
			t.Parallel()
			base := scopeDocument()
			base.Group("a").Replicas = append(base.Group("a").Replicas, learner("n3", 2))
			base.Group("a").NextReplicaID = 3
			desired := base.Clone()
			desired.Version++
			desired.Group("a").Replicas = desired.Group("a").Replicas[:1]
			if deletion {
				desired.Groups = slices.Delete(desired.Groups, 1, 2)
			}
			input := testPlanInput(base, statusForDocument(base))
			plan, err := scopedPlan(input, desired, PlanOptions{Rules: plannerRules()})
			if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].SourceVersion != base.Version {
				t.Fatal("lost scoped provenance", plan, err)
			}
			block := plan.Blocks[0]
			wantRequired, wantCleanup := []int{1}, []int{2, 3}
			if deletion {
				wantRequired, wantCleanup = []int{1, 2}, []int{3}
			}
			if !slices.Equal(block.Steps, wantRequired) || !slices.Equal(block.Cleanup, wantCleanup) {
				t.Fatal("wrong execution boundary", plan)
			}
			if err := validatePlanBlocks(plan); err != nil {
				t.Fatal("steps lost unique coverage or immutable sequence", err)
			}
		})
	}
}

func TestScopedPartitionsCarryRequiredDebtStopWithoutNewRetirement(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Retained["work"].Replicas[0].Progress = RetainedCleanup
	input.Retained["work"].Replicas[1].Progress = RetainedStopped
	desired.Group("work").Replicas[2] = learner("d", 6)
	status := groupStatus("work", 1, []uint64{1, 2}, nil)
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true}
	flat, err := retainedRetirementPlan(input, desired, status)
	if err != nil {
		t.Fatal(err)
	}
	plan := flat.Clone()
	bindPlanBlocks(input, desired, &plan, []string{"work"})
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(flat.Steps, plan.Steps) || !reflect.DeepEqual(flat.VoterBases, plan.VoterBases) {
		t.Fatal("partition rewrote exact actions or voter proofs")
	}
	for _, step := range plan.Steps {
		if step.ReplicaID == 4 && (step.Kind != StepStopReplica || !slices.Contains(plan.Blocks[0].Steps, step.Seq)) {
			t.Fatal("retained stop detached or debt got a second retirement", step)
		}
	}
	// Native presence of the successor does not establish predecessor stop.
	broken := Plan{Version: desired.Version, Steps: []Step{
		{Seq: 1, GroupID: "work", NodeName: "d", ReplicaID: 4, Kind: StepStopReplica}},
		Blocks: []PlanBlock{{GroupID: "work", SourceVersion: 7, Cleanup: []int{1}}}}
	if err := validateBlockSafety(desired, broken); err == nil {
		t.Fatal("already-native successor waived required predecessor stop")
	}
}

func TestScopedPlannerDetachesObsoleteRequiredStopOnce(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Approved.Group("work").Replicas[2] = learner("d", 6)
	input.Retained["work"].Replicas[0].Progress = RetainedCleanup
	input.Retained["work"].Replicas[1].Progress = RetainedStopped
	desired.Group("work").Replicas[2] = learner("e", 6)
	// Use a fresh ID for the newly selected host, preserving the former key.
	desired.Group("work").Replicas[2].ReplicaID = 7
	desired.Group("work").NextReplicaID = 8
	status := groupStatus("work", 1, []uint64{1, 2}, nil)
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true}
	input.Native = map[string]NativeState{"work": {GroupID: "work", ReplicaID: 1, Applied: 1, ConfState: status.ConfState}}
	row := input.Groups["work"]
	row.Observation = &status
	input.Groups["work"] = row
	plan, err := scopedPlan(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil {
		t.Fatal(err)
	}
	var stops int
	for _, step := range plan.Steps {
		if step.ReplicaID != 4 {
			continue
		}
		if step.Kind != StepStopReplica || !slices.Contains(plan.Blocks[0].Cleanup, step.Seq) {
			t.Fatal("relocation retained a required stop or recreated retirement", step)
		}
		stops++
	}
	if stops != 1 {
		t.Fatal("relocation lost the exact required-to-detached binding", plan)
	}
	input.Approved = desired.Clone()
	desired.Version++
	desired.Group("work").Labels = map[string]string{"owner": "next"}
	plan, err = scopedPlan(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if step.ReplicaID == 4 {
			t.Fatal("already detached debt acquired a second stop", step)
		}
	}
	native := input.Native["work"]
	native.ConfState.Voters = []uint64{1, 2, 4}
	input.Native["work"] = native
	if _, err := scopedPlan(input, desired, PlanOptions{Rules: plannerRules()}); err == nil {
		t.Fatal("detached debt hid a still-native replica")
	}
}

func TestScopedAdmissionEdgesCoverExactFoundersAndLearners(t *testing.T) {
	t.Parallel()
	for _, role := range []ReplicaRole{ReplicaVoter, ReplicaLearner} {
		t.Run(string(role), func(t *testing.T) {
			t.Parallel()
			base := planDoc(1, 3, []string{"n1"})
			desired := base.Clone()
			desired.Version++
			desired.Nodes = append(desired.Nodes, Node{Name: "n2", Type: "worker", State: NodeActive})
			desired.MetaGroup().Replicas = append(desired.MetaGroup().Replicas, learner("n2", 2))
			desired.MetaGroup().NextReplicaID = 3
			incoming := Replica{NodeName: "n2", ReplicaID: 2, Role: role}
			desired.Groups = append(desired.Groups, shardOf("work", rep("n1", 1), incoming))
			plan, err := scopedPlan(testPlanInput(base, statusForDocument(base)), desired, PlanOptions{Rules: plannerRules()})
			if err != nil || len(plan.Dependencies) != 1 || !slices.Equal(plan.Dependencies[0].Requires, []int{1}) {
				t.Fatal("admission edge lost exact metadata preparation", plan, err)
			}
			kind := StepCreateFoundingGroup
			if role == ReplicaLearner {
				kind = StepPrepareReplica
			}
			if plan.Steps[plan.Dependencies[0].StepSeq-1].Kind != kind {
				t.Fatal("learner admission became a founding prerequisite", plan)
			}
		})
	}
}

func TestScopedRemovalEdgeUsesRequiredEvacuation(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"n1", "n2", "n3"}, shardOf("work", rep("n3", 1)))
	desired := base.Clone()
	desired.Version++
	desired.Node("n3").State = NodeExcluded
	desired.MetaGroup().Replicas = desired.MetaGroup().Replicas[:2]
	desired.Groups = desired.Groups[:1]
	observed := statusForDocument(base)
	status := observed[MetaGroupID]
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 3: true}
	observed[MetaGroupID] = status
	plan, err := scopedPlan(testPlanInput(base, observed), desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Dependencies) != 1 || len(plan.Dependencies[0].Requires) != 1 {
		t.Fatal("decommission omitted exact stop barrier", plan, err)
	}
	edge := plan.Dependencies[0]
	if plan.Steps[edge.StepSeq-1].Kind != StepRemoveReplica ||
		plan.Steps[edge.Requires[0]-1].Kind != StepStopReplica ||
		!slices.Contains(plan.Blocks[1].Steps, edge.Requires[0]) {
		t.Fatal("detached retirement substituted for evacuation", plan)
	}
	// A replacement member must not wait for evacuation of its successor.
	desired.MetaGroup().Replicas = append(desired.MetaGroup().Replicas, learner("n3", 4))
	plan.Dependencies = nil
	bindPlanBarriers(desired, &plan)
	if len(plan.Dependencies) != 0 {
		t.Fatal("readmission was treated as whole-node decommission", plan)
	}
}

func TestScopedDeclarationBlockKeepsItsOwnSource(t *testing.T) {
	t.Parallel()
	base := scopeDocument()
	input := testPlanInput(base, statusForDocument(base))
	row := input.Groups["b"]
	row.ConvergedVersion = 3
	input.Groups["b"] = row
	desired := base.Clone()
	desired.Version++
	desired.Group("b").Labels = map[string]string{"edit": "yes"}
	plan, err := scopedPlan(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != "b" ||
		plan.Blocks[0].SourceVersion != 3 || len(plan.Steps) != 0 || plan.IsEmpty() {
		t.Fatal("zero-step block lost independent provenance", plan, err)
	}
}

func TestScopedDeletionUsesCompletedReadmissionWithoutOwningMetadata(t *testing.T) {
	t.Parallel()
	input, desired, _ := scopedReadmissionPlanFixture()
	input.Approved = desired.Clone()
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
		Converged: input.Approved.MetaGroup()}
	desired.Version++
	desired.Groups = desired.Groups[:1]
	plan, err := scopedPlan(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != "work" || len(plan.Steps) != 3 {
		t.Fatal("load-only deletion repeated metadata admission or contacted its retired predecessor", plan, err)
	}
	if plan.Steps[0].NodeName != "b" || plan.Steps[1].NodeName != "c" ||
		!slices.Equal(plan.Blocks[0].Steps, []int{1, 2}) || !slices.Equal(plan.Blocks[0].Cleanup, []int{3}) {
		t.Fatal("completed readmission waived unrelated host stops", plan)
	}
}
