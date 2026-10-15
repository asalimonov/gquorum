//go:build !functional

package topology

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func scopedReadmissionPlanFixture() (PlanInput, Document, Plan) {
	base := planDoc(1, 3, []string{"a", "b", "c"}, shardOf("work", rep("a", 1), rep("b", 2), rep("c", 3)))
	desired := base.Clone()
	desired.Version++
	for i := range desired.Groups {
		desired.Groups[i].Replicas[0].ReplicaID, desired.Groups[i].NextReplicaID = 4, 5
	}
	plan := Plan{Version: desired.Version}
	for i, kind := range []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState} {
		plan.Steps = append(plan.Steps, Step{Seq: i + 1, GroupID: "work", NodeName: "a", ReplicaID: 1, Kind: kind})
	}
	return convergedPlanInput(base), desired, plan
}

func TestReadmissionReplanAfterMetaConvergence(t *testing.T) {
	t.Parallel()
	input, desired, loadPlan := scopedReadmissionPlanFixture()
	original := bindReadmissionRetirement(input, desired, loadPlan)
	if !ReadmissionCovers(input, desired, original, "a") {
		t.Fatal("original admission did not cover its historical load owner")
	}
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
		Converged: desired.MetaGroup()}
	if !ReadmissionCovers(input, desired, original, "a") {
		t.Fatal("meta convergence invalidated the immutable admission boundary")
	}
	replanned := bindReadmissionRetirement(input, desired, loadPlan)
	if !slices.Equal(replanned.Steps, loadPlan.Steps[:1]) {
		t.Fatal("load replan repeated metadata admission or dispatched to the retired host", replanned)
	}
	if !ReadmissionCovers(input, desired, replanned, "a") {
		t.Fatal("load-only replan lost the completed admission boundary")
	}
	if len(loadPlan.Steps) != 3 || loadPlan.Steps[1].Kind != StepStopReplica {
		t.Fatal("binding mutated the caller's plan")
	}
}

func TestReadmissionReplanPreservesOrdinaryMetaPromotion(t *testing.T) {
	t.Parallel()
	input, desired, plan := scopedReadmissionPlanFixture()
	converged := desired.Clone()
	converged.MetaGroup().Replicas[0].Role = ReplicaLearner
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
		Converged: converged.MetaGroup()}
	steps := []Step{
		{Seq: 1, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepWaitCaughtUp},
		{Seq: 2, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepPromoteVoter},
	}
	plan.Steps = append(steps, plan.Steps...)
	bound := bindReadmissionRetirement(input, desired, plan)
	if len(bound.Steps) != 3 || !slices.Equal(bound.Steps[:2], steps) || bound.Steps[2].Kind != StepRemoveReplica {
		t.Fatal("completed admission suppressed a distinct voter-role change", bound)
	}
}

func TestReadmissionConvergenceCoversOnlyExactDeletionHosts(t *testing.T) {
	t.Parallel()
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			t.Parallel()
			input, desired, plan := scopedReadmissionPlanFixture()
			desired.Groups = desired.Groups[:1]
			input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
				Converged: desired.MetaGroup()}
			old := input.Groups["work"].Converged
			old.Replicas = old.Replicas[:1]
			plan.Steps = []Step{{GroupID: "work", NodeName: "a", ReplicaID: 1, Kind: StepStopReplica}}
			if partial {
				old.Replicas = append(old.Replicas, rep("b", 2))
				plan.Steps = append(plan.Steps, Step{GroupID: "work", NodeName: "b", ReplicaID: 2, Kind: StepStopReplica})
			}
			plan.Steps = append(plan.Steps, Step{GroupID: "work", Kind: StepDeleteGroupState})
			bound := bindReadmissionRetirement(input, desired, plan)
			if partial {
				if len(bound.Steps) != 2 || bound.Steps[0].NodeName != "b" || bound.Steps[1].Kind != StepDeleteGroupState {
					t.Fatal("completed readmission waived another host's deletion", bound)
				}
			} else if len(bound.Steps) != 0 {
				t.Fatal("completed readmission repeated retired host deletion", bound)
			}
		})
	}
}

func TestReadmissionBoundaryRequiresProvenReplacement(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unknown node", "zero node", "unknown meta", "same member", "ahead meta"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			input, desired, plan := scopedReadmissionPlanFixture()
			meta := desired.Clone()
			input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
				Converged: meta.MetaGroup()}
			node := input.Nodes["a"]
			switch mode {
			case "unknown node":
				node.ConvergedVersion = 0
			case "zero node":
				node.MemberID = 0
			case "unknown meta":
				row := input.Groups[MetaGroupID]
				row.ConvergedVersion = 0
				input.Groups[MetaGroupID] = row
			case "same member":
				node.MemberID = 4
			case "ahead meta":
				meta.MetaGroup().Replicas[0].ReplicaID = 5
			}
			input.Nodes["a"] = node
			if ReadmissionCovers(input, desired, plan, "a") {
				t.Fatal("unproven or non-replacement owner supplied retirement coverage")
			}
		})
	}
}

func TestReadmissionPlanningRefusesStaleMemberIntent(t *testing.T) {
	t.Parallel()
	input, desired, plan := scopedReadmissionPlanFixture()
	meta := desired.Clone()
	meta.MetaGroup().Replicas[0].ReplicaID, meta.MetaGroup().NextReplicaID = 5, 6
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
		Converged: meta.MetaGroup()}
	if err := ValidatePlanShape(input, desired, plan); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("shape accepted a metadata identity older than convergence", err)
	}
	if _, err := BuildPlan(testPlanInput(meta, statusForDocument(meta)), desired, plannerRules()); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("planning accepted a metadata identity older than convergence", err)
	}
	desired.MetaGroup().Replicas[0].ReplicaID, desired.MetaGroup().NextReplicaID = 6, 7
	if err := validatePlanMembers(input, desired); err != nil {
		t.Fatal("a newer requested readmission was mistaken for stale intent", err)
	}
}
