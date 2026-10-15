//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func TestScopedPlanShapeDistinguishesUnknownAndDeletedGroup(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"n1"})
	desired := base.Clone()
	desired.Version++
	desired.Groups = append(desired.Groups, shardOf("work", rep("n1", 1)))
	observed := statusForDocument(base)
	observed["work"] = GroupStatus{GroupID: "work"}
	plan, err := BuildPlan(base, desired, observed, plannerRules())
	if err != nil {
		t.Fatal(err)
	}
	input := convergedPlanInput(base)
	input.Approved = desired.Clone()
	input.Groups["work"] = GroupProgress{GroupID: "work"}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("initial unknown group cannot be founded", err)
	}
	input.Groups["work"] = GroupProgress{GroupID: "work", ConvergedVersion: 2}
	if err := ValidatePlanShape(input, desired, plan); err == nil {
		t.Fatal("tombstoned group was refounded")
	}
	if err := validatePlannedSteps(input, desired, observed, plan); err == nil {
		t.Fatal("membership simulation refounded a tombstoned group")
	}
	input.Groups["work"] = GroupProgress{GroupID: "work"}
	observed["work"] = groupStatus("work", 1, []uint64{1}, nil)
	if err := validatePlannedSteps(input, desired, observed, plan); err == nil {
		t.Fatal("unknown checkpoint erased observed established membership")
	}
}

func TestScopedDeletionKeepsEveryFormerOwnerAfterApproval(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"n1", "n2"}, shardOf("work", rep("n1", 1), rep("n2", 2)))
	desired := base.Clone()
	desired.Version++
	desired.Groups = desired.Groups[:1]
	observed := statusForDocument(base)
	plan, err := BuildPlan(base, desired, observed, plannerRules())
	if err != nil {
		t.Fatal(err)
	}
	input := convergedPlanInput(base)
	input.Approved = desired.Clone()
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("new approval erased the retained former owners", err)
	}
	if err := validatePlannedSteps(input, desired, observed, plan); err != nil {
		t.Fatal(err)
	}
	incomplete := plan.Clone()
	incomplete.Steps = slices.DeleteFunc(incomplete.Steps, func(step Step) bool {
		return step.GroupID == "work" && step.Kind == StepStopReplica && step.ReplicaID == 2
	})
	for i := range incomplete.Steps {
		incomplete.Steps[i].Seq = i + 1
	}
	if err := ValidatePlanShape(input, desired, incomplete); err == nil {
		t.Fatal("deletion shape omitted a former host stop")
	}
	if err := validatePlannedSteps(input, desired, observed, incomplete); err == nil {
		t.Fatal("deletion simulation omitted a former host stop")
	}
}

func TestScopedReadmissionUsesExactProvenMember(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"a", "b", "c"}, shardOf("work", rep("a", 1), rep("b", 2), rep("c", 3)))
	desired := base.Clone()
	desired.Version++
	for i := range desired.Groups {
		desired.Groups[i].Replicas[0].ReplicaID, desired.Groups[i].NextReplicaID = 4, 5
	}
	observed := statusForDocument(base)
	for id, status := range observed {
		status.HealthKnown, status.LiveVoters = true, map[uint64]bool{}
		for _, voter := range status.ConfState.Voters {
			status.LiveVoters[voter] = true
		}
		observed[id] = status
	}
	plan, err := BuildPlan(base, desired, observed, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 4, MaxReplicas: 5}})
	if err != nil {
		t.Fatal(err)
	}
	input := convergedPlanInput(base)
	input.Approved = desired.Clone()
	if !hasReadmissionBoundary(input, desired, plan, "a") {
		t.Fatal("new approved member hid the proven predecessor")
	}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("exact readmission lost its original host boundary", err)
	}
	previousMeta := input.Groups[MetaGroupID]
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID, ConvergedVersion: desired.Version,
		Converged: desired.MetaGroup()}
	if !hasReadmissionBoundary(input, desired, plan, "a") {
		t.Fatal("converged metadata erased the historical load-owner retirement boundary")
	}
	input.Groups[MetaGroupID] = previousMeta
	row := input.Nodes["a"]
	row.MemberID = 4
	input.Nodes["a"] = row
	if hasReadmissionBoundary(input, desired, plan, "a") {
		t.Fatal("same member became a replacement boundary")
	}
	row.MemberID, row.ConvergedVersion = 1, 0
	input.Nodes["a"] = row
	if !hasReadmissionBoundary(input, desired, plan, "a") {
		t.Fatal("unconverged node hid its exact converged meta member")
	}
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID}
	if hasReadmissionBoundary(input, desired, plan, "a") {
		t.Fatal("unknown node checkpoint supplied retirement proof")
	}
}
