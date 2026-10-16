//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func retainedRetirementPlan(input PlanInput, desired Document, status GroupStatus) (Plan, error) {
	observed := map[string]GroupStatus{"work": status}
	if err := observedVoterIdentities(input, desired, observed); err != nil {
		return Plan{}, err
	}
	builder := topologyPlanBuilder{input: input, desired: desired, observed: observed,
		options: PlanOptions{Rules: Rules{Limits: Limits{MaxNodes: 5, MaxReplicas: 5}}},
		plan:    Plan{Version: desired.Version}}
	if err := builder.planGroup("work"); err != nil {
		return Plan{}, err
	}
	plan := bindReadmissionRetirement(input, desired, builder.plan, unionGroupIDs(input, desired))
	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
	}
	if err := bindVoterGuards(&plan, observed, desired); err != nil {
		return Plan{}, err
	}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		return Plan{}, err
	}
	return plan, validatePlannedSteps(input, desired, observed, plan)
}

func retainedStepKinds(plan Plan, id uint64) []StepKind {
	var kinds []StepKind
	for _, step := range plan.Steps {
		if step.GroupID == "work" && step.ReplicaID == id {
			kinds = append(kinds, step.Kind)
		}
	}
	return kinds
}

func TestRetainedRetirementCoversNativeAndPreparedOwners(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	status := groupStatus("work", 1, []uint64{1, 2, 4}, nil)
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 4: true}
	plan, err := retainedRetirementPlan(input, desired, status)
	if err != nil {
		t.Fatal(err)
	}
	want := []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState}
	for _, id := range []uint64{3, 4, 5} {
		if got := retainedStepKinds(plan, id); !slices.Equal(got, want) {
			t.Fatal("native/prepared former owner lost exact retirement", id, got)
		}
	}
	// An immutable approved plan remains shape-valid after host work completes;
	// the collector preserves its original exact identities until proof pruning.
	for i := range input.Retained["work"].Replicas {
		input.Retained["work"].Replicas[i].Progress = RetainedStopped
	}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("later host cleanup invalidated immutable retirement steps", err)
	}
}

func TestRetainedNativeStopRequiresExactReadmissionBoundary(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	for i := range input.Retained["work"].Replicas {
		input.Retained["work"].Replicas[i].Progress = RetainedStopped
	}
	status := groupStatus("work", 1, []uint64{1, 2, 4}, nil)
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 4: true}
	if _, err := retainedRetirementPlan(input, desired, status); err == nil {
		t.Fatal("a bare stopped enum waived the native member's exact retirement boundary")
	}
	desired.MetaGroup().Replicas[3].ReplicaID, desired.MetaGroup().NextReplicaID = 6, 7
	input.Groups[MetaGroupID] = GroupProgress{ConvergedVersion: desired.Version, Converged: desired.MetaGroup()}
	plan, err := retainedRetirementPlan(input, desired, status)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(retainedStepKinds(plan, 4), []StepKind{StepRemoveReplica}) || len(retainedStepKinds(plan, 5)) != 0 {
		t.Fatal("readmission repeated completed host work", plan)
	}
}

func TestRetainedCleanupCarriesOnlyRequiredStop(t *testing.T) {
	t.Parallel()
	for _, sameHost := range []bool{false, true} {
		t.Run(map[bool]string{false: "detached", true: "incoming owner"}[sameHost], func(t *testing.T) {
			t.Parallel()
			input, desired := retainedPlanFixture()
			input.Retained["work"].Replicas[0].Progress = RetainedCleanup
			input.Retained["work"].Replicas[1].Progress = RetainedStopped
			desired.Group("work").Replicas[2].Role = ReplicaLearner
			if sameHost {
				desired.Group("work").Replicas[2].NodeName = "d"
			}
			status := groupStatus("work", 1, []uint64{1, 2}, nil)
			status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true}
			plan, err := retainedRetirementPlan(input, desired, status)
			if err != nil {
				t.Fatal(err)
			}
			var want []StepKind
			if sameHost {
				want = []StepKind{StepStopReplica}
			}
			if !slices.Equal(retainedStepKinds(plan, 4), want) || len(retainedStepKinds(plan, 5)) != 0 {
				t.Fatal("existing debt received a duplicate retirement action", plan)
			}
			if sameHost {
				broken := plan.Clone()
				broken.Steps = slices.DeleteFunc(broken.Steps, func(s Step) bool { return s.ReplicaID == 4 })
				for i := range broken.Steps {
					broken.Steps[i].Seq = i + 1
				}
				if err := ValidatePlanShape(input, desired, broken); err == nil {
					t.Fatal("incoming owner bypassed required debt stop")
				}
				if err := validatePlannedSteps(input, desired, map[string]GroupStatus{"work": status}, broken); err == nil {
					t.Fatal("native simulation bypassed required debt stop")
				}
			}
		})
	}
}

func TestRetainedCleanupRefusesContradictoryNativeMember(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Retained["work"].Replicas[0].Progress = RetainedCleanup
	status := groupStatus("work", 1, []uint64{1, 2, 6}, []uint64{4})
	if _, err := retainedRetirementPlan(input, desired, status); err == nil {
		t.Fatal("removal debt produced another native removal")
	}
}

func TestRetainedDeletionNeedsEveryUnprovenHostStop(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Groups["work"] = GroupProgress{GroupID: "work"}
	desired.Groups = desired.Groups[:1]
	input.Retained["work"].Replicas[1].Progress = RetainedCleanup
	status := GroupStatus{GroupID: "work"}
	plan, err := retainedRetirementPlan(input, desired, status)
	if err != nil {
		t.Fatal("possibly founded deletion required native quorum or invented convergence", err)
	}
	if len(plan.Steps) != 3 || plan.Steps[2].Kind != StepDeleteGroupState {
		t.Fatal("deletion lost a former owner", plan)
	}
	broken := plan.Clone()
	broken.Steps = append(broken.Steps[:1], broken.Steps[2:]...)
	broken.Steps[1].Seq = 2
	if err := ValidatePlanShape(input, desired, broken); err == nil {
		t.Fatal("deletion shape waived another debt's unproven stop")
	}
	if err := validatePlannedSteps(input, desired, map[string]GroupStatus{"work": status}, broken); err == nil {
		t.Fatal("deletion simulation waived another debt's unproven stop")
	}
	input.Retained["work"].Replicas[0].Progress = RetainedStopped
	plan, err = retainedRetirementPlan(input, desired, status)
	if err != nil || len(plan.Steps) != 1 || plan.Steps[0].Kind != StepStopReplica || plan.Steps[0].ReplicaID != 5 {
		t.Fatal("debt-only deletion duplicated retirement or lost its last stop", plan, err)
	}
	if err := ValidatePlanShape(input, desired, Plan{Version: desired.Version}); err == nil {
		t.Fatal("zero-step deletion bypassed the remaining cleanup stop")
	}
}

func TestRetainedDeletionReadmissionCoversOnlyItsHost(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Groups["work"] = GroupProgress{GroupID: "work"}
	desired.Groups = desired.Groups[:1]
	desired.MetaGroup().Replicas[3].ReplicaID, desired.MetaGroup().NextReplicaID = 6, 7
	input.Groups[MetaGroupID] = GroupProgress{ConvergedVersion: desired.Version, Converged: desired.MetaGroup()}
	plan, err := retainedRetirementPlan(input, desired, GroupStatus{GroupID: "work"})
	if err != nil || len(plan.Steps) != 0 {
		t.Fatal("complete readmission repeated retained host deletion", plan, err)
	}
	group := input.Retained["work"]
	group.Replicas = append(group.Replicas, RetainedReplica{
		ReplicaKey: ReplicaKey{NodeName: "c", ReplicaID: 3}, Progress: RetainedHosted})
	input.Retained["work"] = group
	plan, err = retainedRetirementPlan(input, desired, GroupStatus{GroupID: "work"})
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].ReplicaID != 3 || plan.Steps[1].Kind != StepDeleteGroupState {
		t.Fatal("partial readmission waived another retained host", plan, err)
	}
}
