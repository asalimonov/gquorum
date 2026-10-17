//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestScopedPlannerDoesNotObserveUnchangedGroups(t *testing.T) {
	t.Parallel()
	base := scopeDocument()
	desired := base.Clone()
	desired.Version++
	desired.Group("b").Replicas = append(desired.Group("b").Replicas, learner("n3", 2))
	desired.Group("b").NextReplicaID = 3
	input := testPlanInput(base, statusForDocument(base))
	delete(input.Native, MetaGroupID)
	delete(input.Native, "a")
	row := input.Groups["a"]
	row.Observation = &GroupStatus{GroupID: "foreign"}
	input.Groups["a"] = row
	before, _ := json.Marshal(input)
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Steps) != 2 || len(plan.VoterBases) != 1 {
		t.Fatal("unchanged unavailable groups blocked B", plan, err)
	}
	if plan.Steps[0].GroupID != "b" || plan.Steps[0].Kind != StepPrepareReplica ||
		plan.Steps[1].Kind != StepAddLearner || plan.VoterBases[0].GroupID != "b" {
		t.Fatal("wrong scoped membership work", plan)
	}
	if err := validatePlanShapeFor(input, desired, plan, []string{"b"}); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(input)
	if !slices.Equal(before, after) {
		t.Fatal("scope selection changed caller evidence")
	}
	if ordinary, err := BuildPlan(input, desired, plannerRules()); err != nil || !ordinary.Equal(plan) {
		t.Fatal("public constructors disagree on unchanged unavailable scopes", ordinary, err)
	}
}

func TestScopedPlannerStillRequiresAffectedEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"declaration only", "missing native", "invalid native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			base := scopeDocument()
			desired := base.Clone()
			desired.Version++
			desired.Group("b").Labels = map[string]string{"owner": "jobs"}
			input := testPlanInput(base, statusForDocument(base))
			delete(input.Native, "a")
			delete(input.Native, MetaGroupID)
			switch mode {
			case "missing native":
				delete(input.Native, "b")
			case "invalid native":
				input.Native["b"] = NativeState{GroupID: "foreign"}
			}
			plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
			if mode == "declaration only" {
				if err != nil || len(plan.Steps) != 0 {
					t.Fatal("declaration-only work changed membership", plan, err)
				}
			} else if !errors.Is(err, fault.ErrUnavailable) && !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatal("affected group bypassed native validation", err)
			}
		})
	}
}

func TestScopedPlannerLeavesIndependentDeletionDebt(t *testing.T) {
	t.Parallel()
	base := scopeDocument()
	desired := base.Clone()
	desired.Version++
	desired.Group("b").Labels = map[string]string{"owner": "jobs"}
	input := testPlanInput(base, statusForDocument(base))
	input.Retained = map[string]RetainedGroup{"gone": {Replicas: []RetainedReplica{
		{ReplicaKey: ReplicaKey{NodeName: "n3", ReplicaID: 1}, Progress: RetainedHosted}}}}
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Steps) != 0 {
		t.Fatal("independent former ownership entered B's plan", plan, err)
	}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("scoped validation required independent deletion", err)
	}
}

func TestScopedDeletionKeepsExactRequiredHosts(t *testing.T) {
	t.Parallel()
	base := scopeDocument()
	base.Group("a").NextReplicaID = 3
	desired := base.Clone()
	desired.Version++
	desired.Groups = slices.DeleteFunc(desired.Groups, func(g Group) bool { return g.ID == "a" })
	input := testPlanInput(base, nil)
	input.Retained = map[string]RetainedGroup{"a": {Replicas: []RetainedReplica{
		{ReplicaKey: ReplicaKey{NodeName: "n3", ReplicaID: 2}, Progress: RetainedCleanup}}}}
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Steps) != 3 {
		t.Fatal("deletion omitted required old hosts or needed a quorum", plan, err)
	}
	if plan.Steps[0].NodeName != "n1" || plan.Steps[1].NodeName != "n3" ||
		plan.Steps[1].ReplicaID != 2 || plan.Steps[2].Kind != StepDeleteGroupState {
		t.Fatal("deletion lost exact identity", plan)
	}
	input.Approved = desired.Clone() // Original-plan validation must not rederive an empty delta.
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("approval invalidated its immutable deletion plan", err)
	}
	broken := plan.Clone()
	broken.Steps = slices.Delete(broken.Steps, 1, 2)
	broken.Steps[1].Seq = 2
	if err := validatePlanShapeFor(input, desired, broken, []string{"a"}); err == nil {
		t.Fatal("scoped deletion waived an unconsumed required stop")
	}
}

func TestScopedPlannerDoesNotTakeOverAnotherAdmission(t *testing.T) {
	t.Parallel()
	base := scopeDocument()
	input := testPlanInput(base, statusForDocument(base))
	input.Approved = base.Clone()
	input.Approved.MetaGroup().Replicas[0].ReplicaID = 4
	input.Approved.MetaGroup().NextReplicaID = 5
	desired := input.Approved.Clone()
	desired.Version++
	desired.Group("b").Labels = map[string]string{"owner": "jobs"}
	delete(input.Native, MetaGroupID)
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Steps) != 0 {
		t.Fatal("B synthesized an unchanged metadata admission", plan, err)
	}
}

func TestScopedFoundingUsesActualSeedEvidence(t *testing.T) {
	t.Parallel()
	desired := planDoc(1, 3, []string{"n1"}, shardOf("work", rep("n1", 1)))
	seed := testPlanInput(desired, statusForDocument(desired)).Native[MetaGroupID]
	input := PlanInput{Native: map[string]NativeState{MetaGroupID: seed}}
	plan, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()})
	if err != nil || len(plan.Steps) != 1 || plan.Steps[0].GroupID != "work" ||
		plan.Steps[0].Kind != StepCreateFoundingGroup {
		t.Fatal("founding ignored actual metadata seed membership", plan, err)
	}
	input.Retained = map[string]RetainedGroup{"work": {FoundingIssued: true}}
	if _, err := BuildPlanPreferred(input, desired, PlanOptions{Rules: plannerRules()}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("possibly-founded group was refounded", err)
	}
}
