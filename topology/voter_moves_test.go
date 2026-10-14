//go:build !functional

package topology

import (
	"slices"
	"strings"
	"testing"
)

func strictReplacementFixture() (Document, Document, map[string]GroupStatus, Rules) {
	active := planDoc(1, 4, []string{"a", "b", "c", "d"}, shardOf("work", rep("a", 1), rep("b", 2), rep("c", 3), learner("d", 4)))
	for i, zone := range []string{"az-a", "az-b", "az-c", "az-c"} {
		active.Nodes[i].Labels = map[string]string{"az": zone}
	}
	voterConfig := DefaultVoterConfig()
	voterConfig.DistinctVoterLabel = "az"
	active.Group("work").VoterConfig = &voterConfig
	active.Group("work").Hosting = &HostingRule{Mode: HostingAllEligible}
	desired := active.Clone()
	desired.Version++
	desired.Group("work").Replicas[2].Role = ReplicaLearner
	desired.Group("work").Replicas[3].Role = ReplicaVoter
	status := statusForDocument(active)
	work := status["work"]
	work.HealthKnown = true
	work.LiveVoters = map[uint64]bool{1: true, 2: true, 3: true}
	status["work"] = work
	return active, desired, status, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 4, MaxReplicas: 5}}
}

func TestStrictSameAZReplacementUsesThreeTwoThree(t *testing.T) {
	t.Parallel()
	active, desired, status, rules := strictReplacementFixture()
	plan, err := BuildPlan(active, desired, status, rules)
	if err != nil {
		t.Fatal(err)
	}
	var relevant []StepKind
	for _, step := range plan.Steps {
		if step.GroupID == "work" {
			relevant = append(relevant, step.Kind)
		}
	}
	want := []StepKind{StepWaitCaughtUp, StepDemoteVoter, StepWaitCaughtUp, StepPromoteVoter}
	if !slices.Equal(relevant, want) {
		t.Fatalf("same-AZ transition did not use paired 3->2->3: %v", relevant)
	}
	base := VoterSetDigest([]uint64{1, 2, 3})
	shrink := VoterSetDigest([]uint64{1, 2})
	final := VoterSetDigest([]uint64{1, 2, 4})
	if plan.Steps[1].ExpectedVotersDigest != base || plan.Steps[1].ResultVotersDigest != shrink || plan.Steps[3].ExpectedVotersDigest != shrink || plan.Steps[3].ResultVotersDigest != final {
		t.Fatal("paired move lost exact voter proof", plan.Steps)
	}
}

func TestStrictSameAZReplacementRefusesLostSurvivor(t *testing.T) {
	t.Parallel()
	active, desired, status, rules := strictReplacementFixture()
	work := status["work"]
	work.LiveVoters[2] = false
	status["work"] = work
	if plan, err := BuildPlan(active, desired, status, rules); err == nil || !strings.Contains(err.Error(), "live quorum") {
		t.Fatalf("unsafe two-survivor replacement accepted: %+v, %v", plan, err)
	}
}

func TestTargetViolationSetCannotTradeOnePairForAnother(t *testing.T) {
	t.Parallel()
	base, desired, _, _ := strictReplacementFixture()
	base.Group("work").VoterConfig = nil // separation is newly enabled
	base.Nodes[1].Labels["az"] = "az-a"
	desired.Nodes[1].Labels["az"] = "az-a"
	state := voterPlanState{groupID: "work"}
	before := []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 2}, {NodeName: "c", ReplicaID: 3}}
	mid := []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "c", ReplicaID: 3}}
	if err := validateVoterTransition(voterPlanTopology{base: base, desired: desired, groupID: "work"}, before, mid, state); err != nil {
		t.Fatalf("repair of exact baseline violation was rejected: %v", err)
	}
	after := []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "c", ReplicaID: 3}, {NodeName: "d", ReplicaID: 4}}
	if err := validateVoterTransition(voterPlanTopology{base: base, desired: desired, groupID: "work"}, before, after, state); err == nil {
		t.Fatal("changed exact domain pair was accepted")
	}
}
