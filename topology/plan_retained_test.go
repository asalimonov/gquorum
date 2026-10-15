//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func retainedPlanFixture() (PlanInput, Document) {
	base := planDoc(1, 5, []string{"a", "b", "c", "d", "e"},
		shardOf("work", rep("a", 1), rep("b", 2), rep("c", 3)))
	input := convergedPlanInput(base)
	input.Approved = base.Clone()
	input.Approved.Group("work").NextReplicaID = 7
	input.Approved.Group("work").Replicas[2] = rep("e", 6)
	desired := input.Approved.Clone()
	desired.Version++
	input.Retained = map[string]RetainedGroup{"work": {Replicas: []RetainedReplica{
		{ReplicaKey: ReplicaKey{NodeName: "d", ReplicaID: 4}, Progress: RetainedHosted},
		{ReplicaKey: ReplicaKey{NodeName: "d", ReplicaID: 5}, Progress: RetainedHosted},
	}}}
	return input, desired
}

func TestRetainedPlannerRejectsContradictoryEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*PlanInput, *Document)
	}{
		{"zero ID", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].ReplicaID = 0 }},
		{"empty host", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].NodeName = "" }},
		{"empty group", func(in *PlanInput, _ *Document) { in.Retained[""] = RetainedGroup{} }},
		{"unknown progress", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].Progress = "retired" }},
		{"duplicate ID", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[1].ReplicaID = 4 }},
		{"duplicate changed host", func(in *PlanInput, _ *Document) {
			in.Retained["work"].Replicas[1].ReplicaKey = ReplicaKey{NodeName: "e", ReplicaID: 4}
		}},
		{"desired overlap", func(in *PlanInput, _ *Document) {
			in.Retained["work"].Replicas[0].ReplicaKey = ReplicaKey{NodeName: "a", ReplicaID: 1}
		}},
		{"desired host conflict", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].ReplicaID = 1 }},
		{"proven host conflict", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].ReplicaID = 3 }},
		{"approved host conflict", func(in *PlanInput, out *Document) {
			in.Approved.Group("work").Replicas[2] = rep("e", 4)
			out.Group("work").Replicas = out.Group("work").Replicas[:2]
		}},
		{"allocation ceiling", func(in *PlanInput, _ *Document) { in.Retained["work"].Replicas[0].ReplicaID = 7 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input, desired := retainedPlanFixture()
			tc.change(&input, &desired)
			if err := validateRetainedInput(input, desired); !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatal("accepted contradictory retained evidence", err)
			}
			if err := ValidatePlanShape(input, desired, Plan{Version: desired.Version}); err == nil {
				t.Fatal("shape validation bypassed retained evidence checks")
			}
		})
	}
}

func TestRetainedPlannerPreservesExactHistoricalIdentity(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	before, _ := json.Marshal(input)
	observed := map[string]GroupStatus{"work": groupStatus("work", 1, []uint64{1, 2, 4}, []uint64{5})}
	if err := observedVoterIdentities(input, desired, observed); err != nil {
		t.Fatal("twice-superseded native identity disappeared", err)
	}
	state := voterPlanState{base: input, desired: desired, groupID: "work"}
	keys := voterKeysForSet(state, idSet([]uint64{1, 2, 4}))
	if len(keys) != 3 || keys[2] != (ReplicaKey{NodeName: "d", ReplicaID: 4}) {
		t.Fatal("native voter proof lost its exact retained key", keys)
	}
	proof := planVoterProof{base: input, desired: desired}
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work", Voters: []uint64{1, 2, 4}}); err != nil {
		t.Fatal("voter proof refused a retained native identity", err)
	}
	after, _ := json.Marshal(input)
	if !slices.Equal(before, after) {
		t.Fatal("planning changed caller-owned evidence")
	}
	delete(input.Retained, "work")
	if err := observedVoterIdentities(input, desired, observed); err == nil {
		t.Fatal("missing evidence became a guessed native identity")
	}
	proof.base = input
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work", Voters: []uint64{1, 2, 4}}); err == nil {
		t.Fatal("unknown native identity entered immutable voter proof")
	}
}

func TestRetainedPlannerDeclarationBounds(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	// Converged's old counter predates these issued IDs. The current approved
	// and desired declarations carry the allocation bound, independently.
	input.Retained["work"].Replicas[0] = RetainedReplica{
		ReplicaKey: ReplicaKey{NodeName: "c", ReplicaID: 3}, Progress: RetainedCleanup}
	input.Retained["work"].Replicas[1].Progress = RetainedStopped
	if err := validateRetainedInput(input, desired); err != nil {
		t.Fatal("exact proven overlap or later allocated identity was refused", err)
	}
	input.Approved.Group("work").NextReplicaID = 4
	if err := validateRetainedInput(input, desired); err != nil {
		t.Fatal("candidate's newer allocation bound was ignored", err)
	}
	input.Approved.Groups = input.Approved.Groups[:1]
	desired.Groups = desired.Groups[:1]
	if err := validateRetainedInput(input, desired); err != nil {
		t.Fatal("older convergence counter pretended to bound later deleted-group effects", err)
	}
}

func TestRetainedPlannerCannotRefoundPossibleState(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fresh", "grant only", "hosted", "cleanup", "stopped", "tombstone"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			input, desired := retainedPlanFixture()
			input.Groups["work"] = GroupProgress{GroupID: "work"}
			retained := input.Retained["work"]
			switch mode {
			case "fresh":
				retained = RetainedGroup{}
			case "grant only":
				retained = RetainedGroup{FoundingIssued: true}
			case "cleanup":
				retained.Replicas[0].Progress = RetainedCleanup
			case "stopped":
				retained.Replicas[0].Progress = RetainedStopped
			case "tombstone":
				retained = RetainedGroup{}
				input.Groups["work"] = GroupProgress{GroupID: "work", ConvergedVersion: 2}
			}
			input.Retained["work"] = retained
			builder := topologyPlanBuilder{input: input, desired: desired,
				observed: map[string]GroupStatus{"work": {}}, options: PlanOptions{Rules: plannerRules()}}
			err := builder.planGroup("work")
			if (err == nil) != (mode == "fresh") {
				t.Fatal("missing native observation changed founding eligibility", mode, err)
			}
			if mode != "fresh" && !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal("possible established state did not remain unavailable", err)
			}
			step := Step{Kind: StepCreateFoundingGroup, GroupID: "work"}
			shape := planShapeValidator{input: input, desired: desired}
			if err := shape.validateStepOrder(0, step); (err == nil) != (mode == "fresh") {
				t.Fatal("shape ignored retained founding state", err)
			}
			simulation := plannedStepValidator{input: input, desired: desired}
			if err := simulation.applyCreateFoundingGroup(step, newValidationGroup(GroupStatus{})); (err == nil) != (mode == "fresh") {
				t.Fatal("native simulation ignored retained founding state", err)
			}
		})
	}
}

func TestRetainedPlannerProofWithoutConvergedDeclaration(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	input.Groups["work"] = GroupProgress{GroupID: "work"}
	proof := planVoterProof{base: input, desired: desired}
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work", Voters: []uint64{1, 2, 4}}); err != nil {
		t.Fatal("retained native voters required fabricated convergence", err)
	}
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work"}); err == nil {
		t.Fatal("retained state admitted an empty founding voter base")
	}
}

func TestRetainedPlannerValueEncoding(t *testing.T) {
	t.Parallel()
	value := RetainedGroup{FoundingIssued: true, Replicas: []RetainedReplica{
		{ReplicaKey: ReplicaKey{NodeName: "a", ReplicaID: 4}, Progress: RetainedStopped}}}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"founding_issued":true,"replicas":[{"node_name":"a","replica_id":4,"progress":"stopped"}]}`
	if string(data) != want {
		t.Fatal("retained evidence changed its explicit encoding", string(data))
	}
	var decoded RetainedGroup
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(value, decoded) {
		t.Fatal("retained evidence did not round trip", decoded, err)
	}
}

func TestRetainedPlannerKeepsDomainAndRoleGuards(t *testing.T) {
	t.Parallel()
	input, desired := retainedPlanFixture()
	config := DefaultVoterConfig()
	config.DistinctVoterLabel = "az"
	input.Groups["work"].Converged.VoterConfig = &config
	for name, row := range input.Nodes {
		row.Converged.Labels = map[string]string{"az": name}
	}
	input.Nodes["d"].Converged.Labels["az"] = "a"
	proof := planVoterProof{base: input, desired: desired}
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work", Voters: []uint64{1, 2, 4}}); err == nil {
		t.Fatal("retained identity bypassed proven domain guards")
	}
	input.Nodes["d"].Converged.Labels["az"] = "c"
	if _, err := proof.validateBase(PlanVoterBase{GroupID: "work", Voters: []uint64{1, 2, 4}}); err != nil {
		t.Fatal("valid retained identity lost its proven node domain", err)
	}
	validator := planShapeValidator{input: input, desired: desired}
	step := Step{Kind: StepPromoteVoter, GroupID: "work", NodeName: "d", ReplicaID: 4}
	if err := validator.validateReplicaRole(step); err == nil {
		t.Fatal("identity-only evidence authorized an undeclared voter role")
	}
	step.NodeName, step.ReplicaID = "e", 6
	desired.Group("work").Replicas[2].PinnedLearner = true
	if err := validator.validateReplicaRole(step); err == nil {
		t.Fatal("retained evidence weakened pinned learner protection")
	}
}
