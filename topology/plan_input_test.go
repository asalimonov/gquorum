//go:build !functional

package topology

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
)

func TestPlanInputKeepsProvenDomainsSeparateFromApproval(t *testing.T) {
	t.Parallel()
	proven, desired, _, _ := strictReplacementFixture()
	input := convergedPlanInput(proven)
	input.Approved = proven.Clone()
	input.Approved.Version++
	input.Approved.Node("b").Labels["az"] = "az-a"
	voters := []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 2}, {NodeName: "c", ReplicaID: 3}}
	if err := validateOldDomains(input, desired, "work", voters); err != nil {
		t.Fatal("unconverged approval changed the old domain interpretation", err)
	}
	row := input.Nodes["b"].Clone()
	row.Converged.Labels["az"] = "az-a"
	input.Nodes["b"] = row
	input.Approved = proven.Clone()
	if err := validateOldDomains(input, desired, "work", voters); err == nil {
		t.Fatal("new approval erased an unsafe proven domain")
	}
}

func TestPlanInputUsesApprovedIdentityForReservedPlacement(t *testing.T) {
	t.Parallel()
	proven, desired, _, _ := strictReplacementFixture()
	proven.Group("work").Replicas = proven.Group("work").Replicas[:3]
	desired.Node("d").Labels[LabelReserved] = enabledNodeLabel
	input := convergedPlanInput(proven)
	input.Approved = desired.Clone()
	voters := []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 2}, {NodeName: "d", ReplicaID: 4}}
	violations, err := targetVoterViolations(input, desired, "work", voters)
	if err != nil || len(violations) != 0 {
		t.Fatal("approved reserved identity was mistaken for a new assignment", violations, err)
	}
	input.Approved = proven.Clone()
	violations, err = targetVoterViolations(input, desired, "work", voters)
	if err != nil || !violations["invalid:4"] {
		t.Fatal("unapproved reserved assignment was accepted", violations, err)
	}
}

func TestPlanInputKeepsFormerNativeIdentityUntilProvenRemoval(t *testing.T) {
	t.Parallel()
	proven, desired, observed, _ := strictReplacementFixture()
	desired.Group("work").Replicas = slices.DeleteFunc(desired.Group("work").Replicas,
		func(r Replica) bool { return r.ReplicaID == 3 })
	input := convergedPlanInput(proven)
	input.Approved = desired.Clone()
	if err := observedVoterIdentities(input, desired, observed); err != nil {
		t.Fatal("approved removal erased the former native identity", err)
	}
	for _, version := range []uint64{0, desired.Version} {
		row := input.Groups["work"]
		row.Converged, row.ConvergedVersion = nil, version
		input.Groups["work"] = row
		if err := observedVoterIdentities(input, desired, observed); err == nil {
			t.Fatal("unknown/deleted convergence fabricated former identity", version)
		}
	}
}

func TestScopedVoterPlanningPreservesSafetyAndCallerInputs(t *testing.T) {
	t.Parallel()
	proven, desired, observed, rules := strictReplacementFixture()
	input := convergedPlanInput(proven)
	input.Approved = desired.Clone()
	view := voterPlanTopology{base: input, desired: desired, groupID: "work"}
	before, _ := json.Marshal(input)
	moves, err := planVoterMoves(view, observed["work"], PlanOptions{Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []StepKind
	for _, move := range moves {
		kinds = append(kinds, move.Kind)
	}
	if !slices.Equal(kinds, []StepKind{StepWaitCaughtUp, StepDemoteVoter, StepWaitCaughtUp, StepPromoteVoter}) {
		t.Fatal("scoped input changed guarded same-domain replacement", moves)
	}
	after, _ := json.Marshal(input)
	if !bytes.Equal(before, after) {
		t.Fatal("voter planning mutated caller-owned declarations")
	}
	status := observed["work"].Clone()
	status.LiveVoters[2] = false
	if _, err := planVoterMoves(view, status, PlanOptions{Rules: rules}); err == nil {
		t.Fatal("scoped planning bypassed live-quorum guard")
	}
}
