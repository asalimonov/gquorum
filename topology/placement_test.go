//go:build !functional

package topology

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func candidateByName(t *testing.T, report GroupPlacementReport, name string) PlacementCandidate {
	t.Helper()
	for _, c := range report.Candidates {
		if c.NodeName == name {
			return c
		}
	}
	t.Fatalf("candidate %q missing", name)
	return PlacementCandidate{}
}

func hasReason(candidate PlacementCandidate, code string) bool {
	for _, reason := range candidate.Reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func TestPlacementSelectorMissingAndEmptyAreDistinct(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Group("load").Placement.NodeSelector = map[string]string{"network": "", "disk": "ssd"}
	d.Node("a").Labels["network"] = ""
	d.Node("a").Labels["disk"] = "ssd"
	d.Node("c").Labels = map[string]string{"network": "private", "disk": "ssd"}
	r, err := EvaluatePlacement(PlacementInput{Desired: d, GroupID: "load"})
	if err != nil {
		t.Fatal(err)
	}
	if !candidateByName(t, r, "a").CanAdd {
		t.Fatal("present empty value should match")
	}
	c := candidateByName(t, r, "c")
	if c.CanAdd || !hasReason(c, "selector_value_mismatch") {
		t.Fatalf("mismatching value accepted: %#v", c)
	}
	delete(d.Node("c").Labels, "network")
	r, err = EvaluatePlacement(PlacementInput{Desired: d, GroupID: "load"})
	if err != nil || !hasReason(candidateByName(t, r, "c"), "selector_key_missing") {
		t.Fatalf("missing key not distinguished: %#v %v", r, err)
	}
	if MatchNodeSelector(nil, map[string]string{"network": ""}) || !MatchNodeSelector(map[string]string{"network": ""}, map[string]string{"network": ""}) {
		t.Fatal("selector lost presence semantics")
	}
}

func TestPlacementReservationRequiresApprovedExactIdentity(t *testing.T) {
	t.Parallel()
	approved := testDocument()
	desired := approved.Clone()
	desired.Node("a").Labels[LabelReserved] = "true"
	r, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: "load"})
	if err != nil {
		t.Fatal(err)
	}
	c := candidateByName(t, r, "a")
	if c.CanAdd || !c.CanRetain || !c.CanVote || !hasReason(c, "node_reserved") {
		t.Fatalf("approved exact identity not retained: %#v", c)
	}
	desired.Group("load").Replicas[0].ReplicaID = 8
	r, err = EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: "load"})
	if err != nil {
		t.Fatal(err)
	}
	c = candidateByName(t, r, "a")
	if c.CanAdd || c.CanRetain || c.CanVote {
		t.Fatalf("fresh identity grandfathered: %#v", c)
	}
	approved = Document{} // staged-only assignment cannot confer grandfathering.
	r, err = EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: "load"})
	if err != nil || candidateByName(t, r, "a").CanRetain {
		t.Fatalf("staged assignment grandfathered: %#v %v", r, err)
	}
}

func TestPlacementLearnerOnlyAppliesToMetaAndStrictVoters(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Node("a").Labels["az"] = "one"
	d.Node("b").Labels = map[string]string{"az": "two"}
	d.Node("c").Labels = map[string]string{"az": "one"}
	d.Node("b").Labels[LabelLearnerOnly] = "true"
	p := DefaultVoterConfig()
	p.TargetVoters = 3
	p.DistinctVoterLabel = "az"
	d.Group("load").VoterConfig = &p
	r, err := EvaluatePlacement(PlacementInput{Desired: d, GroupID: "load"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.EligibleDomains) != 1 || r.EligibleDomains[0] != "one" || len(r.Issues) != 1 || r.Issues[0].Code != "insufficient_distinct_zones" {
		t.Fatalf("strict shortage missing: %#v", r)
	}
	d.MetaGroup().VoterConfig = &p
	r, err = EvaluatePlacement(PlacementInput{Desired: d, GroupID: MetaGroupID})
	if err != nil || candidateByName(t, r, "b").CanVote || !hasReason(candidateByName(t, r, "b"), "node_learner_only") {
		t.Fatalf("meta learner-only restriction missed: %#v %v", r, err)
	}
	if err := ValidateVoterPlacement(d, MetaGroupID, []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 2}}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("learner-only meta voter accepted: %v", err)
	}
	delete(d.Node("b").Labels, LabelLearnerOnly)
	if err := ValidateVoterPlacement(d, MetaGroupID, []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "c", ReplicaID: 3}}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("same-domain voters accepted: %v", err)
	}
	if err := ValidateVoterPlacement(d, MetaGroupID, []ReplicaKey{{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 2}}); err != nil {
		t.Fatalf("distinct voter pair rejected: %v", err)
	}
}

func TestPlacementReportCloneAndError(t *testing.T) {
	t.Parallel()
	r := PlacementReport{AffectedGroups: []string{"work"}, Groups: []GroupPlacementReport{{
		GroupID: "work", EligibleDomains: []string{"one"},
		Candidates: []PlacementCandidate{{NodeName: "a", Reasons: []PlacementIssue{{Code: "node_reserved"}}}},
		Issues:     []PlacementIssue{{Code: "insufficient_distinct_zones", Message: "two zones are missing"}},
	}}}
	copy := r.Clone()
	copy.AffectedGroups[0] = "other"
	copy.Groups[0].EligibleDomains[0] = "other"
	copy.Groups[0].Candidates[0].Reasons[0].Code = "other"
	copy.Groups[0].Issues[0].Message = "other"
	if !reflect.DeepEqual(r.AffectedGroups, []string{"work"}) || r.Groups[0].EligibleDomains[0] != "one" || r.Groups[0].Candidates[0].Reasons[0].Code != "node_reserved" || r.Groups[0].Issues[0].Message != "two zones are missing" {
		t.Fatal("report clone shared backing storage")
	}
	e := &PlacementError{Code: fault.Invalid, Report: r}
	if !errors.Is(e, fault.ErrInvalid) || e.Error() == "" {
		t.Fatal("placement error did not preserve fault classification")
	}
}
