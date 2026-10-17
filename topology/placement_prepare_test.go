//go:build !functional

package topology

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func placementTestView() ReadView {
	d := testDocument()
	return ReadView{Latest: d.Clone(), Target: d.Clone()}
}

func TestPreparePlacementRelocatesLegacyVoterWithoutMutatingInputs(t *testing.T) {
	t.Parallel()
	view := placementTestView()
	view.Latest.Node("b").Type = "worker"
	view.Target.Node("b").Type = "worker"
	view.Target.Node("b").Type = "worker"
	edited := view.Latest.Clone()
	edited.Node("b").Labels = map[string]string{"network": "public_ip"}
	edited.Group("load").Placement.NodeSelector = map[string]string{"network": "public_ip"}
	edited.ChangedBy = "operator"
	original := edited.Clone()
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	preparation, err := PreparePlacement(view, edited, testRules(), at)
	next, report := preparation.Document, preparation.Report
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(edited, original) || view.Target.Group("load").Replicas[0].NodeName != "a" {
		t.Fatal("preparation mutated caller input or approved view")
	}
	if next.Version != 2 || next.ParentVersion != 1 || next.Origin != OriginOperator || next.CreatedAt != at || !reflect.DeepEqual(report.AffectedGroups, []string{"load"}) {
		t.Fatalf("incorrect audit or affected groups: %#v %#v", next, report)
	}
	g := next.Group("load")
	if len(g.Replicas) != 1 || g.Replicas[0].NodeName != "b" || g.Replicas[0].ReplicaID != 3 || g.Replicas[0].Role != ReplicaVoter {
		t.Fatalf("legacy voter not safely relocated in desired intent: %#v", g.Replicas)
	}
	if !next.Node("b").UpdatedAt.Equal(at) || !g.UpdatedAt.Equal(at) {
		t.Fatal("changed node/group timestamps not updated")
	}
}

func TestPreparePlacementRejectsStrictShortageWithoutAllocation(t *testing.T) {
	t.Parallel()
	view := placementTestView()
	edited := view.Latest.Clone()
	edited.Node("a").Labels["az"] = "one"
	edited.Node("b").Labels = map[string]string{"az": "two"}
	edited.Node("c").Labels = map[string]string{"az": "two"}
	p := DefaultVoterConfig()
	p.DistinctVoterLabel = "az"
	edited.Groups = append(edited.Groups, Group{ID: "strict", NextReplicaID: 1, VoterConfig: &p, Hosting: &HostingRule{Mode: HostingAllEligible}})
	edited.ChangedBy = "operator"
	original := edited.Clone()
	preparation, err := PreparePlacement(view, edited, testRules(), time.Now())
	next, report := preparation.Document, preparation.Report
	if !errors.Is(err, fault.ErrInvalid) || next.Version != 0 || !reflect.DeepEqual(edited, original) ||
		view.Latest.Group("strict") != nil || view.Latest.Group("load").NextReplicaID != 3 {
		t.Fatalf("failed preflight leaked allocation or changed input: %#v %#v %v", next, report, err)
	}
	var pe *PlacementError
	if !errors.As(err, &pe) || len(report.Groups) == 0 {
		t.Fatalf("strict shortage lacks placement explanation: %v %#v", err, report)
	}
	// Explanation evaluates the same impossible edit without exposing IDs.
	explained, err := ExplainPlacement(view, edited, testRules())
	if !errors.Is(err, fault.ErrInvalid) || len(explained.Groups) == 0 || edited.Group("strict").NextReplicaID != 1 {
		t.Fatalf("explanation changed edit or missed shortage: %#v %v", explained, err)
	}
}

func TestPreparePlacementRejectsConflictingMembershipAndBadAudit(t *testing.T) {
	t.Parallel()
	view := placementTestView()
	edited := view.Latest.Clone()
	edited.Node("a").Labels["network"] = "public_ip"
	edited.ChangedBy = "operator"
	edited.Group("load").Replicas[0].ReplicaID = 9
	if _, err := PreparePlacement(view, edited, testRules(), time.Now()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("manual replica edit silently merged: %v", err)
	}
	edited = view.Latest.Clone()
	edited.Node("a").Labels["network"] = "public_ip"
	if _, err := PreparePlacement(view, edited, testRules(), time.Now()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("missing actor accepted: %v", err)
	}
	edited.ChangedBy = "operator"
	if _, err := PreparePlacement(view, edited, testRules(), time.Time{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("zero time accepted: %v", err)
	}
	view.Latest.Version++
	if _, err := PreparePlacement(view, edited, testRules(), time.Now()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("stale edited view accepted: %v", err)
	}
}

func TestPreparePlacementDescriptiveEditDoesNotReselectVoters(t *testing.T) {
	t.Parallel()
	view := placementTestView()
	edited := view.Latest.Clone()
	edited.ChangedBy = "operator"
	if err := edited.Group("load").SetLabels(map[string]string{"purpose": "accounting"}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	preparation, err := PreparePlacement(view, edited, testRules(), at)
	next, report := preparation.Document, preparation.Report
	if err != nil || len(report.AffectedGroups) != 0 || !reflect.DeepEqual(next.Group("load").Replicas, view.Latest.Group("load").Replicas) || !next.Group("load").UpdatedAt.Equal(at) {
		t.Fatalf("descriptive edit changed placement or lost timestamp: %#v %#v %v", next, report, err)
	}
}
