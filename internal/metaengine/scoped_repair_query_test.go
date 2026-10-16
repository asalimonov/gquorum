//go:build !functional

package metaengine

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestRepairPlanCapturesStagedIntentAndIndependentScope(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	// The legacy helper stamps a node. Scoped repair preserves node declarations
	// so a load-only repair does not acquire _meta accidentally.
	p.Doc.Nodes = s.Target.Clone().Nodes
	query := QueryRequest{Kind: "repair_plan", Key: "b", Desired: &p.Doc}
	read, err := queryScopedPlan(&s, query)
	if err != nil || read.Latest.Version != s.Latest.Version || read.Input.Approved.Version != s.Target.Version ||
		len(read.Scopes) != 1 || read.Scopes[0].GroupID != "b" {
		t.Fatal(read, err)
	}
	if !reflect.DeepEqual(read.Target, p.Doc) {
		t.Fatal("candidate lost")
	}
	p.Plan = repairFixturePlan(t, &s, p.Doc)
	p.Scopes = slices.Clone(read.Scopes)
	result, err := s.applyCommitRepairOperation(11, p)
	if err != nil || !result.result.Accepted || s.Target.Version != 3 || s.Latest.Version != 4 {
		t.Fatal("load-only repair or atomic rebase failed", result, err)
	}
	if len(s.Operations[len(s.Operations)-1].Plan.Blocks) != 1 {
		t.Fatal("repair owned metadata")
	}
	read.Latest.Groups[0].ID = "mutated"
	if s.Latest.Groups[0].ID == "mutated" {
		t.Fatal("borrowed capture")
	}
}

func TestRepairPlanQueryValidationAndNilAdapter(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	for _, q := range []QueryRequest{
		{Kind: "repair_plan", Version: 1}, {Kind: "repair_plan", OperationID: "foreign"},
		{Kind: "repair_plan", Desired: &p.Doc}, {Kind: "repair_plan", Key: "b", Desired: &p.Doc},
	} {
		// The unmodified helper edits _meta and b; the keyed query refuses both.
		if _, err := queryScopedPlan(&s, q); !errors.Is(err, fault.ErrInvalid) && !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal(q, err)
		}
	}
	if _, err := queryScopedPlan(nil, QueryRequest{Kind: "repair_plan"}); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	read, err := queryScopedPlan(&s, QueryRequest{Kind: "repair_plan"})
	if err != nil || len(read.Scopes) != len(s.Target.Groups) || read.Latest.Version != 2 {
		t.Fatal(read, err)
	}
	if err := ValidateRepairIndependence(s.Target, s.Latest, p.Doc); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRepairIndependence(s.Target, s.Latest, s.Latest); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}
