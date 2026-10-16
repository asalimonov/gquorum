//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestApprovalScopeSetUsesTrustedDeclarations(t *testing.T) {
	t.Parallel()
	state := scopedGraphFixture()
	desired := state.Target.Clone()
	desired.Group("shard0").Labels = map[string]string{"edit": "yes"}
	groups := topology.AffectedGroups(state.Target, desired)
	cases := map[string][]ScopeExpectation{
		"omitted": nil, "extra": {{GroupID: "shard0"}, {GroupID: topology.MetaGroupID}},
		"wrong": {{GroupID: topology.MetaGroupID}}, "duplicate": {{GroupID: "shard0"}, {GroupID: "shard0"}},
		"invalid": {{GroupID: " "}},
	}
	for name, scopes := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateScopeGroups(scopes, groups); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("caller selected its own affected groups", err)
			}
		})
	}
	if err := validateScopeGroups([]ScopeExpectation{{GroupID: "shard0"}}, groups); err != nil {
		t.Fatal(err)
	}
	desired = state.Target.Clone()
	desired.Node("n1").Region = "descriptive"
	if err := validateScopeGroups([]ScopeExpectation{{GroupID: topology.MetaGroupID}},
		topology.AffectedGroups(state.Target, desired)); err != nil {
		t.Fatal("node-only declaration lost its metadata block", err)
	}
}

func TestCapturedScopeRequiresExactSourceAndOwner(t *testing.T) {
	t.Parallel()
	state := scopedGraphFixture()
	row := state.Groups[1]
	valid := ScopeExpectation{GroupID: row.GroupID, SourceVersion: row.SourceVersion, Owner: row.Owner}
	if !valid.matches(&row) || !(ScopeExpectation{GroupID: "new"}).matches(nil) {
		t.Fatal("exact current scope or genuinely new group refused")
	}
	for _, stale := range []ScopeExpectation{
		{GroupID: "other", SourceVersion: row.SourceVersion, Owner: row.Owner},
		{GroupID: row.GroupID, SourceVersion: row.SourceVersion + 1, Owner: row.Owner},
		{GroupID: row.GroupID, SourceVersion: row.SourceVersion},
		{GroupID: row.GroupID, SourceVersion: row.SourceVersion, Owner: "exec-v2-a1"},
	} {
		if stale.matches(&row) || stale.matches(nil) {
			t.Fatal("stale capture accepted", stale)
		}
	}
}

func TestApprovalPayloadRejectsScopePlanMismatchAtomically(t *testing.T) {
	t.Parallel()
	valid := BeginOperationProposal{Version: 2, Actor: "n1", ProposalID: "begin",
		Plan:   topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1}}},
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1}}}
	for _, scopes := range [][]ScopeExpectation{nil, {{GroupID: "shard0", SourceVersion: 2}},
		{{GroupID: "shard0", SourceVersion: 1}, {GroupID: "shard0", SourceVersion: 1}}} {
		bad := valid
		bad.Scopes = scopes
		data, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		got := valid
		if err := json.Unmarshal(data, &got); !errors.Is(err, fault.ErrInvalid) || !reflect.DeepEqual(got, valid) {
			t.Fatal("bad decode replaced prior capture", got, err)
		}
	}
	plan := valid.Plan.Clone()
	plan.Blocks = append(plan.Blocks, plan.Blocks[0])
	if err := validatePlanScopes(plan, append(valid.Scopes, valid.Scopes[0])); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("duplicate block/capture pair accepted", err)
	}
}
