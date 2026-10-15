//go:build !functional

package metaengine

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestLegacyApprovalRejectsScopedPlanWithoutLosingEvidence(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	doc := m.latest.Clone()
	doc.Comment = "declaration"
	if r := apply(t, m.topologySM, 6, ProposeDocument{ExpectedVersion: 1, Doc: doc}); !r.Accepted {
		t.Fatal(r)
	}
	plan := topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1},
		{GroupID: "shard0", SourceVersion: 1}}}
	result := apply(t, m.topologySM, 7, BeginOperatorProposal{Version: 2, Plan: plan, At: time.Unix(110, 0)})
	if result.Accepted || result.Code != proposalAuthorization || m.target.Version != 1 || len(m.operators) != 1 {
		t.Fatal("legacy FSM silently accepted unsupported scoped execution", result)
	}
	plan.Blocks = nil
	plan.Dependencies = []topology.StepDependency{{StepSeq: 1, Requires: []int{2}}}
	if err := validateExactPlan(plan); err == nil {
		t.Fatal("legacy exact-plan validator accepted dependencies")
	}
	if err := validateVoterChangePlan(topology.Document{}, topology.Document{}, topology.VoterChange{}, plan); err == nil {
		t.Fatal("voter path bypassed the legacy guard")
	}
}
