//go:build !functional

package topology

import "testing"

func TestValidatePlanShapeRequiresExactVoterProof(t *testing.T) {
	t.Parallel()
	active, desired, status, rules := strictReplacementFixture()
	valid, err := BuildPlan(testPlanInput(active, status), desired, rules)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlanShape(convergedPlanInput(active), desired, valid); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Plan)
	}{
		{"missing base", func(p *Plan) { p.VoterBases = nil }},
		{"unknown base identity", func(p *Plan) { p.VoterBases[0].Voters[0] = 99 }},
		{"duplicate base identity", func(p *Plan) { p.VoterBases[0].Voters[1] = 1 }},
		{"missing predecessor", func(p *Plan) { p.Steps[1].ExpectedVotersDigest = "" }},
		{"forged result", func(p *Plan) { p.Steps[1].ResultVotersDigest = p.Steps[1].ExpectedVotersDigest }},
		{"guard on wait", func(p *Plan) { p.Steps[0].ExpectedVotersDigest = VoterSetDigest(nil) }},
		{"unmatched preparatory wait", func(p *Plan) { p.Steps[0].NodeName = "b" }},
		{"unmatched fresh wait", func(p *Plan) { p.Steps[2].NodeName = "b" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan := valid.Clone()
			tc.change(&plan)
			if err := ValidatePlanShape(convergedPlanInput(active), desired, plan); err == nil {
				t.Fatalf("accepted altered voter proof: %+v", plan)
			}
		})
	}
}

func TestValidatePlanShapeRejectsSecondDestructionInOnePair(t *testing.T) {
	t.Parallel()
	active, desired, status, rules := strictReplacementFixture()
	plan, err := BuildPlan(testPlanInput(active, status), desired, rules)
	if err != nil {
		t.Fatal(err)
	}
	extra := plan.Steps[1]
	plan.Steps = append(plan.Steps[:2], append([]Step{extra}, plan.Steps[2:]...)...)
	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
	}
	if err := ValidatePlanShape(convergedPlanInput(active), desired, plan); err == nil {
		t.Fatal("preparatory wait covered two destructive actions")
	}
}

func TestValidatePlanShapeBindsApprovedLabelViews(t *testing.T) {
	t.Parallel()
	active, desired, status, rules := strictReplacementFixture()
	plan, err := BuildPlan(testPlanInput(active, status), desired, rules)
	if err != nil {
		t.Fatal(err)
	}
	changedTarget := desired.Clone()
	changedTarget.Node("d").Labels["az"] = "az-a"
	if err := ValidatePlanShape(convergedPlanInput(active), changedTarget, plan); err == nil {
		t.Fatal("approved plan was reinterpreted against a conflicting Target AZ")
	}
	changedBase := active.Clone()
	changedBase.Node("b").Labels["az"] = "az-a"
	if err := ValidatePlanShape(convergedPlanInput(changedBase), desired, plan); err == nil {
		t.Fatal("approved plan accepted a different Active AZ baseline")
	}
}
