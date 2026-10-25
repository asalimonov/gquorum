package placement

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateEdit rechecks replicated scope/deficit provenance after a policy
// callback. Native observation, liveness and immutable-plan checks remain separate.
func ValidateEdit(ctx Context, base, desired topology.Document) error {
	_, err := allowances(ctx, base, base, desired)
	return err
}

// ValidateDocument keeps structural policy/identity checks strict, including
// historical documents. Deficit exceptions belong only to placement/count paths.
func ValidateDocument(_ Context, _ topology.Document, desired topology.Document, rules topology.Rules) error {
	return desired.Validate(rules)
}

// CheckLoadBounds adapts the service's additional declared-voter count check.
func CheckLoadBounds(ctx Context, approved, desired topology.Document) error {
	allowed, err := allowances(ctx, approved, approved, desired)
	if err != nil {
		return err
	}
	doc := normalized(desired, allowed)
	for _, group := range doc.LoadGroups() {
		minimum, maximum := doc.GroupBounds(group.ID)
		count := len(doc.VotersOf(group.ID))
		if count < minimum || count > maximum {
			return fault.New(fault.Invalid, fmt.Sprintf("group %q would have %d voters outside [%d,%d]",
				group.ID, count, minimum, maximum), nil)
		}
	}
	return nil
}

// ValidateTransition retains historical identity/tombstone checks at staging.
//
//revive:disable-next-line:argument-limit The existing transition contract carries explicit retired-ID evidence.
func ValidateTransition(ctx Context, approved, base, desired topology.Document, rules topology.Rules,
	retired map[string]bool) error {
	allowed, err := allowances(ctx, approved, base, desired)
	if err != nil {
		return err
	}
	return topology.ValidateTransition(normalized(base, allowed), normalized(desired, allowed), rules, retired)
}

// Explain uses normal preparation while reporting the original retained policy.
func Explain(ctx Context, view topology.ReadView, edited topology.Document,
	rules topology.Rules) (topology.PlacementReport, error) {
	allowed, err := allowances(ctx, view.Target, view.Latest, edited)
	if err != nil {
		return topology.PlacementReport{}, err
	}
	if len(allowed) == 0 {
		return topology.ExplainPlacement(view, edited, rules)
	}
	normalView := view
	normalView.Latest = normalized(view.Latest, allowed)
	normalView.Target = normalized(view.Target, allowed)
	report, err := topology.ExplainPlacement(normalView, normalized(edited, allowed), rules)
	restoreReport(&report, edited)
	return report, err
}

func restoreReport(report *topology.PlacementReport, original topology.Document) {
	for i := range report.Groups {
		group := original.Group(report.Groups[i].GroupID)
		if group == nil {
			continue
		}
		if group.VoterConfig != nil {
			report.Groups[i].RequiredVoters = group.VoterConfig.TargetVoters
		} else {
			_, report.Groups[i].RequiredVoters = original.GroupBounds(group.ID)
		}
	}
}
