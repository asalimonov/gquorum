package topology

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
)

// PlacementIssue explains one unsatisfied host or voter constraint.
type PlacementIssue struct {
	Code     string `json:"code"`
	GroupID  string `json:"group_id"`
	NodeName string `json:"node_name,omitempty"`
	Key      string `json:"key,omitempty"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
	Message  string `json:"message"`
}

// PlacementCandidate reports a node's addition, retention and voter
// eligibility.
type PlacementCandidate struct {
	NodeName  string           `json:"node_name"`
	CanAdd    bool             `json:"can_add"`
	CanRetain bool             `json:"can_retain"`
	CanVote   bool             `json:"can_vote"`
	Domain    string           `json:"domain,omitempty"`
	Reasons   []PlacementIssue `json:"reasons,omitempty"`
}

// GroupPlacementReport explains candidate eligibility and voter requirements
// for one group.
type GroupPlacementReport struct {
	GroupID         string               `json:"group_id"`
	RequiredVoters  int                  `json:"required_voters"`
	AssignedVoters  int                  `json:"assigned_voters"`
	EligibleDomains []string             `json:"eligible_domains,omitempty"`
	Candidates      []PlacementCandidate `json:"candidates"`
	Issues          []PlacementIssue     `json:"issues,omitempty"`
}

func (e *PlacementError) Error() string {
	if e == nil {
		return "placement error"
	}
	for _, group := range e.Report.Groups {
		if len(group.Issues) > 0 {
			return fmt.Sprintf("%s: %s", e.Code, group.Issues[0].Message)
		}
	}
	return fmt.Sprintf("%s: placement is unsatisfied", e.Code)
}

func (e *PlacementError) Unwrap() error {
	if e == nil {
		return nil
	}
	return fault.New(e.Code, "placement is unsatisfied", nil)
}

// Clone copies groups, candidate reasons, issues and affected-group names.
func (r PlacementReport) Clone() PlacementReport {
	r.AffectedGroups = slices.Clone(r.AffectedGroups)
	r.Groups = slices.Clone(r.Groups)
	for i := range r.Groups {
		g := &r.Groups[i]
		g.EligibleDomains = slices.Clone(g.EligibleDomains)
		g.Issues = slices.Clone(g.Issues)
		g.Candidates = slices.Clone(g.Candidates)
		for j := range g.Candidates {
			g.Candidates[j].Reasons = slices.Clone(g.Candidates[j].Reasons)
		}
	}
	return r
}
