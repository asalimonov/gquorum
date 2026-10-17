package metaengine

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// These pure predicates run under the FSM lock at approval; decoding only checks
// the proposal's internal consistency, never its authority against current state.
func validateScopeGroups(scopes []ScopeExpectation, groups []string) error {
	if len(scopes) != len(groups) {
		return fault.New(fault.Invalid, "captured scopes do not cover the required groups", nil)
	}
	seen := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		if seen[scope.GroupID] || topology.ValidateID(scope.GroupID) != nil {
			return fault.New(fault.Invalid, "captured scopes have invalid or duplicate groups", nil)
		}
		seen[scope.GroupID] = true
	}
	for _, group := range groups {
		if !seen[group] {
			return fault.New(fault.Invalid, "captured scopes omit a required group", nil)
		}
		delete(seen, group)
	}
	return nil
}

func validatePlanScopes(plan topology.Plan, scopes []ScopeExpectation) error {
	groups := make([]string, 0, len(plan.Blocks))
	sources := make(map[string]uint64, len(plan.Blocks))
	for _, block := range plan.Blocks {
		groups = append(groups, block.GroupID)
		sources[block.GroupID] = block.SourceVersion
	}
	if err := validateScopeGroups(scopes, groups); err != nil {
		captured := make([]string, 0, len(scopes))
		for _, scope := range scopes {
			captured = append(captured, scope.GroupID)
		}
		slices.Sort(captured)
		message := fmt.Sprintf("captured scopes %v do not cover plan blocks %v", captured, groups)
		return fault.New(fault.Invalid, message, err)
	}
	for _, scope := range scopes {
		if sources[scope.GroupID] != scope.SourceVersion {
			return fault.New(fault.Invalid, "plan and captured source versions differ", nil)
		}
	}
	return nil
}

func (scope ScopeExpectation) matches(row *groupConvergence) bool {
	if row == nil {
		return scope.SourceVersion == 0 && scope.Owner == ""
	}
	return scope.GroupID == row.GroupID && scope.SourceVersion == row.SourceVersion && scope.Owner == row.Owner
}
