package metaengine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Unknown actions make a committed plan uninterpretable by this binary. Keep
// them fatal; known actions with invalid content are deterministic refusals.
func knownPlanSteps(plan topology.Plan) error {
	for _, step := range plan.Steps {
		if !validStepKind(step.Kind) {
			return fmt.Errorf("step %d has unknown kind %q", step.Seq, step.Kind)
		}
	}
	return nil
}

func planContentRefusal(err error) error {
	var classified *fault.Error
	if errors.As(err, &classified) {
		return err
	}
	return fault.New(fault.Invalid, "invalid plan", err)
}

// Deterministic command refusals are committed results. Corrupt state, malformed
// encodings, persistence failure and the local founding-seed assertion remain
// Apply errors, which stop the replica instead of acknowledging a different fact.
func scopedProposalRefusal(body json.RawMessage, err error) (ApplyResult, error) {
	var classified *fault.Error
	if !errors.As(err, &classified) {
		return ApplyResult{}, err
	}
	switch classified.Code {
	case fault.Invalid, fault.VersionConflict, fault.HistoryUnavailable, fault.Unavailable,
		fault.UnsafeMembership, fault.OperationAuthorization, fault.AuthorityFenced,
		fault.AuthorityPending, fault.ReplanRequired, fault.IdentityMismatch,
		fault.GroupRetired, fault.Overloaded, fault.Unsupported:
	default:
		return ApplyResult{}, err
	}
	var request struct {
		ProposalID string `json:"proposal_id"`
	}
	if decodeErr := json.Unmarshal(body, &request); decodeErr != nil {
		return ApplyResult{}, decodeErr
	}
	result := rejectProposalError(request.ProposalID, proposalErrorCode(classified.Code), err)
	result.ExpectedVersion, result.CurrentVersion = classified.ExpectedVersion, classified.CurrentVersion
	result.RebasedTo = classified.RebasedTo
	return result, nil
}
