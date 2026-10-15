package metaservice

import (
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func applyResultError(result metaengine.ApplyResult) error {
	if result.Accepted {
		return nil
	}
	code := proposalFaultCode(string(result.Code))
	if result.Placement != nil {
		return &topology.PlacementError{Code: code, Report: result.Placement.Clone()}
	}
	return &fault.Error{Code: code, Message: result.Reason, ExpectedVersion: result.ExpectedVersion,
		CurrentVersion: result.CurrentVersion, RebasedTo: result.RebasedTo}
}

func proposalFaultCode(code string) fault.Code {
	switch code {
	case "version_conflict":
		return fault.VersionConflict
	case "history_unavailable":
		return fault.HistoryUnavailable
	case "overloaded":
		return fault.Overloaded
	case "authority_pending":
		return fault.AuthorityPending
	case "authority_fenced":
		return fault.AuthorityFenced
	case "authorization", "superseded":
		return fault.OperationAuthorization
	default:
		return fault.Invalid
	}
}

func (r MutationResult) outcome(err error) (MutationResult, error) {
	owned := r
	// Remove the old field when all callers use OperationID at the API cutover.
	if owned.OperationID == "" {
		owned.OperationID = topology.OperationID(r.OperatorID)
	}
	var conflict *fault.Error
	if errors.As(err, &conflict) {
		if conflict.RebasedTo != 0 {
			owned.RebasedTo = conflict.RebasedTo
		}
	}
	if report := reportFromError(err); report != nil {
		owned.Placement = report
	}
	return owned, err
}
