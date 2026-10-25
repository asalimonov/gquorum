package gquorum

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Approve durably accepts the exact inspected request without moving source data.
func (r *Recovery) Approve(ctx context.Context, request recovery.Request,
	acceptance recovery.Acceptance) (operation recovery.Operation, err error) {
	if err := ctx.Err(); err != nil {
		return operation, err
	}
	if request.RecoveryID == "" || recoverymodel.ValidateRequest(request) != nil {
		return operation, recoveryRefusal("invalid_request",
			"approve requires the effective inspected request", fault.ErrInvalid)
	}
	if !acceptance.AcknowledgeDataLoss || request.Mode == recovery.Rebuild && !acceptance.OldDeploymentFenced {
		return operation, recoveryRefusal("acceptance_required",
			"potential data loss and old-deployment fencing require explicit acceptance", fault.ErrInvalid)
	}
	owner, err := recoverydisk.Acquire(ctx, r.source.Paths)
	if err != nil {
		return operation, inspectionRefusal(err, nil)
	}
	defer func() { err = errors.Join(err, releaseRecoveryOwner(owner)) }()
	prior, err := r.priorApproval(ctx, owner, request)
	if err != nil {
		return operation, inspectionRefusal(err, nil)
	}
	var accepted recoverymodel.AcceptedPlan
	if prior != nil {
		accepted = recoverymodel.AcceptedFromManifest(*prior)
	} else {
		accepted, err = r.inspectOwned(ctx, owner, request)
		if err != nil {
			return operation, inspectionRefusal(err, accepted.Issues)
		}
	}
	digest, err := recoverymodel.DigestAccepted(accepted)
	if err != nil {
		return operation, err
	}
	if digest != acceptance.PlanDigest {
		return operation, recoveryRefusal("source_changed",
			"acceptance does not match exact retained source evidence; inspect again", fault.ErrConflict)
	}
	journal, err := recoverydisk.Create(ctx, owner, accepted, acceptance)
	if journal != nil {
		operation = recoverymodel.OperationFromManifest(journal.View())
	}
	return operation, inspectionRefusal(err, nil)
}
