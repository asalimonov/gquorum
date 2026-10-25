package gquorum

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryinspect"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Inspect reads source evidence and builds a reviewable recovery report.
func (r *Recovery) Inspect(ctx context.Context, input recovery.Request) (plan recovery.Plan, err error) {
	if err := ctx.Err(); err != nil {
		return plan, err
	}
	request, err := recoveryinspect.EffectiveRequest(input)
	if err != nil {
		return plan, recoveryRefusal("invalid_request", "invalid recovery inspection request", err)
	}
	owner, err := recoverydisk.Acquire(ctx, r.source.Paths)
	if err != nil {
		return plan, inspectionRefusal(err, nil)
	}
	defer func() { err = errors.Join(err, releaseRecoveryOwner(owner)) }()
	prior, err := r.priorApproval(ctx, owner, request)
	if err != nil {
		return plan, inspectionRefusal(err, nil)
	}
	if prior != nil {
		accepted := recoverymodel.AcceptedFromManifest(*prior)
		probeErr := r.probeAccepted(ctx, &accepted)
		report, reportErr := recoverymodel.PlanFromAccepted(accepted)
		return report, errors.Join(inspectionRefusal(probeErr, accepted.Issues), reportErr)
	}
	accepted, inspectErr := r.inspectOwned(ctx, owner, request)
	plan, err = recoverymodel.PlanFromAccepted(accepted)
	return plan, errors.Join(inspectionRefusal(inspectErr, accepted.Issues), err)
}
