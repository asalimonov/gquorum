package gquorum

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryexec"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Cancel undoes application preparation only before library archiving begins.
func (r *Recovery) Cancel(ctx context.Context, id, actor, reason string) (status recovery.Status, err error) {
	if err := ctx.Err(); err != nil {
		return status, err
	}
	valid := func(value string) bool {
		return strings.TrimSpace(value) != "" && !strings.ContainsRune(value, 0) && utf8.ValidString(value)
	}
	if recoverymodel.ValidateRecoveryID(id) != nil || !valid(actor) || !valid(reason) {
		return status, recoveryRefusal("invalid_request",
			"cancellation requires an exact recovery ID, actor and reason", fault.ErrInvalid)
	}
	owner, err := recoverydisk.Acquire(ctx, r.source.Paths)
	if err != nil {
		return status, inspectionRefusal(err, nil)
	}
	defer func() { err = errors.Join(err, releaseRecoveryOwner(owner)) }()
	journal, err := recoverydisk.Open(ctx, owner, id)
	if err != nil {
		return status, inspectionRefusal(err, nil)
	}
	manifest := journal.View()
	status = recoverymodel.StatusFromManifest(manifest)
	if err := r.validateExecutionOptions(owner, manifest); err != nil {
		return status, inspectionRefusal(err, nil)
	}
	executor, err := recoveryexec.New(recoveryexec.Config{Owner: owner, Journal: journal, Files: r.options.Files})
	if err != nil {
		return status, inspectionRefusal(err, nil)
	}
	status, err = executor.Cancel(ctx, actor, reason)
	return status, executionRefusal(err, status)
}

func executionRefusal(err error, status recovery.Status) error {
	if err == nil {
		return nil
	}
	if status.Issue != nil {
		return &recovery.Refusal{Issue: *status.Issue, Cause: err}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	remedy := recovery.RemedyInvestigate
	if status.Phase == recovery.Files || status.Phase == recovery.Cancelling || status.Phase == recovery.Archiving {
		remedy = recovery.RemedyRetry
	}
	return &recovery.Refusal{Issue: recovery.Issue{Code: "recovery_refused", Blocking: true,
		Message: "recovery remains at its last durable phase", Remedy: remedy}, Cause: err}
}
