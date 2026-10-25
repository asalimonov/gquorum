package gquorum

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryexec"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Run executes or resumes one accepted operation under exclusive ownership.
func (r *Recovery) Run(ctx context.Context, id string) (status recovery.Status, err error) {
	if err := ctx.Err(); err != nil {
		return status, err
	}
	if err := recoverymodel.ValidateRecoveryID(id); err != nil {
		return status, inspectionRefusal(err, nil)
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
	if manifest.Phase == recovery.Approved || manifest.Phase == recovery.Files ||
		manifest.Phase == recovery.Archiving {
		// Temporary gate until Task12 provides the matching safe Node startup.
		return status, pendingRecovery(ctx, "destructive recovery execution")
	}
	executor, err := recoveryexec.New(recoveryexec.Config{Owner: owner, Journal: journal, Files: r.options.Files})
	if err != nil {
		return status, inspectionRefusal(err, nil)
	}
	status, err = executor.Run(ctx)
	return status, executionRefusal(err, status)
}
