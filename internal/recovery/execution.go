package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	recoveryexec "github.com/asalimonov/gquorum/internal/recovery/exec"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/recovery"
)

// Run executes or resumes one accepted operation under exclusive ownership.
func (r *Recovery) Run(ctx context.Context, id string) (status recovery.Status, err error) {
	if err := ctx.Err(); err != nil {
		return status, err
	}
	if err := model.ValidateRecoveryID(id); err != nil {
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
	status = model.StatusFromManifest(manifest)
	if err := r.validateExecutionOptions(owner, manifest); err != nil {
		return status, inspectionRefusal(err, nil)
	}
	executor, err := recoveryexec.New(recoveryexec.Config{Owner: owner, Journal: journal, Files: r.options.Files})
	if err != nil {
		return status, inspectionRefusal(err, nil)
	}
	status, err = executor.Run(ctx)
	return status, executionRefusal(err, status)
}

// Cancel undoes application preparation only before library archiving begins.
func (r *Recovery) Cancel(ctx context.Context, id, actor, reason string) (status recovery.Status, err error) {
	if err := ctx.Err(); err != nil {
		return status, err
	}
	valid := func(value string) bool {
		return strings.TrimSpace(value) != "" && !strings.ContainsRune(value, 0) && utf8.ValidString(value)
	}
	if model.ValidateRecoveryID(id) != nil || !valid(actor) || !valid(reason) {
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
	status = model.StatusFromManifest(manifest)
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

// Status reads historical operation progress; an empty ID selects the current operation.
// A valid result may accompany an error describing unrelated invalid namespace entries.
// Without a current pointer, use List to discover pending operations by their exact IDs.
func (r *Recovery) Status(ctx context.Context, id string) (recovery.Status, error) {
	if err := ctx.Err(); err != nil {
		return recovery.Status{}, err
	}
	paths, err := r.readOnlyPaths()
	if err != nil {
		return recovery.Status{}, statusRefusal(err)
	}
	record, err := recoverydisk.Read(ctx, paths, id)
	if record.Request.RecoveryID == "" {
		return recovery.Status{}, statusRefusal(err)
	}
	return model.StatusFromManifest(record), statusRefusal(err)
}

// List reads all recognized operation reports in descending acceptance order.
func (r *Recovery) List(ctx context.Context) ([]recovery.Status, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths, err := r.readOnlyPaths()
	if err != nil {
		return nil, statusRefusal(err)
	}
	records, err := recoverydisk.List(ctx, paths)
	statuses := make([]recovery.Status, 0, len(records))
	for _, record := range records {
		statuses = append(statuses, model.StatusFromManifest(record))
	}
	return statuses, statusRefusal(err)
}
func (r *Recovery) readOnlyPaths() (model.Paths, error) {
	root, err := filepath.Abs(r.source.Paths.StorageDir)
	if err != nil {
		return model.Paths{}, err
	}
	// WAL mounts and current live identity are deliberately irrelevant to history.
	return model.Paths{StorageDir: root}, nil
}
func statusRefusal(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, fault.ErrHistoryUnavailable) {
		return recoveryRefusal("operation_not_found", "recovery operation is unavailable", err)
	}
	cause := fault.New(fault.Unreadable, "invalid recovery namespace", err)
	return recoveryRefusal("invalid_namespace", "recovery history contains invalid or conflicting evidence", cause)
}
