// Package recoveryexec executes one journaled offline recovery under borrowed root ownership.
package recoveryexec

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryinspect"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
)

// Config supplies one exact operation; its caller retains ownership until return.
type Config struct {
	Owner   *recoverydisk.Owner
	Journal *recoverydisk.Journal
	Files   model.FilesHook
}

// Executor has no independent lifetime, workers or application handles.
type Executor struct{ config Config }

// New validates a borrowed per-call execution scope without acquiring resources.
func New(config Config) (*Executor, error) {
	if config.Owner == nil || config.Journal == nil {
		return nil, fault.ErrInvalid
	}
	manifest := config.Journal.View()
	if manifest.Files.Required != (config.Files != nil) || manifest.Paths != config.Owner.Paths() {
		return nil, fault.ErrConflict
	}
	return &Executor{config: config}, nil
}

// Run resumes only the journal's durable direction and joins every callback.
func (e *Executor) Run(ctx context.Context) (model.Status, error) {
	err := e.execute(ctx)
	return model.StatusFromManifest(e.config.Journal.View()), err
}

func (e *Executor) execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.config.Owner.CheckRecoveryNamespaces(ctx); err != nil {
		return err
	}
	manifest := e.config.Journal.View()
	switch manifest.Phase {
	case model.Cancelling:
		return e.resumeCancellation(ctx)
	case model.Cancelled, model.AwaitingStart, model.Importing, model.Validating, model.Complete,
		model.JoinReady, model.Joined:
		return nil
	case model.RollingBack, model.RolledBack:
		// TODO(not planned): rollback after Archiving; preserve the guarded evidence.
		return fault.ErrUnsupported
	case model.Approved:
		if err := e.config.Journal.SetPhase(ctx, model.Files, nil); err != nil {
			return err
		}
	case model.Files, model.Archiving:
	default:
		return fault.ErrUnreadable
	}
	if e.config.Journal.View().Phase == model.Files {
		if err := e.prepareFiles(ctx); err != nil {
			return err
		}
	}
	if err := e.archive(ctx); err != nil {
		return e.recordFailure(err, "archive_failed", model.RemedyRetry)
	}
	phase := model.AwaitingStart
	if manifest.Request.Mode == model.ResetForJoin {
		phase = model.JoinReady
	}
	return e.config.Journal.SetPhase(ctx, phase, nil)
}

// Cancel records explicit intent before Undo and refuses every post-archive phase.
func (e *Executor) Cancel(ctx context.Context, actor, reason string) (model.Status, error) {
	err := e.beginCancelFlow(ctx, actor, reason)
	return model.StatusFromManifest(e.config.Journal.View()), err
}

func (e *Executor) beginCancelFlow(ctx context.Context, actor, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.config.Owner.CheckRecoveryNamespaces(ctx); err != nil {
		return err
	}
	manifest := e.config.Journal.View()
	if manifest.Cancellation != nil && (manifest.Cancellation.Actor != actor ||
		manifest.Cancellation.Reason != reason) {
		return fault.ErrConflict
	}
	if manifest.Phase == model.Cancelled {
		return e.config.Journal.ClearCurrent(ctx)
	}
	if manifest.Phase == model.Cancelling {
		return e.resumeCancellation(ctx)
	}
	if manifest.Phase != model.Approved && manifest.Phase != model.Files {
		return fault.ErrConflict
	}
	if err := e.verifyOriginal(ctx); err != nil {
		return err
	}
	audit := model.ActionAudit{Actor: actor, Reason: reason, At: time.Now().UTC()}
	if err := e.config.Journal.BeginCancellation(ctx, audit); err != nil {
		return err
	}
	return e.resumeCancellation(ctx)
}

func (e *Executor) resumeCancellation(ctx context.Context) error {
	manifest := e.config.Journal.View()
	if manifest.Phase != model.Cancelling || manifest.Cancellation == nil {
		return fault.ErrConflict
	}
	if err := e.verifyOriginal(ctx); err != nil {
		return err
	}
	if manifest.Files.Backup.Attempt > 0 {
		if err := e.files(ctx, model.FilesUndo); err != nil {
			return e.recordFailure(err, "files_undo", model.RemedyRetry)
		}
	}
	if err := e.verifyOriginal(ctx); err != nil {
		return e.recordFailure(err, "source_changed", model.RemedyInvestigate)
	}
	if err := e.config.Journal.CompleteCancellation(ctx, time.Now().UTC()); err != nil {
		return err
	}
	return e.config.Journal.ClearCurrent(ctx)
}

func (e *Executor) files(ctx context.Context, action model.FilesAction) error {
	manifest := e.config.Journal.View()
	progress := manifest.Files.Backup
	if action == model.FilesUndo {
		progress = manifest.Files.Undo
	}
	if progress.State == "complete" {
		return nil
	}
	if !manifest.Files.Required {
		return e.config.Journal.MarkFiles(ctx, action, "complete", 0)
	}
	if progress.Attempt == math.MaxUint64 {
		return fault.New(fault.Invalid, "file callback attempts exhausted", nil)
	}
	application := filepath.Join(recoverydisk.Namespace, manifest.BackupName, "application")
	if err := e.config.Owner.EnsureDirectory("system", application); err != nil {
		return err
	}
	attempt := progress.Attempt + 1
	if err := e.config.Journal.MarkFiles(ctx, action, "running", attempt); err != nil {
		return err
	}
	task := model.FilesTask{Action: action, RecoveryID: manifest.Request.RecoveryID,
		PlanDigest: manifest.PlanDigest,
		Mode:       manifest.Request.Mode, Source: manifest.Source, ClusterID: manifest.Identity.ClusterID,
		BackupDir: filepath.Join(manifest.Paths.StorageDir, application)}
	for _, group := range manifest.Approved.LoadGroups() {
		task.Groups = append(task.Groups, group.ID)
	}
	// Direct invocation deliberately retains ownership when a callback ignores
	// cancellation. A successful return is recorded before observing cancellation.
	if err := e.config.Files(ctx, task); err != nil {
		return errors.Join(err, ctx.Err())
	}
	if err := e.config.Journal.MarkFiles(context.WithoutCancel(ctx), action, "complete", attempt); err != nil {
		return err
	}
	return ctx.Err()
}

func (e *Executor) verifyOriginal(ctx context.Context) error {
	manifest := e.config.Journal.View()
	if manifest.SuccessorStartedAt != nil {
		return fault.ErrConflict
	}
	if err := e.requireOriginalLocations(ctx); err != nil {
		return err
	}
	accepted, err := recoveryinspect.Inspect(ctx, e.config.Owner, manifest.Request, manifest.Resources)
	if err != nil {
		return err
	}
	accepted.FilesRequired = manifest.Files.Required
	digest, err := model.DigestAccepted(accepted)
	if err != nil {
		return err
	}
	if digest != manifest.PlanDigest {
		return fault.New(fault.Conflict, "accepted library source evidence changed", nil)
	}
	return nil
}

func (e *Executor) recordFailure(cause error, code string, remedy model.Remedy) error {
	manifest := e.config.Journal.View()
	message := strings.ReplaceAll(strings.ToValidUTF8(cause.Error(), "�"), "\x00", "�")
	if strings.TrimSpace(message) == "" {
		message = "recovery operation failed"
	}
	const maxDiagnostic = 4096
	if len(message) > maxDiagnostic {
		message = strings.ToValidUTF8(message[:maxDiagnostic], "�")
	}
	issue := model.Issue{Code: code, Message: message, Blocking: true, Remedy: remedy}
	err := e.config.Journal.SetPhase(context.Background(), manifest.Phase, &issue)
	return errors.Join(cause, err)
}

func (e *Executor) prepareFiles(ctx context.Context) error {
	if err := e.verifyOriginal(ctx); err != nil {
		return e.recordFailure(err, "source_changed", model.RemedyInvestigate)
	}
	if err := e.files(ctx, model.FilesBackup); err != nil {
		return e.recordFailure(err, "files_backup", model.RemedyRetry)
	}
	if err := e.verifyOriginal(ctx); err != nil {
		return e.recordFailure(err, "source_changed", model.RemedyInvestigate)
	}
	if err := e.prepareArchive(ctx); err != nil {
		return e.recordFailure(err, "archive_preflight", model.RemedyRetry)
	}
	return e.config.Journal.SetPhase(ctx, model.Archiving, nil)
}
