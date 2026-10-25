package recoverydisk

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
)

// MarkFiles records one exact callback attempt before or after its joined work.
func (j *Journal) MarkFiles(ctx context.Context, action recoverymodel.FilesAction, state string,
	attempt uint64) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		progress := recoverymodel.HookProgress{State: state, Attempt: attempt}
		switch action {
		case recoverymodel.FilesBackup:
			if next.Phase != recoverymodel.Files {
				return fault.ErrConflict
			}
			next.Files.Backup = progress
		case recoverymodel.FilesUndo:
			if next.Phase != recoverymodel.Cancelling {
				return fault.ErrConflict
			}
			next.Files.Undo = progress
		default:
			return fault.ErrInvalid
		}
		return nil
	})
}

// BeginCancellation preserves the first matching audit, including its timestamp.
func (j *Journal) BeginCancellation(ctx context.Context, audit recoverymodel.ActionAudit) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		if next.Cancellation != nil {
			if next.Cancellation.Actor != audit.Actor || next.Cancellation.Reason != audit.Reason {
				return fault.ErrConflict
			}
			if next.Phase != recoverymodel.Cancelling && next.Phase != recoverymodel.Cancelled {
				return fault.ErrConflict
			}
			return nil
		}
		if next.Phase != recoverymodel.Approved && next.Phase != recoverymodel.Files {
			return fault.ErrConflict
		}
		next.Cancellation = &audit
		next.Phase = recoverymodel.Cancelling
		return nil
	})
}

// CompleteCancellation releases the guard only after any attempted backup was undone.
func (j *Journal) CompleteCancellation(ctx context.Context, at time.Time) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		if next.Phase == recoverymodel.Cancelled {
			return nil
		}
		if next.Phase != recoverymodel.Cancelling {
			return fault.ErrConflict
		}
		next.CancelledAt = &at
		next.Phase = recoverymodel.Cancelled
		next.Failure = nil
		return nil
	})
}

// MarkSuccessorStarted durably prohibits rollback before any successor file opens.
func (j *Journal) MarkSuccessorStarted(ctx context.Context, at time.Time) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		if next.SuccessorStartedAt != nil {
			return nil
		}
		if next.Phase != recoverymodel.AwaitingStart && next.Phase != recoverymodel.JoinReady {
			return fault.ErrConflict
		}
		next.SuccessorStartedAt = &at
		return nil
	})
}

// MarkArchive acknowledges the exact accepted move, after its parents were synced.
func (j *Journal) MarkArchive(ctx context.Context, root, name string) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		if next.Phase != recoverymodel.Archiving {
			return fault.ErrConflict
		}
		for i := range next.Archive {
			if next.Archive[i].Root == root && next.Archive[i].Name == name {
				next.Archive[i].Moved = true
				return nil
			}
		}
		return fault.ErrIdentityMismatch
	})
}

// MarkSeed records preparation/replay progress without manufacturing consumption proof.
func (j *Journal) MarkSeed(ctx context.Context, progress recoverymodel.SeedProgress) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		if next.Phase != recoverymodel.Importing {
			return fault.ErrConflict
		}
		for i, seed := range next.Seeds {
			if seed.GroupID == progress.GroupID {
				next.Seeds[i] = progress
				return nil
			}
		}
		return fault.ErrIdentityMismatch
	})
}

// SetPhase advances one validated state and retains the latest owned diagnostic.
func (j *Journal) SetPhase(ctx context.Context, phase recoverymodel.Phase, issue *recoverymodel.Issue) error {
	return j.change(ctx, func(next *recoverymodel.LocalManifest) error {
		next.Phase = phase
		next.Failure = nil
		if issue != nil {
			value := *issue
			next.Failure = &value
		}
		if phase == recoverymodel.Complete && next.CompletedAt == nil {
			at := j.owner.clock()
			next.CompletedAt = &at
		}
		return nil
	})
}
