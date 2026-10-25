package disk

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

// MarkFiles records one exact callback attempt before or after its joined work.
func (j *Journal) MarkFiles(ctx context.Context, action model.FilesAction, state string,
	attempt uint64) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		progress := model.HookProgress{State: state, Attempt: attempt}
		switch action {
		case model.FilesBackup:
			if next.Phase != model.Files {
				return fault.ErrConflict
			}
			next.Files.Backup = progress
		case model.FilesUndo:
			if next.Phase != model.Cancelling {
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
func (j *Journal) BeginCancellation(ctx context.Context, audit model.ActionAudit) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		if next.Cancellation != nil {
			if next.Cancellation.Actor != audit.Actor || next.Cancellation.Reason != audit.Reason {
				return fault.ErrConflict
			}
			if next.Phase != model.Cancelling && next.Phase != model.Cancelled {
				return fault.ErrConflict
			}
			return nil
		}
		if next.Phase != model.Approved && next.Phase != model.Files {
			return fault.ErrConflict
		}
		next.Cancellation = &audit
		next.Phase = model.Cancelling
		return nil
	})
}

// CompleteCancellation releases the guard only after any attempted backup was undone.
func (j *Journal) CompleteCancellation(ctx context.Context, at time.Time) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		if next.Phase == model.Cancelled {
			return nil
		}
		if next.Phase != model.Cancelling {
			return fault.ErrConflict
		}
		next.CancelledAt = &at
		next.Phase = model.Cancelled
		next.Failure = nil
		return nil
	})
}

// MarkSuccessorStarted durably prohibits rollback before any successor file opens.
func (j *Journal) MarkSuccessorStarted(ctx context.Context, at time.Time) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		if next.SuccessorStartedAt != nil {
			return nil
		}
		if next.Phase != model.AwaitingStart && next.Phase != model.JoinReady {
			return fault.ErrConflict
		}
		next.SuccessorStartedAt = &at
		return nil
	})
}

// MarkArchive acknowledges the exact accepted move, after its parents were synced.
func (j *Journal) MarkArchive(ctx context.Context, root, name string) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		if next.Phase != model.Archiving {
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
func (j *Journal) MarkSeed(ctx context.Context, progress model.SeedProgress) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		if next.Phase != model.Importing {
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
func (j *Journal) SetPhase(ctx context.Context, phase model.Phase, issue *model.Issue) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		next.Phase = phase
		next.Failure = nil
		if issue != nil {
			value := model.LimitFailure(*issue)
			next.Failure = &value
		}
		if phase == model.Complete && next.CompletedAt == nil {
			at := j.owner.clock()
			next.CompletedAt = &at
		}
		return nil
	})
}

// MarkJoined records only an exact durable successor pin acquired by normal admission.
func (j *Journal) MarkJoined(ctx context.Context, identity topology.Identity) error {
	return j.change(ctx, func(next *model.LocalManifest) error {
		identity.InstanceID = ""
		if next.Phase == model.Joined {
			if next.JoinedIdentity == nil || *next.JoinedIdentity != identity {
				return fault.ErrIdentityMismatch
			}
			return nil
		}
		if next.Phase != model.JoinReady || next.SuccessorStartedAt == nil ||
			identity.NodeName != next.Identity.NodeName || identity.ClusterID != next.Identity.ClusterID ||
			identity.MemberID == 0 {
			return fault.ErrOperationAuthorization
		}
		next.JoinedIdentity, next.Phase = &identity, model.Joined
		next.Failure = nil
		return nil
	})
}

// ReconcileSeed advances acknowledged progress without regressing prior receipts.
func (j *Journal) ReconcileSeed(ctx context.Context,
	progress model.SeedProgress) error {
	states := []string{"pending", "prepared", "installed", "replayed", "consumed"}
	target := slices.Index(states, progress.State)
	if target < 1 {
		return fault.ErrInvalid
	}
	for {
		record := j.View()
		position := slices.IndexFunc(record.Seeds,
			func(seed model.SeedProgress) bool { return seed.GroupID == progress.GroupID })
		if position < 0 {
			return fault.ErrIdentityMismatch
		}
		old := record.Seeds[position]
		rank := slices.Index(states, old.State)
		if old.Attempt != 0 && (old.Destination != progress.Destination ||
			old.MarkerDigest != progress.MarkerDigest ||
			old.Attempt != progress.Attempt) {
			return fault.ErrIdentityMismatch
		}
		if rank >= target {
			return nil
		}
		next := progress
		next.State = states[rank+1]
		if rank+1 < slices.Index(states, "replayed") {
			next.Checkpoint = model.Position{}
		}
		if err := j.MarkSeed(ctx, next); err != nil {
			return err
		}
	}
}
