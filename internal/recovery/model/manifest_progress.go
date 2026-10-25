package model

import (
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func validateManifestProgress(r LocalManifest) error {
	if err := validateAudits(r); err != nil {
		return err
	}
	if err := validateHook(r.Files.Backup); err != nil {
		return err
	}
	if err := validateHook(r.Files.Undo); err != nil {
		return err
	}
	if r.Files.Required && r.Files.Backup.State == stateComplete && r.Files.Backup.Attempt == 0 {
		return fault.ErrInvalid
	}
	if !r.Files.Required && (r.Files.Backup.Attempt != 0 || r.Files.Undo.Attempt != 0) {
		return fault.ErrInvalid
	}
	if r.Phase == RollingBack || r.Phase == RolledBack {
		return validateReservedProgress(r)
	}
	if r.Rollback != nil || r.RolledBackAt != nil {
		return fault.ErrInvalid
	}
	for _, entry := range r.Archive {
		if entry.Restored {
			return fault.ErrInvalid
		}
	}
	return validatePhaseFacts(r)
}
func validateAudits(r LocalManifest) error {
	for _, at := range []*time.Time{r.CompletedAt, r.CancelledAt, r.SuccessorStartedAt, r.RolledBackAt} {
		if at != nil && at.IsZero() {
			return fault.ErrInvalid
		}
	}
	for _, audit := range []*ActionAudit{r.Cancellation, r.Rollback} {
		if audit != nil && (!validText(audit.Actor) || !validText(audit.Reason) || audit.At.IsZero()) {
			return fault.ErrInvalid
		}
	}
	return nil
}
func validateHook(h HookProgress) error {
	switch h.State {
	case statePending:
		if h.Attempt != 0 {
			return fault.ErrInvalid
		}
	case stateRunning:
		if h.Attempt == 0 {
			return fault.ErrInvalid
		}
	case stateComplete:
	default:
		return fault.ErrInvalid
	}
	return nil
}
func validateReservedProgress(r LocalManifest) error {
	if r.Rollback == nil {
		return fault.ErrInvalid
	}
	if r.Phase == RolledBack && r.RolledBackAt == nil {
		return fault.ErrInvalid
	}
	return nil
}
func validatePhaseFacts(r LocalManifest) error {
	switch r.Phase {
	case Approved, Files, Cancelling, Cancelled:
		return validateBeforeArchive(r)
	case Archiving:
		if r.SuccessorStartedAt != nil {
			return fault.ErrInvalid
		}
	case AwaitingStart, Importing, Validating, Complete:
		if r.Request.Mode != Rebuild {
			return fault.ErrInvalid
		}
	case JoinReady, Joined:
		if r.Request.Mode != ResetForJoin {
			return fault.ErrInvalid
		}
	default:
		return fault.ErrInvalid
	}
	if r.Files.Backup.State != stateComplete || r.Cancellation != nil || r.CancelledAt != nil ||
		r.Files.Undo != (HookProgress{State: statePending}) {
		return fault.ErrInvalid
	}
	if r.Phase != Archiving {
		for _, entry := range r.Archive {
			if !entry.Moved {
				return fault.ErrInvalid
			}
		}
	}
	return validateSuccessorFacts(r)
}
func validateBeforeArchive(r LocalManifest) error {
	if err := validateNoArchiveEffects(r); err != nil {
		return err
	}
	if r.Phase == Cancelling || r.Phase == Cancelled {
		return validateCancellationPhase(r)
	}
	if r.Phase == Approved && r.Files.Backup != (HookProgress{State: statePending}) {
		return fault.ErrInvalid
	}
	if r.Cancellation != nil || r.CancelledAt != nil || r.Files.Undo != (HookProgress{State: statePending}) {
		return fault.ErrInvalid
	}
	return nil
}
func validateNoArchiveEffects(r LocalManifest) error {
	if r.SuccessorStartedAt != nil || r.CompletedAt != nil || r.JoinedIdentity != nil {
		return fault.ErrInvalid
	}
	for _, entry := range r.Archive {
		if entry.Moved {
			return fault.ErrInvalid
		}
	}
	for _, seed := range r.Seeds {
		if seed.State != statePending || seed.Attempt != 0 {
			return fault.ErrInvalid
		}
	}
	return nil
}
func validateCancellationPhase(r LocalManifest) error {
	if r.Cancellation == nil {
		return fault.ErrInvalid
	}
	if r.Phase == Cancelled {
		if r.CancelledAt == nil || r.Files.Backup.Attempt > 0 && r.Files.Undo.State != stateComplete {
			return fault.ErrInvalid
		}
	} else if r.CancelledAt != nil {
		return fault.ErrInvalid
	}
	return nil
}
func validateSuccessorFacts(r LocalManifest) error {
	if err := validatePreStartSeeds(r); err != nil {
		return err
	}
	if r.Phase == Importing || r.Phase == Validating || r.Phase == Complete || r.Phase == Joined {
		if r.SuccessorStartedAt == nil {
			return fault.ErrInvalid
		}
	}
	if r.Phase == Validating || r.Phase == Complete {
		for _, seed := range r.Seeds {
			if seed.State != stateConsumed {
				return fault.ErrInvalid
			}
		}
	}
	if r.Phase == Complete {
		if r.CompletedAt == nil {
			return fault.ErrInvalid
		}
	} else if r.CompletedAt != nil {
		return fault.ErrInvalid
	}
	return validateJoinedIdentity(r)
}
func validatePreStartSeeds(r LocalManifest) error {
	if r.Phase != Archiving && r.Phase != AwaitingStart && r.Phase != JoinReady {
		return nil
	}
	for _, seed := range r.Seeds {
		if seed.State != statePending || seed.Attempt != 0 || seed.Destination.ReplicaID != 0 {
			return fault.ErrInvalid
		}
	}
	return nil
}
func validateJoinedIdentity(r LocalManifest) error {
	if r.Phase != Joined {
		if r.JoinedIdentity != nil {
			return fault.ErrInvalid
		}
		return nil
	}
	id := r.JoinedIdentity
	if id == nil {
		return fault.ErrIdentityMismatch
	}
	if id.NodeName != r.Identity.NodeName || id.ClusterID != r.Identity.ClusterID ||
		id.MemberID == 0 {
		return fault.ErrIdentityMismatch
	}
	return nil
}
