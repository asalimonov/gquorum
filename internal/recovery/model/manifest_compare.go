package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"reflect"

	"github.com/asalimonov/gquorum/fault"
)

// AcceptedFromManifest projects only the persisted immutable inspection inputs.
// Operator summaries are derived reports and do not replace the source fingerprint.
func AcceptedFromManifest(r LocalManifest) AcceptedPlan {
	entries := append([]ArchiveEntry(nil), r.Archive...)
	for i := range entries {
		entries[i].Moved = false
		entries[i].Restored = false
	}
	return CloneAcceptedPlan(AcceptedPlan{Request: r.Request, Paths: r.Paths, Resources: r.Resources,
		Source: r.Source, Identity: r.Identity, Fingerprint: r.Fingerprint, Rules: r.Rules, Approved: r.Approved,
		Floors: r.Floors, Retired: r.Retired, Sources: r.Sources, Archive: entries,
		FilesRequired: r.Files.Required})
}

// DigestAccepted excludes resource settings and derived reports. It binds source
// evidence through its semantic fingerprint and the complete accepted inventory.
func DigestAccepted(input AcceptedPlan) (string, error) {
	p := CanonicalAccepted(input)
	p = AcceptedFromManifest(LocalManifest{Request: p.Request, Paths: p.Paths, Source: p.Source,
		Identity:    p.Identity,
		Fingerprint: p.Fingerprint, Rules: p.Rules, Approved: p.Approved, Floors: p.Floors, Retired: p.Retired,
		Sources: p.Sources, Archive: p.Archive, Files: FilesProgress{Required: p.FilesRequired}})
	data, err := EncodeCanonical(p)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// ValidateManifestUpdate refuses immutable edits and regressing durable progress.
func ValidateManifestUpdate(old, next LocalManifest) error {
	if old.Revision == math.MaxUint64 || next.Revision != old.Revision+1 {
		return fault.ErrConflict
	}
	if err := SameAcceptance(old, next); err != nil {
		return err
	}
	if err := validateWriteOnce(old, next); err != nil {
		return err
	}
	if err := validateProgressAdvance(old, next); err != nil {
		return err
	}
	return ValidateManifest(next)
}
func validateWriteOnce(old, next LocalManifest) error {
	oldValues := []any{old.Cancellation, old.SuccessorStartedAt, old.CompletedAt, old.CancelledAt,
		old.JoinedIdentity}
	newValues := []any{next.Cancellation, next.SuccessorStartedAt, next.CompletedAt, next.CancelledAt,
		next.JoinedIdentity}
	for i, oldValue := range oldValues {
		if !reflect.ValueOf(oldValue).IsNil() && !reflect.DeepEqual(oldValue, newValues[i]) {
			return fault.ErrConflict
		}
	}
	// Rollback and reverse movement are reserved, not initiated by this release.
	if !reflect.DeepEqual(old.Rollback, next.Rollback) || !reflect.DeepEqual(old.RolledBackAt, next.RolledBackAt) {
		return fault.ErrUnsupported
	}
	if old.Phase == RollingBack || old.Phase == RolledBack {
		return fault.ErrUnsupported
	}
	return nil
}
func validateProgressAdvance(old, next LocalManifest) error {
	if !phaseFollows(old.Phase, next.Phase) {
		return fault.ErrConflict
	}
	for i, entry := range old.Archive {
		if entry.Moved && !next.Archive[i].Moved || entry.Restored != next.Archive[i].Restored {
			return fault.ErrConflict
		}
	}
	if err := hookFollows(old.Files.Backup, next.Files.Backup); err != nil {
		return err
	}
	if err := hookFollows(old.Files.Undo, next.Files.Undo); err != nil {
		return err
	}
	if len(old.Seeds) != len(next.Seeds) {
		return fault.ErrConflict
	}
	for i, seed := range old.Seeds {
		if !seedFollows(seed, next.Seeds[i]) {
			return fault.ErrConflict
		}
	}
	return nil
}
func phaseFollows(old, next Phase) bool {
	if old == next {
		return true
	}
	allowed := map[Phase][]Phase{Approved: {Files, Cancelling}, Files: {Cancelling, Archiving},
		Cancelling: {Cancelled},
		Archiving:  {AwaitingStart, JoinReady}, AwaitingStart: {Importing}, Importing: {Validating},
		Validating: {Complete}, JoinReady: {Joined}}
	for _, phase := range allowed[old] {
		if phase == next {
			return true
		}
	}
	return false
}
func hookFollows(old, next HookProgress) error {
	if old == next {
		return nil
	}
	if old.State == stateComplete || next.Attempt < old.Attempt {
		return fault.ErrConflict
	}
	if next.State == stateRunning {
		if old.Attempt == math.MaxUint64 || next.Attempt != old.Attempt+1 {
			return fault.ErrConflict
		}
	} else if next.State != stateComplete || next.Attempt != old.Attempt {
		return fault.ErrConflict
	}
	return nil
}
func seedFollows(old, next SeedProgress) bool {
	if old.GroupID != next.GroupID || old.Destination.ReplicaID != 0 &&
		old.Destination != next.Destination || next.Attempt < old.Attempt {
		return false
	}
	rank := seedRanks
	if rank[next.State] < rank[old.State] || rank[next.State] > rank[old.State]+1 {
		return false
	}
	if next.Attempt != old.Attempt && (old.Attempt == math.MaxUint64 || next.Attempt != old.Attempt+1 ||
		old.State != statePending) {
		return false
	}
	if old.MarkerDigest != "" && old.MarkerDigest != next.MarkerDigest {
		return false
	}
	return old.Checkpoint.Index <= next.Checkpoint.Index && old.Checkpoint.Term <= next.Checkpoint.Term
}

// SameAcceptance compares all accepted fields, excluding revision and progress.
func SameAcceptance(a, b LocalManifest) error {
	if a.Acceptance != b.Acceptance || a.PlanDigest != b.PlanDigest || a.Schema != b.Schema ||
		a.BackupName != b.BackupName || !a.AcceptedAt.Equal(b.AcceptedAt) {
		return fault.ErrConflict
	}
	first, err := EncodeCanonical(AcceptedFromManifest(a))
	if err != nil {
		return err
	}
	second, err := EncodeCanonical(AcceptedFromManifest(b))
	if err != nil {
		return err
	}
	if !bytes.Equal(first, second) {
		return fault.ErrConflict
	}
	return nil
}
