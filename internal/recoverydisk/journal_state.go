package recoverydisk

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"golang.org/x/sys/unix"
)

func newManifest(p recoverymodel.AcceptedPlan, a recoverymodel.Acceptance, at time.Time,
	name string) recoverymodel.LocalManifest {
	r := recoverymodel.LocalManifest{Schema: recoverymodel.SchemaVersion, Revision: 1, Request: p.Request,
		Acceptance: a, Paths: p.Paths,
		Resources: p.Resources, Source: p.Source, Identity: p.Identity, Fingerprint: p.Fingerprint,
		PlanDigest: a.PlanDigest,
		Rules:      p.Rules, Approved: p.Approved, Floors: p.Floors, Retired: p.Retired, Sources: p.Sources,
		Archive: p.Archive,
		Files: recoverymodel.FilesProgress{Required: p.FilesRequired,
			Backup: recoverymodel.HookProgress{State: "pending"},
			Undo:   recoverymodel.HookProgress{State: "pending"}},
		Phase: recoverymodel.Approved, AcceptedAt: at, BackupName: name}
	if p.Request.Mode == recoverymodel.Rebuild {
		for _, group := range p.Approved.LoadGroups() {
			r.Seeds = append(r.Seeds,
				recoverymodel.SeedProgress{GroupID: group.ID, State: "pending"})
		}
	}
	return recoverymodel.CloneManifest(r)
}

// Update atomically applies a caller's exact revision as a compare-and-swap.
// The supplied Revision is the observed predecessor; the journal increments it.
func (j *Journal) Update(ctx context.Context, next recoverymodel.LocalManifest) error {
	expected := next.Revision
	return j.change(ctx, func(current *recoverymodel.LocalManifest) error {
		if current.Revision != expected {
			return fault.ErrConflict
		}
		*current = recoverymodel.CloneManifest(next)
		return nil
	})
}
func (j *Journal) change(ctx context.Context, edit func(*recoverymodel.LocalManifest) error) error {
	j.owner.journalMu.Lock()
	defer j.owner.journalMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.owner.CheckRoots(); err != nil {
		return err
	}
	old := j.value
	relative := manifestRelative(old.BackupName)
	readErr := readRecordFrom(j.owner.system.root, relative, &old, recoverymodel.ManifestMaxBytes)
	if readErr != nil {
		return readErr
	}
	if old.Request.RecoveryID != j.value.Request.RecoveryID || recoverymodel.SameAcceptance(j.value, old) != nil {
		return fault.ErrIdentityMismatch
	}
	if old.Revision == math.MaxUint64 {
		return fault.ErrConflict
	}
	next := recoverymodel.CloneManifest(old)
	if err := edit(&next); err != nil {
		return err
	}
	next.Revision = old.Revision + 1
	if err := recoverymodel.ValidateManifestUpdate(old, next); err != nil {
		return err
	}
	data, err := recoverymodel.EncodeManifest(next)
	if err != nil {
		return err
	}
	if err := j.write("manifest", manifestRelative(next.BackupName), data, true); err != nil {
		var actual recoverymodel.LocalManifest
		readErr := readRecordFrom(j.owner.system.root, relative, &actual, recoverymodel.ManifestMaxBytes)
		if readErr == nil {
			j.value = actual
		}
		return err
	}
	j.value = next
	return nil
}
func (owner *Owner) cleanupUnpublished(name string) error {
	root := owner.system.root
	if err := removeEmptyDirectory(root, filepath.Join(Namespace, name)); err != nil {
		return err
	}
	if err := SyncParents(root, filepath.Join(Namespace, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := removeEmptyDirectory(root, Namespace); err != nil && !errors.Is(err, unix.ENOTEMPTY) {
		return err
	}
	return SyncParents(root, Namespace)
}
func removeEmptyDirectory(root *os.Root, relative string) error {
	parent, err := openParent(root, relative)
	if err != nil {
		return err
	}
	return errors.Join(unix.Unlinkat(int(parent.Fd()), filepath.Base(relative), unix.AT_REMOVEDIR), parent.Close())
}

// ClearCurrent clears only a matching cancelled operation, after its durable audit.
func (j *Journal) ClearCurrent(ctx context.Context) error {
	j.owner.journalMu.Lock()
	defer j.owner.journalMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.owner.CheckRoots(); err != nil {
		return err
	}
	var record recoverymodel.LocalManifest
	relative := manifestRelative(j.value.BackupName)
	readErr := readRecordFrom(j.owner.system.root, relative, &record, recoverymodel.ManifestMaxBytes)
	if readErr != nil {
		return readErr
	}
	if err := recoverymodel.SameAcceptance(j.value, record); err != nil {
		return err
	}
	if record.Phase != recoverymodel.Cancelled {
		return fault.ErrConflict
	}
	if err := SyncParents(j.owner.system.root, manifestRelative(record.BackupName)); err != nil {
		return err
	}
	var pointer recoverymodel.Current
	err := readRecordFrom(j.owner.system.root, filepath.Join(Namespace, "current.json"), &pointer,
		recoverymodel.HeaderMaxBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if pointer.RecoveryID != record.Request.RecoveryID {
		return nil
	}
	if pointer.PlanDigest != record.PlanDigest || pointer.BackupName != record.BackupName {
		return fault.ErrIdentityMismatch
	}
	data, err := recoverymodel.EncodeCanonical(recoverymodel.Current{Schema: recoverymodel.SchemaVersion})
	if err != nil {
		return err
	}
	return j.write("current", filepath.Join(Namespace, "current.json"), data, true)
}
