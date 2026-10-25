package disk

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"golang.org/x/sys/unix"
)

func newManifest(p model.AcceptedPlan, a model.Acceptance, at time.Time,
	name string) model.LocalManifest {
	r := model.LocalManifest{Schema: model.SchemaVersion, Revision: 1, Request: p.Request,
		Acceptance: a, Paths: p.Paths,
		Resources: p.Resources, Source: p.Source, Identity: p.Identity, Fingerprint: p.Fingerprint,
		PlanDigest: a.PlanDigest,
		Rules:      p.Rules, Approved: p.Approved, Floors: p.Floors, Retired: p.Retired, Sources: p.Sources,
		Archive: p.Archive,
		Files: model.FilesProgress{Required: p.FilesRequired,
			Backup: model.HookProgress{State: "pending"},
			Undo:   model.HookProgress{State: "pending"}},
		Phase: model.Approved, AcceptedAt: at, BackupName: name}
	if p.Request.Mode == model.Rebuild {
		for _, group := range p.Approved.LoadGroups() {
			r.Seeds = append(r.Seeds,
				model.SeedProgress{GroupID: group.ID, State: "pending"})
		}
	}
	model.StartHistory(&r)
	return model.CloneManifest(r)
}

// Update atomically applies a caller's exact revision as a compare-and-swap.
// The supplied Revision is the observed predecessor; the journal increments it.
func (j *Journal) Update(ctx context.Context, next model.LocalManifest) error {
	expected := next.Revision
	return j.change(ctx, func(current *model.LocalManifest) error {
		if current.Revision != expected {
			return fault.ErrConflict
		}
		*current = model.CloneManifest(next)
		return nil
	})
}
func (j *Journal) change(ctx context.Context, edit func(*model.LocalManifest) error) error {
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
	readErr := readRecordFrom(j.owner.system.root, relative, &old, model.ManifestMaxBytes)
	if readErr != nil {
		return readErr
	}
	if old.Request.RecoveryID != j.value.Request.RecoveryID || model.SameAcceptance(j.value, old) != nil {
		return fault.ErrIdentityMismatch
	}
	if old.Revision == math.MaxUint64 {
		return fault.ErrConflict
	}
	next := model.CloneManifest(old)
	if err := edit(&next); err != nil {
		return err
	}
	next.Revision = old.Revision + 1
	model.AdvanceHistory(old, &next, j.owner.clock())
	if err := model.ValidateManifestUpdate(old, next); err != nil {
		return err
	}
	data, err := model.EncodeManifest(next)
	if err != nil {
		return err
	}
	if err := j.write("manifest", manifestRelative(next.BackupName), data, true); err != nil {
		var actual model.LocalManifest
		readErr := readRecordFrom(j.owner.system.root, relative, &actual, model.ManifestMaxBytes)
		if readErr == nil {
			j.value = actual
		}
		return err
	}
	j.value = next
	return nil
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
	var record model.LocalManifest
	relative := manifestRelative(j.value.BackupName)
	readErr := readRecordFrom(j.owner.system.root, relative, &record, model.ManifestMaxBytes)
	if readErr != nil {
		return readErr
	}
	if err := model.SameAcceptance(j.value, record); err != nil {
		return err
	}
	if record.Phase != model.Cancelled {
		return fault.ErrConflict
	}
	if err := SyncParents(j.owner.system.root, manifestRelative(record.BackupName)); err != nil {
		return err
	}
	var pointer model.Current
	err := readRecordFrom(j.owner.system.root, filepath.Join(Namespace, "current.json"), &pointer,
		model.HeaderMaxBytes)
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
	data, err := model.EncodeCanonical(model.Current{Schema: model.SchemaVersion})
	if err != nil {
		return err
	}
	return j.write("current", filepath.Join(Namespace, "current.json"), data, true)
}
