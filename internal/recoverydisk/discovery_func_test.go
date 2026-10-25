//go:build functional

package recoverydisk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
)

func TestDiscoveryKeepsValidRowsAndExcludesDuplicateOrRenamedIDs(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	cancelled, _, _ := acceptedJournal(t, owner, "history", false)
	mustJournal(t, cancelled.BeginCancellation(t.Context(), model.ActionAudit{Actor: "operator", Reason: "cancel", At: owner.clock()}))
	mustJournal(t, cancelled.CompleteCancellation(t.Context(), owner.clock()))
	mustJournal(t, cancelled.ClearCurrent(t.Context()))
	next, _, _ := acceptedJournal(t, owner, "pending", false)
	unknown := filepath.Join(owner.system.path, Namespace, "foreign")
	mustJournal(t, os.WriteFile(unknown, []byte("unowned"), 0o600))
	rows, err := List(t.Context(), owner.paths())
	if err == nil || len(rows) != 2 {
		t.Fatal("unowned entry hid valid history", rows, err)
	}
	if err := ValidateNamespace(t.Context(), owner.system.path); err == nil {
		t.Fatal("namespace validation adopted unowned data")
	}
	if _, err := Open(t.Context(), owner, "pending"); err == nil {
		t.Fatal("mutation ignored invalid namespace")
	}
	mustJournal(t, os.Remove(unknown))
	clone := cancelled.View()
	clone.AcceptedAt = clone.AcceptedAt.Add(time.Hour)
	clone.BackupName, err = model.BackupName(clone.AcceptedAt, clone.Request.RecoveryID)
	mustJournal(t, err)
	clone.Cancellation.At = clone.AcceptedAt
	clone.CancelledAt = &clone.AcceptedAt
	path := filepath.Join(owner.system.path, Namespace, clone.BackupName)
	mustJournal(t, os.Mkdir(path, 0o700))
	data, err := model.EncodeManifest(clone)
	mustJournal(t, err)
	mustJournal(t, os.WriteFile(filepath.Join(path, "manifest.json"), data, 0o600))
	rows, err = List(t.Context(), owner.paths())
	if !errors.Is(err, fault.ErrConflict) || len(rows) != 1 || rows[0].Request.RecoveryID != next.View().Request.RecoveryID {
		t.Fatal("duplicate ID adopted or unrelated row hidden", rows, err)
	}
	mustJournal(t, os.Remove(filepath.Join(path, "manifest.json")))
	mustJournal(t, os.Remove(path))
	original := filepath.Join(owner.system.path, Namespace, cancelled.View().BackupName)
	mustJournal(t, os.Rename(original, original+"-renamed"))
	rows, err = List(t.Context(), owner.paths())
	if err == nil || len(rows) != 1 || rows[0].Request.RecoveryID != "pending" {
		t.Fatal("renamed manifest adopted", rows, err)
	}
}

func TestOrphanControlRecordsAndBoundedReadRefuseWithoutAdoption(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"orphan temporary", "oversized", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			record := recoverytest.Manifest(owner.paths(), "orphan", owner.clock(), model.Approved)
			dir := filepath.Join(owner.system.path, Namespace, record.BackupName)
			mustJournal(t, os.MkdirAll(dir, 0o700))
			data, err := model.EncodeManifest(record)
			mustJournal(t, err)
			path := filepath.Join(dir, "manifest.json")
			switch kind {
			case "orphan temporary":
				mustJournal(t, os.WriteFile(filepath.Join(dir, "@tmp-owned-looking"), data, 0o600))
			case "oversized":
				f, err := os.Create(path)
				mustJournal(t, err)
				mustJournal(t, f.Truncate(int64(model.ManifestMaxBytes)+1))
				mustJournal(t, f.Close())
			case "symlink":
				foreign := filepath.Join(t.TempDir(), "foreign.json")
				mustJournal(t, os.WriteFile(foreign, data, 0o600))
				mustJournal(t, os.Symlink(foreign, path))
			}
			rows, err := List(t.Context(), owner.paths())
			if err == nil || len(rows) != 0 {
				t.Fatal("unowned/oversized control data adopted", rows, err)
			}
			if _, err := Open(t.Context(), owner, "orphan"); err == nil {
				t.Fatal("partial approval resumed from names alone")
			}
			if source, err := os.ReadFile(filepath.Join(owner.system.path, "_meta.bolt")); err != nil || string(source) != "_meta.bolt" {
				t.Fatal("refusal changed source", err)
			}
		})
	}
}

func TestAtomicPublicationNoClobberAndNoFollow(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	root := owner.system.root
	mustJournal(t, AtomicWrite(root, "record.json", []byte("original"), false))
	if err := AtomicWrite(root, "record.json", []byte("foreign"), false); !errors.Is(err, os.ErrExist) {
		t.Fatal("no-clobber publication overwrote record", err)
	}
	mustJournal(t, AtomicWrite(root, "record.json", []byte("successor"), true))
	dir := filepath.Join(owner.system.path, "foreign")
	mustJournal(t, os.Mkdir(dir, 0o700))
	mustJournal(t, os.Symlink(dir, filepath.Join(owner.system.path, "alias")))
	if err := AtomicWrite(root, "alias/record.json", nil, false); err == nil {
		t.Fatal("symlink parent followed")
	}
	mustJournal(t, os.Link(filepath.Join(owner.system.path, "record.json"), filepath.Join(owner.system.path, "hardlink")))
	if err := AtomicWrite(root, "record.json", nil, true); err == nil {
		t.Fatal("writable hard-link alias replaced")
	}
	data, err := os.ReadFile(filepath.Join(owner.system.path, "hardlink"))
	mustJournal(t, err)
	if string(data) != "successor" {
		t.Fatal("foreign alias changed")
	}
	info, err := os.Stat(filepath.Join(owner.system.path, "record.json"))
	mustJournal(t, err)
	if info.Mode().Perm() != 0o600 {
		t.Fatal("private permissions broadened", info.Mode())
	}
}
