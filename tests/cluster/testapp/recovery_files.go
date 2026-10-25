package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
)

// Only these application-owned paths move. The live job, event journal and
// library-owned files are deliberately outside this hook's inventory.
var recoveryApplicationPaths = []string{"established.json", "projection"}

type recoveryFileEntry struct {
	Name    string `json:"name"`
	Present bool   `json:"present"`
}
type recoveryFileManifest struct {
	RecoveryID string              `json:"recovery_id"`
	Digest     string              `json:"digest"`
	Entries    []recoveryFileEntry `json:"entries"`
}

func (a *application) recoveryFiles(ctx context.Context, task recovery.FilesTask) error {
	if err := a.recoveryControlFiles(ctx, task.Action); err != nil {
		return err
	}
	if err := os.MkdirAll(task.BackupDir, 0o700); err != nil {
		return err
	}
	manifest, err := a.recoveryFilePlan(ctx, task)
	if err != nil {
		return err
	}
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.Present {
			continue
		}
		live := filepath.Join(a.cfg.DataDir, entry.Name)
		archived := filepath.Join(task.BackupDir, entry.Name)
		from, to := live, archived
		if task.Action == recovery.FilesUndo {
			from, to = archived, live
		}
		if err := moveRecoveryApplicationFile(from, to); err != nil {
			return err
		}
	}
	if task.Action == recovery.FilesBackup {
		return a.recoveryAfterBackup(ctx)
	}
	return nil
}
func (a *application) recoveryFilePlan(ctx context.Context, task recovery.FilesTask) (recoveryFileManifest, error) {
	filename := filepath.Join(task.BackupDir, "files.json")
	raw, err := os.ReadFile(filename)
	if err == nil {
		var manifest recoveryFileManifest
		if err := decodeJSON(raw, &manifest); err != nil {
			return manifest, err
		}
		if manifest.RecoveryID != task.RecoveryID || manifest.Digest != task.PlanDigest ||
			len(manifest.Entries) != len(recoveryApplicationPaths) {
			return manifest, fault.ErrIdentityMismatch
		}
		for i, entry := range manifest.Entries {
			if entry.Name != recoveryApplicationPaths[i] {
				return manifest, fault.ErrIdentityMismatch
			}
		}
		return manifest, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return recoveryFileManifest{}, err
	}
	// A crash before Backup created its inventory is also an idempotent Undo.
	manifest := recoveryFileManifest{RecoveryID: task.RecoveryID, Digest: task.PlanDigest}
	if task.Action == recovery.FilesUndo {
		return manifest, nil
	}
	for _, name := range recoveryApplicationPaths {
		info, statErr := os.Lstat(filepath.Join(a.cfg.DataDir, name))
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return manifest, statErr
		}
		if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return manifest, fault.ErrInvalid
		}
		manifest.Entries = append(manifest.Entries, recoveryFileEntry{Name: name, Present: statErr == nil})
	}
	raw, err = json.Marshal(manifest)
	if err != nil {
		return manifest, err
	}
	return manifest, writeProjection(ctx, filename, raw)
}
func moveRecoveryApplicationFile(from, to string) error {
	_, sourceErr := os.Lstat(from)
	_, targetErr := os.Lstat(to)
	if !(errors.Is(sourceErr, os.ErrNotExist) && targetErr == nil) {
		if sourceErr != nil {
			return sourceErr
		}
		if !errors.Is(targetErr, os.ErrNotExist) {
			return fault.ErrConflict
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	for _, dir := range []string{filepath.Dir(from), filepath.Dir(to)} {
		handle, err := os.Open(dir)
		if err != nil {
			return err
		}
		if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
			return err
		}
	}
	return nil
}
