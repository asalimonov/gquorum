package exec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/internal/walbackend"
)

const archiveWAL = "wal"

func archiveRelative(backupName, name string) string {
	return filepath.Join(recoverydisk.Namespace, backupName, "backup", name)
}

func (e *Executor) requireOriginalLocations(ctx context.Context) error {
	manifest := e.config.Journal.View()
	if err := e.validateInventories(ctx, manifest); err != nil {
		return err
	}
	for _, entry := range manifest.Archive {
		if entry.Moved || entry.Restored {
			return fault.ErrConflict
		}
		source, err := e.config.Owner.ArtifactExists(entry.Root, entry.Name)
		if err != nil {
			return err
		}
		backup, err := e.config.Owner.ArtifactExists(entry.Root, archiveRelative(manifest.BackupName, entry.Name))
		if err != nil {
			return err
		}
		if !source || backup {
			return fault.New(fault.Conflict, "original library paths show an archive move or conflict", nil)
		}
	}
	return nil
}

func (e *Executor) prepareArchive(ctx context.Context) error {
	manifest := e.config.Journal.View()
	for _, entry := range manifest.Archive {
		if err := ctx.Err(); err != nil {
			return err
		}
		parent := filepath.Dir(archiveRelative(manifest.BackupName, entry.Name))
		if err := e.config.Owner.EnsureDirectory(entry.Root, parent); err != nil {
			return err
		}
	}
	return e.requireOriginalLocations(ctx)
}

func (e *Executor) archive(ctx context.Context) error {
	manifest := e.config.Journal.View()
	if manifest.Phase != model.Archiving || manifest.SuccessorStartedAt != nil {
		return fault.ErrConflict
	}
	if err := e.validateInventories(ctx, manifest); err != nil {
		return err
	}
	entries := slices.Clone(manifest.Archive)
	slices.SortFunc(entries, func(a, b model.ArchiveEntry) int {
		return archiveOrder(a) - archiveOrder(b)
	})
	for _, entry := range entries {
		if _, err := e.ReconcileArchiveEntry(ctx, entry); err != nil {
			return err
		}
		if err := e.config.Journal.MarkArchive(ctx, entry.Root, entry.Name); err != nil {
			return err
		}
	}
	return nil
}

func archiveOrder(entry model.ArchiveEntry) int {
	const (
		authorityOrder = iota
		metadataOrder
		readmissionOrder
		walOrder
	)
	if entry.Root == archiveWAL {
		return walOrder
	}
	if entry.Name == "_authority.bolt" {
		return authorityOrder
	}
	if entry.Name == "_meta.bolt" {
		return metadataOrder
	}
	return readmissionOrder
}

// ReconcileArchiveEntry verifies the exact accepted source or backup before a
// no-clobber move or acknowledgement. Absence alone never proves completion.
func (e *Executor) ReconcileArchiveEntry(ctx context.Context, entry model.ArchiveEntry) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	manifest := e.config.Journal.View()
	if manifest.Phase != model.Archiving || manifest.SuccessorStartedAt != nil {
		return false, fault.ErrConflict
	}
	if !slices.Contains(manifest.Archive, entry) {
		return false, fault.ErrIdentityMismatch
	}
	backup := archiveRelative(manifest.BackupName, entry.Name)
	sourcePresent, err := e.config.Owner.ArtifactExists(entry.Root, entry.Name)
	if err != nil {
		return false, err
	}
	backupPresent, err := e.config.Owner.ArtifactExists(entry.Root, backup)
	if err != nil {
		return false, err
	}
	if sourcePresent == backupPresent || entry.Moved && sourcePresent {
		return false, fault.New(fault.Conflict,
			"archive paths are both present, both absent or contradict durable movement", nil)
	}
	location := backup
	if sourcePresent {
		location = entry.Name
	}
	if err := e.verifyArtifact(ctx, entry, location, manifest.Resources); err != nil {
		return false, err
	}
	if err := e.config.Owner.EnsureDirectory(entry.Root, filepath.Dir(backup)); err != nil {
		return false, err
	}
	if sourcePresent {
		if err := e.config.Owner.MoveArtifact(ctx, entry.Root, entry.Name, backup); err != nil {
			return false, err
		}
	} else if err := e.config.Owner.SyncArtifactParents(entry.Root, entry.Name, backup); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Executor) verifyArtifact(ctx context.Context, entry model.ArchiveEntry, relative string,
	resources model.Resources) error {
	if err := e.config.Owner.ValidateArtifact(ctx, entry.Root, relative, resources.MaxMetadataBytes); err != nil {
		return err
	}
	root, err := e.config.Owner.RootPath(entry.Root)
	if err != nil {
		return err
	}
	var digest string
	if entry.Classification == "source" && entry.Root == "system" {
		db, err := recoverydisk.OpenReadOnlyDB(ctx, filepath.Join(root, relative), resources.MaxMetadataBytes)
		if err != nil {
			return err
		}
		digest, err = recoverydisk.DigestDB(ctx, db, resources.MaxMetadataBytes)
		if err := errors.Join(err, db.Close()); err != nil {
			return err
		}
	} else if entry.Classification == "source" && entry.Root == archiveWAL {
		evidence, err := walbackend.ReadRecovery(ctx, filepath.Dir(filepath.Join(root, relative)), entry.Name,
			model.ReadBounds{MaxRecordBytes: resources.MaxRecordBytes, MaxGroupBytes: resources.MaxGroupBytes})
		if err != nil {
			return err
		}
		digest = evidence.Digest
	} else {
		digest, err = e.config.Owner.ArtifactDigest(ctx, entry.Root, relative, resources.MaxMetadataBytes)
		if err != nil {
			return err
		}
	}
	if digest != entry.EvidenceDigest {
		return fault.New(fault.IdentityMismatch, "archive artifact differs from accepted evidence", nil)
	}
	return nil
}

func (e *Executor) validateInventories(ctx context.Context, manifest model.LocalManifest) error {
	if err := e.config.Owner.CheckRecoveryNamespaces(ctx); err != nil {
		return err
	}
	for _, kind := range []string{"system", archiveWAL} {
		if kind == archiveWAL && manifest.Paths.LoadWALDir == "" {
			continue
		}
		allowed := make(map[string]bool)
		for _, entry := range manifest.Archive {
			if entry.Root == kind {
				allowed[entry.Name] = true
			}
		}
		if err := e.checkInventory(ctx, kind, allowed); err != nil {
			return err
		}
		backup := filepath.Join(recoverydisk.Namespace, manifest.BackupName, "backup")
		if err := e.checkBackupInventory(ctx, kind, backup, allowed); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) checkInventory(ctx context.Context, kind string, allowed map[string]bool) error {
	return e.config.Owner.WalkEntries(ctx, kind, ".", func(entry os.DirEntry) error {
		name := entry.Name()
		if kind == "system" && name != "_meta.bolt" && name != "_authority.bolt" && !strings.HasPrefix(name,
			"_meta.bolt.retired-") {
			return nil
		}
		if name == recoverydisk.LockName || name == recoverydisk.Namespace || allowed[name] {
			return nil
		}
		return fault.New(fault.Unreadable, "unrecognized library evidence appeared during recovery", nil)
	})
}

func (e *Executor) checkBackupInventory(ctx context.Context, kind, path string, allowed map[string]bool) error {
	err := e.config.Owner.WalkEntries(ctx, kind, path, func(entry os.DirEntry) error {
		if !allowed[entry.Name()] {
			return fault.New(fault.Unreadable, "backup contains unrecognized library evidence", nil)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
