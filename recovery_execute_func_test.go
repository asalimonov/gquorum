//go:build functional

package gquorum

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryexec"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

func acceptedRecovery(t *testing.T, opts NodeOptions, hook recovery.FilesHook) (*Recovery, recovery.Operation) {
	t.Helper()
	facade, err := NewRecovery(opts, recovery.Options{Files: hook})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "offline execution"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true})
	if err != nil {
		t.Fatal(err)
	}
	return facade, operation
}

func TestRecoveryArchiveKeepsSeparateRootsAndDetectsMissingWALBeforeBackup(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "separate volumes", true: "missing configured WAL"}[missing], func(t *testing.T) {
			t.Parallel()
			walRoot := t.TempDir()
			spec := singleReplicaSpec("work", nil)
			opts := recoveryFixtureStorage(t, &spec, walRoot)
			var backups atomic.Int32
			facade, operation := acceptedRecovery(t, opts, func(_ context.Context, task recovery.FilesTask) error {
				if task.Action == recovery.FilesBackup {
					backups.Add(1)
				}
				return nil
			})
			if missing {
				if err := os.Rename(walRoot, walRoot+"-offline"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = os.RemoveAll(walRoot)
					if err := os.Rename(walRoot+"-offline", walRoot); err != nil {
						t.Error(err)
					}
				})
			}
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if missing {
				if err == nil || status.Phase != recovery.Files || backups.Load() != 0 {
					t.Fatal("missing WAL caused application backup or archiving", status, err)
				}
				return
			}
			if err != nil || status.Phase != recovery.AwaitingStart || backups.Load() != 1 {
				t.Fatal(status, err)
			}
			for _, path := range []string{filepath.Join(opts.StorageDir, recoverydisk.Namespace, operation.BackupName, "backup", "_meta.bolt"), filepath.Join(walRoot, recoverydisk.Namespace, operation.BackupName, "backup", "work")} {
				if _, err := os.Stat(path); err != nil {
					t.Fatal("archive moved to a different configured root", path, err)
				}
			}
		})
	}
}

func TestRecoveryResetArchiveAndBeforeBackupCancellation(t *testing.T) {
	t.Parallel()
	for _, cancelBefore := range []bool{false, true} {
		t.Run(map[bool]string{false: "reset preparation", true: "cancel before backup"}[cancelBefore], func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, true)
			before := sourceFileHashes(t, opts.StorageDir)
			var actions atomic.Int32
			facade, err := NewRecovery(opts, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { actions.Add(1); return nil }})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.ResetForJoin, ClusterID: "recovered-target", Actor: "operator", Comment: "explicit reset"})
			if err != nil {
				t.Fatal(err)
			}
			operation, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true})
			if err != nil {
				t.Fatal(err)
			}
			if cancelBefore {
				status, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "keep old files")
				if err != nil || status.Phase != recovery.Cancelled || actions.Load() != 0 || !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
					t.Fatal(status, err)
				}
				if _, err := facade.Status(t.Context(), ""); !errors.Is(err, fault.ErrHistoryUnavailable) {
					t.Fatal("cancelled pointer retained", err)
				}
				return
			}
			guarded, err := facade.Run(t.Context(), operation.RecoveryID)
			if err == nil || guarded.Phase != recovery.Approved || actions.Load() != 0 {
				t.Fatal("public reset bypassed intermediate guard", guarded, err)
			}
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil || status.Phase != recovery.JoinReady || !status.Complete || status.Identity.MemberID != 0 || actions.Load() != 1 {
				t.Fatal(status, err)
			}
			if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "too late"); !errors.Is(err, fault.ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoveryFileHookCannotMutateLibrarySourceAndStillArchive(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	var backups atomic.Int32
	hook := func(_ context.Context, task recovery.FilesTask) error {
		if task.Action != recovery.FilesBackup {
			t.Error("failure automatically invoked Undo")
			return nil
		}
		backups.Add(1)
		store, err := metastore.Open(filepath.Join(opts.StorageDir, "_meta.bolt"))
		if err != nil {
			return err
		}
		err = store.SaveOverride(1)
		return errors.Join(err, store.Close())
	}
	facade, operation := acceptedRecovery(t, opts, hook)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if !errors.Is(err, fault.ErrConflict) || status.Phase != recovery.Files || backups.Load() != 1 {
		t.Fatal(status, err)
	}
	if _, err := os.Stat(filepath.Join(opts.StorageDir, "_authority.bolt")); err != nil {
		t.Fatal("changed source was archived", err)
	}
}

func TestRecoveryCancelledWaiterRetainsCallbackAndRootOwnership(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	var backups, undos atomic.Int32
	hook := func(_ context.Context, task recovery.FilesTask) error {
		if task.Action == recovery.FilesUndo {
			undos.Add(1)
			return nil
		}
		backups.Add(1)
		close(entered)
		<-release
		return nil
	}
	facade, operation := acceptedRecovery(t, opts, hook)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type result struct {
		status model.Status
		err    error
	}
	done := make(chan result, 1)
	go func() { status, err := executeRecovery(ctx, facade, operation.RecoveryID); done <- result{status, err} }()
	select {
	case <-entered:
	case result := <-done:
		t.Fatal("callback was not reached", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "join callback first"); !errors.Is(err, fault.ErrUnavailable) {
		close(release)
		<-done
		t.Fatal("cancel stole active callback ownership", err)
	}
	select {
	case result := <-done:
		close(release)
		t.Fatal("cancelled waiter detached an owned callback", result)
	default:
	}
	close(release)
	observed := <-done
	if !errors.Is(observed.err, context.Canceled) || observed.status.Phase != recovery.Files {
		t.Fatal(observed)
	}
	status, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "join callback first")
	if err != nil || status.Phase != recovery.Cancelled || backups.Load() != 1 || undos.Load() != 1 {
		t.Fatal(status, err)
	}
}

func TestRecoveryReconcilesMovedBytesWithoutMovedJournalAcknowledgement(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, operation := acceptedRecovery(t, opts, nil)
	owner, err := recoverydisk.Acquire(t.Context(), facade.source.Paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	journal, err := recoverydisk.Open(t.Context(), owner, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.SetPhase(t.Context(), model.Files, nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0); err != nil {
		t.Fatal(err)
	}
	if err := journal.SetPhase(t.Context(), model.Archiving, nil); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "no move happened yet"); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("durable archive boundary was ignored", err)
	}
	owner, err = recoverydisk.Acquire(t.Context(), facade.source.Paths)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(recoverydisk.Namespace, operation.BackupName, "backup")
	if err := owner.EnsureDirectory("system", backup); err != nil {
		t.Fatal(err)
	}
	if err := owner.MoveArtifact(t.Context(), "system", "_authority.bolt", filepath.Join(backup, "_authority.bolt")); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	// The durable manifest still says Moved=false. Resume must verify the
	// exact authority database in backup, then continue without founding A.
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil || status.Phase != recovery.AwaitingStart {
		t.Fatal(status, err)
	}
}

func TestRecoveryArchiveRefusesConflictingBothOrMissingPaths(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"both present", "both absent", "changed backup"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, false)
			facade, operation := acceptedRecovery(t, opts, nil)
			owner, err := recoverydisk.Acquire(t.Context(), facade.source.Paths)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			journal, err := recoverydisk.Open(t.Context(), owner, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.SetPhase(t.Context(), model.Files, nil); err != nil {
				t.Fatal(err)
			}
			if err := journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0); err != nil {
				t.Fatal(err)
			}
			if err := journal.SetPhase(t.Context(), model.Archiving, nil); err != nil {
				t.Fatal(err)
			}
			backup := filepath.Join(opts.StorageDir, recoverydisk.Namespace, operation.BackupName, "backup")
			if err := os.MkdirAll(backup, 0o700); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(opts.StorageDir, "_authority.bolt")
			switch shape {
			case "both present":
				data, err := os.ReadFile(source)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(backup, "_authority.bolt"), data, 0o600); err != nil {
					t.Fatal(err)
				}
			case "both absent":
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
			case "changed backup":
				if err := os.Rename(source, filepath.Join(backup, "_authority.bolt")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(backup, "_authority.bolt"), []byte("not accepted evidence"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err == nil || status.Phase != recovery.Archiving {
				t.Fatal("uncertain paths became completion proof", status, err)
			}
			if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "cannot roll back"); !errors.Is(err, fault.ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}

func executeRecovery(ctx context.Context, facade *Recovery, id string) (model.Status, error) {
	owner, err := recoverydisk.Acquire(ctx, facade.source.Paths)
	if err != nil {
		return model.Status{}, err
	}
	defer owner.Close()
	journal, err := recoverydisk.Open(ctx, owner, id)
	if err != nil {
		return model.Status{}, err
	}
	executor, err := recoveryexec.New(recoveryexec.Config{Owner: owner, Journal: journal, Files: facade.options.Files})
	if err != nil {
		return model.Status{}, err
	}
	return executor.Run(ctx)
}

func TestRecoveryPrivateArchivePreservesOriginalsAndPublicDestructiveGate(t *testing.T) {
	t.Parallel()
	for _, withLoad := range []bool{false, true} {
		t.Run(map[bool]string{false: "metadata only", true: "nested WAL"}[withLoad], func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, withLoad)
			before := sourceFileHashes(t, opts.StorageDir)
			facade, operation := acceptedRecovery(t, opts, nil)
			guarded, err := facade.Run(t.Context(), operation.RecoveryID)
			var refusal *recovery.Refusal
			if !errors.As(err, &refusal) || refusal.Issue.Code != "implementation_pending" || guarded.Phase != recovery.Approved {
				t.Fatal("intermediate public Run archived A", guarded, err)
			}
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil || status.Phase != recovery.AwaitingStart || status.Complete {
				t.Fatal(status, err)
			}
			node, err := NewNode(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Start(t.Context()); !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatal("archived A identity was accepted for B startup", err)
			}
			stopAuthorityNode(t, node)
			for source, digest := range before {
				if filepath.Base(source) == recoverydisk.LockName {
					continue
				}
				root := opts.StorageDir
				name, err := filepath.Rel(root, source)
				if err != nil {
					t.Fatal(err)
				}
				if withLoad {
					if relative, err := filepath.Rel(opts.Loads.Storage.WAL.RootDir, source); err == nil && relative != ".." && len(relative) > 0 && relative[0] != '.' {
						root = opts.Loads.Storage.WAL.RootDir
						name = relative
					}
				}
				backup := filepath.Join(root, recoverydisk.Namespace, operation.BackupName, "backup", name)
				data, err := os.ReadFile(backup)
				if err != nil {
					t.Fatal(source, backup, err)
				}
				if sha256.Sum256(data) != digest {
					t.Fatal("archived bytes changed", source)
				}
			}
			repeated, err := facade.Run(t.Context(), operation.RecoveryID)
			if err != nil || repeated.Phase != recovery.AwaitingStart {
				t.Fatal("post-archive Run did not return history", repeated, err)
			}
			if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "too late"); !errors.Is(err, fault.ErrConflict) {
				t.Fatal("archived recovery cancelled", err)
			}
		})
	}
}

func TestRecoveryCancelArchivesNoLibraryDataAndResumesOnlyFailedUndo(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	app := filepath.Join(opts.StorageDir, "application.db")
	if err := os.WriteFile(app, []byte("old database"), 0o600); err != nil {
		t.Fatal(err)
	}
	var backups, undos atomic.Int32
	backupFailure := errors.New("backup returned after moving application files")
	undoFailure := errors.New("first Undo failed")
	hook := func(_ context.Context, task recovery.FilesTask) error {
		backup := filepath.Join(task.BackupDir, "old.db")
		switch task.Action {
		case recovery.FilesBackup:
			backups.Add(1)
			if err := os.Rename(app, backup); err != nil {
				return err
			}
			return backupFailure
		case recovery.FilesUndo:
			if undos.Add(1) == 1 {
				return undoFailure
			}
			return os.Rename(backup, app)
		default:
			return fault.ErrInvalid
		}
	}
	facade, operation := acceptedRecovery(t, opts, hook)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if !errors.Is(err, backupFailure) || status.Phase != recovery.Files {
		t.Fatal(status, err)
	}
	status, err = facade.Cancel(t.Context(), operation.RecoveryID, "operator", "restore application files")
	if !errors.Is(err, undoFailure) || status.Phase != recovery.Cancelling {
		t.Fatal(status, err)
	}
	status, err = facade.Run(t.Context(), operation.RecoveryID)
	if err != nil || status.Phase != recovery.Cancelled || status.Complete || backups.Load() != 1 || undos.Load() != 2 {
		t.Fatal("Run(Cancelling) resumed backup or failed Undo", status, backups.Load(), undos.Load(), err)
	}
	data, err := os.ReadFile(app)
	if err != nil || string(data) != "old database" {
		t.Fatal("application files were not restored", err)
	}
	if _, err := os.Stat(filepath.Join(opts.StorageDir, "_meta.bolt")); err != nil {
		t.Fatal("cancellation moved metadata", err)
	}
	if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "different", "restore application files"); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("cancellation audit changed", err)
	}
	if _, err := facade.Cancel(t.Context(), operation.RecoveryID, "operator", "restore application files"); err != nil {
		t.Fatal(err)
	}
}
