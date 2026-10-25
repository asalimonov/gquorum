//go:build functional

package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"

	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
)

func TestOwnerExclusionAndWALTransfer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys := t.TempDir()
	paths := model.Paths{StorageDir: sys, LoadWALDir: filepath.Join(sys, "load")}
	owner, err := Acquire(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if other, err := Acquire(ctx, paths); !errors.Is(err, fault.ErrUnavailable) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("source ownership overlapped", err)
	}
	lease, err := owner.TakeWALLease()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := AcquireRoot(ctx, paths.LoadWALDir); !errors.Is(err, fault.ErrUnavailable) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("transfer released WAL lock", err)
	}
	if _, err := Acquire(ctx, paths); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("partial ownership did not refuse", err)
	}
	system, err := AcquireRoot(ctx, sys)
	if err != nil {
		t.Fatal("failed acquisition leaked the system lock", err)
	}
	if err := system.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRejectsRootAndLockReplacement(t *testing.T) {
	t.Parallel()
	for _, replaceLock := range []bool{false, true} {
		t.Run(map[bool]string{false: "root", true: "lock"}[replaceLock], func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "root")
			lease, err := AcquireRoot(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := lease.Close(); err != nil {
					t.Error(err)
				}
			})
			if replaceLock {
				if err := os.Rename(filepath.Join(path, LockName), filepath.Join(path, "old-lock")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, LockName), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := lease.CheckRoot(); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("changed root/lock accepted", err)
			}
		})
	}
}

func TestOwnerRejectsAliasedAndNestedSystemRoots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	if lease, err := AcquireRoot(ctx, alias); err == nil {
		_ = lease.Close()
		t.Fatal("symlink root accepted")
	}
	if owner, err := Acquire(ctx, model.Paths{StorageDir: filepath.Join(real, "system"), LoadWALDir: real}); !errors.Is(err, fault.ErrInvalid) {
		if owner != nil {
			_ = owner.Close()
		}
		t.Fatal("system inside WAL accepted", err)
	}
	if owner, err := Acquire(ctx, model.Paths{StorageDir: real, LoadWALDir: real}); err == nil {
		_ = owner.Close()
		t.Fatal("same roots accepted")
	}
	if lease, err := AcquireRoot(ctx, filepath.Join(real, Namespace, "backup")); err == nil {
		_ = lease.Close()
		t.Fatal("private recovery root accepted")
	}
}

func TestStartupGuardValidatesHistoryAndPendingRecords(t *testing.T) {
	t.Parallel()
	for _, phase := range []model.Phase{model.Approved, model.Archiving, model.Complete} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			at := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
			name, err := model.BackupName(at, "incident")
			if err != nil {
				t.Fatal(err)
			}
			record := recoverytest.Manifest(model.Paths{StorageDir: root}, "incident", at, phase)
			dir := filepath.Join(root, Namespace, name)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			data, err := model.EncodeCanonical(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			paths := model.Paths{StorageDir: root}
			err = CheckStartup(context.Background(), paths)
			if phase != model.Complete {
				if !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal("pending recovery allowed startup", err)
				}
				return
			}
			if !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("archive authorized fresh founding", err)
			}
			for _, file := range []string{"_meta.bolt", "_authority.bolt"} {
				if err := os.WriteFile(filepath.Join(root, file), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := CheckStartup(context.Background(), paths); err != nil {
				t.Fatal("valid terminal namespace rejected", err)
			}
			if err := os.WriteFile(filepath.Join(root, Namespace, "foreign"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := CheckStartup(context.Background(), paths); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("unknown control entry ignored", err)
			}
		})
	}
}

func TestOwnerRetainsReturnedWALUntilTeardown(t *testing.T) {
	t.Parallel()
	paths := model.Paths{StorageDir: t.TempDir(), LoadWALDir: t.TempDir()}
	owner, err := Acquire(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := owner.TakeWALLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.ReturnWALLease(lease); err != nil {
		t.Fatal(err)
	}
	if other, err := AcquireRoot(t.Context(), paths.LoadWALDir); !errors.Is(err, fault.ErrUnavailable) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("failed adoption relinquished ownership", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRejectsCanonicalPrivateRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	private := filepath.Join(root, Namespace)
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	// The final component is not a symlink; checking only its spelling misses this.
	if lease, err := AcquireRoot(t.Context(), filepath.Join(alias, "backup")); !errors.Is(err, fault.ErrInvalid) {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatal("canonical archive root accepted", err)
	}
	if _, err := os.Stat(filepath.Join(private, "backup")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusal created data inside archive", err)
	}
}

func TestValidatedHistoryExcludesOnlyOwnedWALArchive(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := model.Paths{StorageDir: root, LoadWALDir: filepath.Join(root, "loads")}
	owner, err := Acquire(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	at := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	name, err := model.BackupName(at, "history")
	if err != nil {
		t.Fatal(err)
	}
	r := recoverytest.Manifest(paths, "history", at, model.Complete)
	dir := filepath.Join(root, Namespace, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := model.EncodeCanonical(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"_meta.bolt", "_authority.bolt"} {
		if err := os.WriteFile(filepath.Join(root, file), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	walArchive := filepath.Join(paths.LoadWALDir, Namespace, name)
	if err := os.MkdirAll(walArchive, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := owner.CheckStartup(t.Context(), paths); err != nil {
		t.Fatal(err)
	}
	lease, err := owner.TakeWALLease()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Error(err)
		}
	})
	if live, err := lease.HasLiveEvidence(t.Context()); err != nil || live {
		t.Fatal("owned history treated as live WAL", live, err)
	}
	if err := os.Mkdir(filepath.Join(paths.LoadWALDir, Namespace, "forged"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.HasLiveEvidence(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("unknown archive silently ignored", err)
	}
}
