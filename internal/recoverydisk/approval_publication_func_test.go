//go:build functional

package recoverydisk

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
)

func TestAbandonedApprovalIsNonAuthorizingAcrossOwnerRestart(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"empty namespace", "empty directory", "partial manifest", "complete manifest"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			paths := owner.paths()
			base := filepath.Join(paths.StorageDir, Namespace)
			mustJournal(t, os.Mkdir(base, 0o700))
			if shape != "empty namespace" {
				base = filepath.Join(base, approvalPrefix+strings.Repeat("a", 32))
				mustJournal(t, os.Mkdir(base, 0o700))
				if shape != "empty directory" {
					data := []byte(`{"schema":`)
					if shape == "complete manifest" {
						r := recoverytest.Manifest(paths, "abandoned", owner.clock(), model.Approved)
						var err error
						data, err = model.EncodeManifest(r)
						mustJournal(t, err)
					}
					mustJournal(t, os.WriteFile(filepath.Join(base, "manifest.json"), data, 0o600))
				}
			}
			mustJournal(t, owner.Close())
			restarted, err := Acquire(t.Context(), paths)
			mustJournal(t, err)
			t.Cleanup(func() { mustJournal(t, restarted.Close()) })
			mustJournal(t, restarted.CheckStartup(t.Context(), paths))
			rows, err := List(t.Context(), paths)
			mustJournal(t, err)
			if len(rows) != 0 {
				t.Fatal("unpublished manifest became an operation")
			}
			acceptedJournal(t, restarted, "abandoned", false)
			entries, err := os.ReadDir(filepath.Join(paths.StorageDir, Namespace))
			mustJournal(t, err)
			for _, entry := range entries {
				if approvalTemporary(entry.Name()) {
					t.Fatal("next approval failed to collect abandoned staging")
				}
			}
		})
	}
}

func TestApprovalStagingDoesNotHideForeignContents(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"foreign file", "nested directory", "symlink", "hardlink"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			dir := filepath.Join(owner.system.path, Namespace, approvalPrefix+strings.Repeat("b", 32))
			mustJournal(t, os.MkdirAll(dir, 0o700))
			path := filepath.Join(dir, "manifest.json")
			switch shape {
			case "foreign file":
				mustJournal(t, os.WriteFile(filepath.Join(dir, "foreign"), nil, 0o600))
			case "nested directory":
				mustJournal(t, os.Mkdir(path, 0o700))
			case "symlink":
				mustJournal(t, os.Symlink(filepath.Join(owner.system.path, "_meta.bolt"), path))
			case "hardlink":
				mustJournal(t, os.Link(filepath.Join(owner.system.path, "_meta.bolt"), path))
			}
			if err := CheckStartup(t.Context(), owner.paths()); err == nil {
				t.Fatal("foreign data hidden by staging-shaped name")
			}
			if err := owner.cleanupApprovals(t.Context()); err == nil {
				t.Fatal("foreign data collected as unpublished approval")
			}
		})
	}
}

func TestLeaseCloseJoinsRootAfterLockWasClosed(t *testing.T) {
	t.Parallel()
	lease, err := AcquireRoot(t.Context(), t.TempDir())
	mustJournal(t, err)
	mustJournal(t, lease.lock.Close())
	mustJournal(t, lease.Close())
	if _, err := lease.root.Stat("."); err == nil {
		t.Fatal("root descriptor leaked after an already-closed lock")
	}
	mustJournal(t, lease.Close())
}
