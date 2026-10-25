//go:build functional

package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestArchiveMoveFaultsPreserveEvidenceAndRequireJoinedSync(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"archive.rename", "archive.parent_sync", "archive.disk_full"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			parent := filepath.Join(Namespace, "owned", "backup")
			mustJournal(t, owner.EnsureDirectory("system", parent))
			injected := errors.New("interrupted archive filesystem operation")
			if point == "archive.disk_full" {
				injected = unix.ENOSPC
			}
			owner.boundary = func(at string) error {
				if at == point || point == "archive.disk_full" && at == "archive.rename" {
					return injected
				}
				return nil
			}
			destination := filepath.Join(parent, "_meta.bolt")
			if err := owner.MoveArtifact(t.Context(), "system", "_meta.bolt", destination); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			owner.boundary = nil
			source, err := owner.ArtifactExists("system", "_meta.bolt")
			mustJournal(t, err)
			backup, err := owner.ArtifactExists("system", destination)
			mustJournal(t, err)
			if point == "archive.rename" || point == "archive.disk_full" {
				if !source || backup {
					t.Fatal("before-rename failure changed paths")
				}
				mustJournal(t, owner.MoveArtifact(t.Context(), "system", "_meta.bolt", destination))
			} else {
				if source || !backup {
					t.Fatal("post-rename failure lost durable evidence")
				}
				mustJournal(t, owner.SyncArtifactParents("system", "_meta.bolt", destination))
			}
			data, err := os.ReadFile(filepath.Join(owner.system.path, destination))
			mustJournal(t, err)
			if string(data) != "_meta.bolt" {
				t.Fatal("source bytes changed")
			}
		})
	}
}

func TestArchiveMoveNeverReplacesExistingDestination(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	parent := filepath.Join(Namespace, "owned", "backup")
	mustJournal(t, owner.EnsureDirectory("system", parent))
	destination := filepath.Join(parent, "_meta.bolt")
	mustJournal(t, os.WriteFile(filepath.Join(owner.system.path, destination), []byte("foreign"), 0o600))
	if err := owner.MoveArtifact(t.Context(), "system", "_meta.bolt", destination); !errors.Is(err, os.ErrExist) {
		t.Fatal("foreign destination overwritten", err)
	}
	for _, input := range []struct{ name, data string }{{"_meta.bolt", "_meta.bolt"}, {destination, "foreign"}} {
		data, err := os.ReadFile(filepath.Join(owner.system.path, input.name))
		mustJournal(t, err)
		if string(data) != input.data {
			t.Fatal(input.name, "changed")
		}
	}
}
