//go:build functional

package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestArtifactDigestIsIndependentOfTopLevelMoveAndChecksWholeContent(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	dir := filepath.Join(owner.wal.path, "artifact")
	mustJournal(t, os.MkdirAll(filepath.Join(dir, "nested"), 0o700))
	mustJournal(t, os.WriteFile(filepath.Join(dir, "nested", "data"), []byte("original"), 0o600))
	first, err := owner.ArtifactDigest(t.Context(), "wal", "artifact", 4096)
	mustJournal(t, err)
	mustJournal(t, os.Rename(dir, filepath.Join(owner.wal.path, "moved")))
	again, err := owner.ArtifactDigest(t.Context(), "wal", "moved", 4096)
	mustJournal(t, err)
	if first != again {
		t.Fatal("move changed opaque content fingerprint")
	}
	mustJournal(t, os.WriteFile(filepath.Join(owner.wal.path, "moved", "nested", "data"), []byte("changed"), 0o600))
	changed, err := owner.ArtifactDigest(t.Context(), "wal", "moved", 4096)
	mustJournal(t, err)
	if changed == first {
		t.Fatal("changed opaque content was not fingerprinted")
	}
	if _, err := owner.ArtifactDigest(t.Context(), "wal", "moved", 1); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("name budget ignored", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := owner.ArtifactDigest(ctx, "wal", "moved", 4096); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}

func TestArtifactValidationRefusesLinksAndDoesNotReadActivePreallocation(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"symlink", "hardlink", "sparse active file"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			dir := filepath.Join(owner.wal.path, "artifact")
			mustJournal(t, os.Mkdir(dir, 0o700))
			path := filepath.Join(dir, "data")
			switch shape {
			case "symlink":
				mustJournal(t, os.Symlink(filepath.Join(owner.system.path, "_meta.bolt"), path))
			case "hardlink":
				mustJournal(t, os.Link(filepath.Join(owner.system.path, "_meta.bolt"), path))
			case "sparse active file":
				file, err := os.Create(path)
				mustJournal(t, err)
				mustJournal(t, file.Truncate(256<<20))
				mustJournal(t, file.Close())
			}
			err := owner.ValidateArtifact(t.Context(), "wal", "artifact", 4096)
			if shape == "sparse active file" {
				mustJournal(t, err)
			} else if !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("alias accepted for archival", err)
			}
		})
	}
}
