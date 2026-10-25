//go:build functional

package authoritystore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestOfflineAuthorityReaderNeverInitializesOrMutatesEvidence(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "authority.bolt")
	id := topology.Identity{NodeName: "n1", ClusterID: "cluster", MemberID: 1, InstanceID: "source"}
	store, err := Open(path, id, FreshEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkMetadataAcquired(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(t.Context(), path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	pinned, err := reader.Identity()
	if err != nil || pinned.NodeName != "n1" || pinned.MemberID != 1 || pinned.InstanceID != "" {
		t.Fatal(pinned, err)
	}
	evidence, err := reader.Evidence(t.Context())
	if err != nil || !evidence.MetadataAcquired || evidence.Identity != pinned {
		t.Fatal(evidence, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := reader.Evidence(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(t.Context(), path, 1); err == nil {
		t.Fatal("authority record budget ignored")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("reader rewrote source authority", err)
	}
	absent := filepath.Join(t.TempDir(), "absent.bolt")
	if _, err := OpenReadOnly(t.Context(), absent, 1<<20); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := os.Stat(absent); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reader initialized absent journal")
	}
}
