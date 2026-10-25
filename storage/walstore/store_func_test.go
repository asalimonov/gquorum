//go:build functional

package walstore_test

import (
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	"github.com/asalimonov/gquorum/storage/walstore"
	"path/filepath"
	"testing"
)

func TestPublicWALConformance(t *testing.T) {
	t.Parallel()
	storagetest.Run(t, func(t *testing.T, node string) storage.Store {
		prewrite := false
		s, err := walstore.Open(walstore.Options{RootDir: filepath.Join(t.TempDir(), node), NodeName: node, ClusterID: "test-cluster", Prewrite: &prewrite, SegmentBytes: 32 << 10})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	})
}
