//go:build functional

package metaengine

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"

	"github.com/asalimonov/gquorum/internal/metastore"
	bolt "go.etcd.io/bbolt"
)

func physicalRows(t *testing.T, path string) map[string]string {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := map[string]string{}
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(key, value []byte) error {
				rows[string(name)+"/"+string(key)] = string(value)
				return nil
			})
		})
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestBaselinePhysicalMetaStoreRejectedWithoutRewritingRecords(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "api-baseline", "meta.bolt")
	before := physicalRows(t, fixture)
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "meta.bolt")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := metastore.LoadIdentity(path); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("legacy pin acquired", err)
	}
	if db, err := metastore.Open(path); !errors.Is(err, fault.ErrUnsupported) {
		if db != nil {
			db.Close()
		}
		t.Fatal("legacy database acquired", err)
	}
	if after := physicalRows(t, path); !maps.Equal(before, after) {
		t.Fatal("recovery rewrote persisted records", before, after)
	}
}
