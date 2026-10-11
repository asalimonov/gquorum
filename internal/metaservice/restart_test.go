//go:build !functional

package metaservice

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestPublicMetaConstructorInvalidDoesNotCreateStorage(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "untouched", "meta.bolt")
	if _, err := New(Options{DBPath: path}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatal("invalid constructor touched local storage", err)
	}
}
