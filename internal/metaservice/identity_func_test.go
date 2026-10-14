//go:build functional

package metaservice

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadStoredIdentityIsReadOnlyAndOmitsProcessID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "absent.bolt")
	if found, err := LoadStoredIdentity(path); err != nil || found.Found {
		t.Fatal(found.Found, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("identity reader created database", err)
	}
	opts := optionsForTest(t)
	group, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = group.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	id, err := LoadStoredIdentity(opts.DBPath)
	if err != nil || !id.Found || id.Identity.NodeName != opts.Identity.NodeName || id.Identity.ClusterID != opts.Identity.ClusterID || id.Identity.MemberID != opts.Identity.MemberID || id.Identity.InstanceID != "" {
		t.Fatal(id.Identity, id.Found, err)
	}
	after, err := os.ReadFile(opts.DBPath)
	if err != nil || string(before) != string(after) {
		t.Fatal("reader changed database", err)
	}
}
