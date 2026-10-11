package metaservice

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadStoredIdentityIsReadOnlyAndOmitsProcessID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "absent.bolt")
	if _, found, err := LoadStoredIdentity(path); err != nil || found {
		t.Fatal(found, err)
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
	id, found, err := LoadStoredIdentity(opts.DBPath)
	if err != nil || !found || id.NodeName != opts.Identity.NodeName || id.ClusterID != opts.Identity.ClusterID || id.MemberID != opts.Identity.MemberID || id.InstanceID != "" {
		t.Fatal(id, found, err)
	}
	after, err := os.ReadFile(opts.DBPath)
	if err != nil || string(before) != string(after) {
		t.Fatal("reader changed database", err)
	}
}
