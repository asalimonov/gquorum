//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestValidateStorageClass(t *testing.T) {
	t.Parallel()
	for class, valid := range map[StorageClass]bool{StorageDefault: true, StorageMemory: true, "disk": false, "Memory": false} {
		if err := ValidateStorageClass(class); (err == nil) != valid || err != nil && !errors.Is(err, fault.ErrInvalid) {
			t.Errorf("%q: %v", class, err)
		}
	}
}

func TestDocumentValidatesGroupStorage(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Group("load").Storage = StorageMemory
	if err := d.Validate(testRules()); err != nil {
		t.Fatal("memory load group refused", err)
	}
	d.Group("load").Storage = "disk"
	if err := d.Validate(testRules()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("unknown class accepted", err)
	}
	d = testDocument()
	d.MetaGroup().Storage = StorageMemory
	if err := d.Validate(testRules()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("memory meta accepted", err)
	}
}

func TestStorageClassIsImmutable(t *testing.T) {
	t.Parallel()
	before := testDocument()
	before.Group("load").Storage = StorageMemory
	same := before.Clone()
	same.Version++
	if err := ValidateTransition(before, same, testRules(), nil); err != nil {
		t.Fatal("unchanged class refused", err)
	}
	changed := before.Clone()
	changed.Group("load").Storage = StorageDefault
	if err := ValidateTransition(before, changed, testRules(), nil); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("class change accepted", err)
	}
}

func TestDefaultStorageKeepsGroupEncoding(t *testing.T) {
	t.Parallel()
	d := testDocument()
	g := d.Group("load")
	raw, err := json.Marshal(g)
	if err != nil || strings.Contains(string(raw), "storage") {
		t.Fatal("default class changed the persisted encoding", string(raw), err)
	}
	g.Storage = StorageMemory
	raw, err = json.Marshal(g)
	if err != nil || !strings.Contains(string(raw), `"storage":"memory"`) {
		t.Fatal(string(raw), err)
	}
}
