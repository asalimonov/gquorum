//go:build !functional

package gquorum

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestAuthorityPathRejectsAliasesAndRetirementRoots(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"separate", "symlink", "hardlink", "parent symlink", "dangling symlink"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			meta, authority := filepath.Join(root, "_meta.bolt"), filepath.Join(root, "_authority.bolt")
			if err := os.WriteFile(meta, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			o := NodeOptions{StorageDir: root}
			switch name {
			case "symlink":
				if err := os.Symlink(meta, authority); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(meta, authority); err != nil {
					t.Fatal(err)
				}
			case "parent symlink":
				wal := filepath.Join(root, "wal")
				if err := os.Mkdir(wal, 0700); err != nil {
					t.Fatal(err)
				}
				alias := filepath.Join(root, "alias")
				if err := os.Symlink(wal, alias); err != nil {
					t.Fatal(err)
				}
				o.StorageDir = filepath.Join(alias, "new")
				o.Loads.Storage.WAL = &WALOptions{RootDir: wal}
			case "dangling symlink":
				if err := os.Symlink(filepath.Join(root, "absent"), authority); err != nil {
					t.Fatal(err)
				}
			}
			// Lexical validation intentionally does no filesystem inspection.
			if err := validateStorageDir(o); err != nil {
				t.Fatal(err)
			}
			err := (&node{opts: o}).checkAuthorityPath()
			if name == "separate" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("unsafe path accepted", err)
			}
			data, err := os.ReadFile(meta)
			if err != nil || string(data) != "retained" {
				t.Fatal("preflight modified evidence", err)
			}
		})
	}
}

func TestSystemStorageRejectsOldFilenamesWithoutCreatingReplacement(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"meta.bolt", "authority.bolt"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			old := filepath.Join(dir, name)
			if err := os.WriteFile(old, []byte("retained evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			n := &node{opts: NodeOptions{StorageDir: dir}}
			if err := n.checkAuthorityPath(); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("old filenames were treated as fresh storage", err)
			}
			data, err := os.ReadFile(old)
			if err != nil || string(data) != "retained evidence" {
				t.Fatal("retained evidence changed", err)
			}
			for _, name := range []string{metaDBFilename, authorityDBFilename} {
				if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("replacement storage was created", name, err)
				}
			}
		})
	}
}
