//go:build !functional

package main

import (
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/storage/walstore"
)

func TestFixtureStorageOwnershipSelection(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{DataDir: "/configured", NodeOwnedWAL: true}}
	first := a.loadStorageOptions()
	if first.Store != nil || first.Retirement != nil || first.WAL == nil || first.WAL.RootDir != filepath.Join(a.cfg.DataDir, "load") || first.WAL.SegmentBytes != 1<<20 || first.WAL.Prewrite == nil || *first.WAL.Prewrite {
		t.Fatal("owned mode did not select Node WAL", first)
	}
	*first.WAL.Prewrite = true
	first.WAL.RootDir = "changed"
	again := a.loadStorageOptions()
	if *again.WAL.Prewrite || again.WAL.RootDir != filepath.Join(a.cfg.DataDir, "load") {
		t.Fatal("owned construction settings aliased earlier options")
	}
	a.cfg.NodeOwnedWAL = false
	a.store = &walstore.Store{}
	borrowed := a.loadStorageOptions()
	store, ok := borrowed.Store.(*fixtureStore)
	if !ok || store.Store != a.store || borrowed.WAL != nil || borrowed.Retirement == nil {
		t.Fatal("borrowed mode lost original wrapper", borrowed)
	}
}
