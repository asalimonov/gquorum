//go:build !functional

//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"testing"
	"time"

	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
)

type untouchedMeta struct{ MetaPort }

func TestRecoveryCoordinatorConstructionIsPureAndOwnsConfiguration(t *testing.T) {
	t.Parallel()
	manifest := recoverytest.Manifest(model.Paths{StorageDir: "/system"}, "coordinator", time.Unix(110, 0).UTC(), model.Importing)
	coordinator, err := New(Config{Manifest: manifest, Journal: &recoverydisk.Journal{}, Meta: untouchedMeta{}})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Approved.Nodes[0].Type = "caller mutation"
	if coordinator.config.Manifest.Approved.Nodes[0].Type != "worker" || coordinator.DataReady() {
		t.Fatal("configuration was borrowed")
	}
	if err := coordinator.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(t.Context()); err == nil {
		t.Fatal("stopped coordinator restarted")
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty ownership accepted")
	}
}
