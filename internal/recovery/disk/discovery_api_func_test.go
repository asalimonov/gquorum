//go:build functional

package disk_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"

	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"

	"github.com/asalimonov/gquorum/internal/recovery/model"

	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryDiscoveryIgnoresRuntimeOwnersWALMountsAndEvolvingPin(t *testing.T) {
	t.Parallel()
	root, wal := t.TempDir(), t.TempDir()
	paths := model.Paths{StorageDir: root, LoadWALDir: wal}
	owner, err := recoverydisk.Acquire(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	canonical := paths
	canonical.StorageDir, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	canonical.LoadWALDir, err = filepath.EvalSymlinks(wal)
	if err != nil {
		t.Fatal(err)
	}
	p := recoverytest.Accepted(canonical, "discovery")
	digest, err := model.DigestAccepted(p)
	if err != nil {
		t.Fatal(err)
	}
	j, err := recoverydisk.Create(t.Context(), owner, p, model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true})
	if err != nil {
		t.Fatal(err)
	}
	issue := model.Issue{Code: "pending_validation", Message: "retained report", Remedy: model.RemedyRetry}
	if err := j.SetPhase(t.Context(), model.Files, &issue); err != nil {
		t.Fatal(err)
	}
	mustNotRun := func() { t.Error("read-only discovery invoked application") }
	facade, err := quorum.NewRecovery(quorum.NodeOptions{StorageDir: root, Identity: topology.Identity{NodeName: "n1", ClusterID: "recovered"}, Loads: quorum.LoadOptions{Storage: quorum.LoadStorageOptions{WAL: &quorum.WALOptions{RootDir: wal}}, Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
		mustNotRun()
		return load.Application{}, fault.ErrInvalid
	}}}, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { mustNotRun(); return fault.ErrInvalid }})
	if err != nil {
		t.Fatal(err)
	}
	// Keep both runtime locks held, make WAL unreachable at its configured path,
	// and replace source pin bytes. Discovery must read only the system journal.
	if err := os.Rename(wal, wal+"-offline"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Rename(wal+"-offline", wal); err != nil {
			t.Error(err)
		}
	})
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("new lineage"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	status, err := facade.Status(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if status.RecoveryID != "discovery" || status.Identity.ClusterID != "recovered" || status.Phase != recovery.Files || status.Complete || status.Identity.InstanceID != "" {
		t.Fatal(status)
	}
	rows, err := facade.List(t.Context())
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	rows[0].Issue.Message = "changed"
	again, err := facade.Status(t.Context(), "discovery")
	if err != nil || again.Issue == nil || again.Issue.Message != "retained report" {
		t.Fatal("caller changed retained report", again, err)
	}
	if _, err := facade.Status(t.Context(), "missing"); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("unknown operation did not classify", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := facade.Status(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, recoverydisk.Namespace, "foreign"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err = facade.List(t.Context())
	var refusal *recovery.Refusal
	if len(rows) != 1 || !errors.As(err, &refusal) || refusal.Issue.Code != "invalid_namespace" {
		t.Fatal("invalid namespace hid valid status or lost diagnostic", rows, err)
	}
}
