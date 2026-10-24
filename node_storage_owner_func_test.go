//go:build functional

package gquorum

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage/walstore"
)

func nodeStoragePaths(o NodeOptions) recoverymodel.Paths {
	paths := recoverymodel.Paths{StorageDir: o.StorageDir}
	if o.Loads.Storage.WAL != nil {
		paths.LoadWALDir = o.Loads.Storage.WAL.RootDir
	}
	return paths
}

func assertNodeStorageBusy(t *testing.T, o NodeOptions) {
	t.Helper()
	for _, path := range []string{nodeStoragePaths(o).StorageDir, nodeStoragePaths(o).LoadWALDir} {
		if path == "" {
			continue
		}
		lease, err := recoverydisk.AcquireRoot(t.Context(), path)
		if lease != nil {
			_ = lease.Close()
		}
		if !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("owned root was released before teardown", path, err)
		}
	}
}

func assertNodeStorageReleased(t *testing.T, o NodeOptions) {
	t.Helper()
	owner, err := recoverydisk.Acquire(t.Context(), nodeStoragePaths(o))
	if err != nil {
		t.Fatal("joined teardown retained root ownership", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeStorageLeaseAndFailedApplicationClose(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "owned")
	loopback := &loopbackTransport{}
	var closes atomic.Int32
	closeFailure := errors.New("retry application release")
	o.Loads = LoadOptions{Transport: loopback, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}}, Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error {
			if closes.Add(1) == 1 {
				return closeFailure
			}
			return nil
		}}, nil
	}}
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	loopback.node = n
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	// Constructors must not reserve either root.
	assertNodeStorageReleased(t, o)
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	assertNodeStorageBusy(t, o)
	result, err := n.Cluster().CreateGroup(ctx, singleReplicaSpec("work", nil), MutationOptions{ExpectedVersion: 1, Actor: "test", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Cluster().WaitOperation(ctx, result.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := n.Stop(ctx); !errors.Is(err, closeFailure) {
		t.Fatal("Close failure was not retained", err)
	}
	assertNodeStorageBusy(t, o)
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	assertNodeStorageReleased(t, o)
	if closes.Load() != 2 {
		t.Fatal("application release did not retry exactly once", closes.Load())
	}
}

func TestNodeStorageContentionBeforeMetadataAcquisition(t *testing.T) {
	t.Parallel()
	for _, walHeld := range []bool{false, true} {
		t.Run(map[bool]string{false: "system", true: "independent WAL"}[walHeld], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			o := authorityNodeOptions(t, root, "blocked")
			path := root
			if walHeld {
				path = filepath.Join(root, "loads")
				o.Loads = LoadOptions{Transport: &loopbackTransport{}, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: path}}, Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
					t.Error("unexpected factory")
					return load.Application{}, fault.ErrUnavailable
				}}
			}
			var release func() error
			if walHeld {
				store, err := walstore.Open(walstore.Options{RootDir: path, NodeName: "n1", ClusterID: "cluster"})
				if err != nil {
					t.Fatal(err)
				}
				release = store.Close
			} else {
				owner, err := recoverydisk.AcquireRoot(t.Context(), path)
				if err != nil {
					t.Fatal(err)
				}
				release = owner.Close
			}
			t.Cleanup(func() {
				if err := release(); err != nil {
					t.Error(err)
				}
			})
			n, err := NewNode(o)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal("overlapping source owner", err)
			}
			stopAuthorityNode(t, n)
			for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
				if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("contended startup acquired metadata", name, err)
				}
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			assertNodeStorageReleased(t, o)
		})
	}
}

func TestNodePendingRecoveryCannotRefound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "pending")
	at := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	name, err := recoverymodel.BackupName(at, "pending")
	if err != nil {
		t.Fatal(err)
	}
	r := recoverymodel.LocalManifest{Schema: 1, Revision: 1, Request: recoverymodel.Request{RecoveryID: "pending", Mode: recoverymodel.Rebuild, Actor: "test", Comment: "lost quorum"}, Paths: nodeStoragePaths(o), BackupName: name, AcceptedAt: at, Phase: recoverymodel.Approved, PlanDigest: strings.Repeat("a", 64)}
	r.Acceptance.PlanDigest = r.PlanDigest
	data, err := recoverymodel.EncodeCanonical(r)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, recoverydisk.Namespace, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("pending recovery permitted founding", err)
	}
	stopAuthorityNode(t, n)
	for _, file := range []string{"_meta.bolt", "_authority.bolt"} {
		if _, err := os.Stat(filepath.Join(root, file)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("pending startup acquired metadata", file, err)
		}
	}
	assertNodeStorageReleased(t, o)
}
