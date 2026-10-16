//go:build functional

package localdb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func testStorageIdentity() storage.Identity {
	return storage.Identity{ClusterID: "test", NodeName: "node1", GroupID: "scheduler", ReplicaID: 11}
}
func openTestMachine(t *testing.T, root string) *Machine {
	t.Helper()
	m, err := Open(t.Context(), root, testStorageIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return m
}
func TestApplicationFilesRestartAndSnapshotRecovery(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	m := openTestMachine(t, root)
	// Opening storage creates the schema but no business rows.
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var initial snapshotState
	if err := jsonwire.Decode(data, &initial); err != nil || initial.Meta.LastTaskID != 0 || len(initial.Tasks) != 0 {
		t.Fatal("factory created business rows", err)
	}
	d := &machineDriver{t: t, m: m}
	d.create(1, executor(2))
	snapshot, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened := openTestMachine(t, root)
	got, err := reopened.Snapshot(t.Context())
	if err != nil || !bytes.Equal(snapshot, got) {
		t.Fatal("retained restart changed SQL", err)
	}
	if err := reopened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	dir, err := applicationDir(root, testStorageIdentity())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "tasks.sqlite")); err != nil {
		t.Fatal(err)
	}
	restored := openTestMachine(t, root)
	if err := restored.Restore(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	got, err = restored.Snapshot(t.Context())
	if err != nil || !bytes.Equal(got, snapshot) {
		t.Fatal("missing projection cannot reconstruct", err)
	}
}
func TestApplicationFilesPreserveForeignEvidence(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unmarked", "foreign", "symlink", "corrupt", "orphan-journal"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			id := testStorageIdentity()
			dir, err := applicationDir(root, id)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "tasks.sqlite")
			evidence := []byte("foreign-evidence")
			switch kind {
			case "unmarked":
				if err := os.WriteFile(path, evidence, 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				other := id
				other.NodeName = "successor"
				if err := verifyOrCreateIdentity(t.Context(), dir, other); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(dir, "identity.json")
				evidence, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := verifyOrCreateIdentity(t.Context(), dir, id); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(t.TempDir(), "foreign")
				if err := os.WriteFile(target, evidence, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := verifyOrCreateIdentity(t.Context(), dir, id); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, evidence, 0600); err != nil {
					t.Fatal(err)
				}
			case "orphan-journal":
				if err := verifyOrCreateIdentity(t.Context(), dir, id); err != nil {
					t.Fatal(err)
				}
				path += "-wal"
				if err := os.WriteFile(path, evidence, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if m, err := Open(t.Context(), root, id); err == nil {
				_ = m.Close(t.Context())
				t.Fatal("opened foreign/unreadable state")
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, evidence) {
				t.Fatal("evidence changed", err)
			}
		})
	}
}
func retirementRequest() load.RetirementRequest {
	id := testStorageIdentity()
	step := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: id.GroupID, Revision: 3}, Purpose: topology.AuthorityEffect, OperatorID: step.OperatorID, Action: &step, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: id.GroupID, NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: 1, ReplicaID: id.ReplicaID}}}
	return load.RetirementRequest{Identity: id, Grant: grant, Reason: load.RetirementTopology, Authorization: &step}
}
func TestRetirementRechecksProofAfterDeletion(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	m := openTestMachine(t, root)
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	req := retirementRequest()
	first, err := Retire(t.Context(), root, req)
	if err != nil || !first.Verified {
		t.Fatal(first, err)
	}
	second, err := Retire(t.Context(), root, req)
	if err != nil || !second.Verified {
		t.Fatal(second, err)
	}
	dir, err := applicationDir(root, req.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tasks.sqlite")); !os.IsNotExist(err) {
		t.Fatal("database retained", err)
	}
	bad := req
	bad.Grant = bad.Grant.Clone()
	bad.Grant.Key.Revision++
	if _, err := Retire(t.Context(), root, bad); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("different proof accepted after deletion", err)
	}
	bad = req
	bad.Authorization = nil
	if _, err := Retire(t.Context(), root, bad); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("incomplete proof accepted after deletion", err)
	}
	if _, err := Open(t.Context(), root, req.Identity); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal("retired identity reopened", err)
	}
}
