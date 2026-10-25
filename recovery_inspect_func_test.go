//go:build functional

package gquorum

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
)

func recoveryFixtureOptions(t *testing.T, withLoad bool) NodeOptions {
	t.Helper()
	if !withLoad {
		return recoveryFixtureWithSpec(t, nil)
	}
	spec := singleReplicaSpec("work", nil)
	return recoveryFixtureWithSpec(t, &spec)
}

func recoveryFixtureWithSpec(t *testing.T, spec *GroupSpec) NodeOptions {
	t.Helper()
	withLoad := spec != nil
	root := t.TempDir()
	options := authorityNodeOptions(t, root, "recovery-source")
	if withLoad {
		loopback := &loopbackTransport{}
		options.Loads = LoadOptions{Transport: loopback, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
			Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
				return load.Application{StateMachine: ensureMachine{}}, nil
			}}
	}
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	if withLoad {
		options.Loads.Transport.(*loopbackTransport).node = node
	}
	t.Cleanup(func() { stopAuthorityNode(t, node) })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := node.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if withLoad {
		created, err := node.Cluster().CreateGroup(ctx, *spec, MutationOptions{ExpectedVersion: 1, Actor: "fixture", Approve: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := node.Cluster().WaitOperation(ctx, created.OperationID); err != nil {
			t.Fatal(err)
		}
	}
	stopAuthorityNode(t, node)
	return options
}

func sourceFileHashes(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == recoverydisk.Namespace {
				return filepath.SkipDir
			}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[path] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRecoveryInspectApproveCompleteSourceAndExactRepeat(t *testing.T) {
	t.Parallel()
	for _, withLoad := range []bool{false, true} {
		t.Run(map[bool]string{false: "metadata only", true: "complete load"}[withLoad], func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, withLoad)
			before := sourceFileHashes(t, opts.StorageDir)
			var callbacks atomic.Int32
			opts.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
				callbacks.Add(1)
				return load.Application{}, fault.ErrInvalid
			}
			facade, err := NewRecovery(opts, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { callbacks.Add(1); return fault.ErrInvalid }})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "permanent majority loss"})
			if err != nil {
				t.Fatal(plan, err)
			}
			expected := 1
			if withLoad {
				expected = 2
			}
			if plan.Digest == "" || plan.Request.RecoveryID == "" || plan.Identity.ClusterID == plan.Source.ClusterID || plan.Identity.MemberID <= plan.Source.MemberID || !plan.MayLoseAcknowledgedWrites || len(plan.Groups) != expected {
				t.Fatal(plan)
			}
			for _, group := range plan.Groups {
				if !group.Usable || group.Snapshot.Index == 0 || group.Snapshot.Index > group.Commit.Index || group.Commit.Index > group.LastLog.Index {
					t.Fatal(group)
				}
			}
			again, err := facade.Inspect(t.Context(), plan.Request)
			if err != nil || again.Digest != plan.Digest {
				t.Fatal("inspection was not stable", again, err)
			}
			acceptance := recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
			operation, err := facade.Approve(t.Context(), plan.Request, acceptance)
			if err != nil {
				t.Fatal(err)
			}
			repeated, err := facade.Approve(t.Context(), plan.Request, acceptance)
			if err != nil || repeated != operation {
				t.Fatal("approval changed immutable result", repeated, err)
			}
			if callbacks.Load() != 0 || !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
				t.Fatal("inspection or approval invoked an application or changed source bytes")
			}
			guardOptions := opts
			if !withLoad {
				guardOptions.Loads.Factory = nil
			}
			node, err := NewNode(guardOptions)
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Start(t.Context()); !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal("Approved failed to guard startup", err)
			}
			stopAuthorityNode(t, node)
		})
	}
}

func TestRecoveryInspectionCollectsCoverageAndResetAllowsOwnedDamagedPayload(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, true)
	path := filepath.Join(opts.Loads.Storage.WAL.RootDir, "work", "snap")
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.WriteFile(filepath.Join(path, entry.Name()), []byte("damaged snapshot"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := sourceFileHashes(t, opts.StorageDir)
	facade, err := NewRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{RecoveryID: "damaged", Mode: recovery.Rebuild, Actor: "operator", Comment: "inspect all coverage", RequiredGroups: []string{"also-required"}}
	plan, err := facade.Inspect(t.Context(), request)
	if err == nil {
		t.Fatal("damaged complete source was accepted", plan)
	}
	issues := make(map[string]bool)
	for _, issue := range plan.Issues {
		issues[issue.Code] = true
	}
	if !issues["required_group_absent"] || !issues["unreadable_group"] || !issues["missing_group_coverage"] {
		t.Fatal("coverage problems were not collected", plan.Issues)
	}
	request.Mode, request.ClusterID, request.RequiredGroups = recovery.ResetForJoin, "recovered-cluster", nil
	reset, err := facade.Inspect(t.Context(), request)
	if err != nil || reset.Identity.MemberID != 0 || len(reset.Groups) != 0 {
		t.Fatal("owned damaged payload blocked reset", reset, err)
	}
	if !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
		t.Fatal("inspection changed damaged evidence")
	}
}
