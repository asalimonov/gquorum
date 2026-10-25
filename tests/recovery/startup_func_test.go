//go:build functional

package recovery_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryNodeMetadataOnlyFoundingImportAndCompletion(t *testing.T) {
	t.Parallel()
	for _, gate := range []lifecycle.StartupGate{lifecycle.LocalAdmission, lifecycle.AllFounders} {
		t.Run(string(gate), func(t *testing.T) { t.Parallel(); recoveryMetadataOnly(t, gate) })
	}
}

func recoveryMetadataOnly(t *testing.T, gate lifecycle.StartupGate) {
	t.Helper()
	source := recoveryFixtureOptions(t, false)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "recovered-process")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Lifecycle.Default.Startup = gate
	options, err = quorum.RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := newNode(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err, participant.Status())
	}
	completed, err := facade.Status(ctx, operation.RecoveryID)
	if err != nil || completed.Phase != recovery.Complete {
		t.Fatal(completed, err)
	}
	// Successful ordinary administration proves the initial recovery gate detached.
	if err := participant.SetSchedulingEnabled(ctx, false); err != nil {
		t.Fatal("completed recovery retained a mutation gate", err)
	}
	actual := participant.Identity()
	if actual.ClusterID != status.Identity.ClusterID || actual.MemberID != status.Identity.MemberID || actual.InstanceID != "recovered-process" {
		t.Fatal("recovered founder retained old protocol identity", actual)
	}
	observed, err := participant.Cluster().ReadTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Target.Version != 2 || observed.Target.Origin != topology.OriginOperator {
		t.Fatal(observed)
	}
	stopAuthorityNode(t, participant)
	// Completed history is evidence, never a second recovery/founding permission.
	restart := authorityNodeOptions(t, source.StorageDir, "recovered-restart")
	restart.Identity.ClusterID = status.Identity.ClusterID
	restart.Identity.MemberID = actual.MemberID
	restart.Bootstrap.Founders[0].MemberID = actual.MemberID
	restarted, err := newNode(restart)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, restarted) })
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.Identity().MemberID != actual.MemberID {
		t.Fatal("retained B was reseeded")
	}
}

func recoverySourceWithLimits(t *testing.T) quorum.NodeOptions {
	t.Helper()
	source := authorityNodeOptions(t, t.TempDir(), "source-custom-rules")
	source.Limits = topology.Limits{MaxNodes: 4}
	participant, err := newNode(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, participant)
	return source
}

func TestRecoveryNodeInheritsAcceptedRulesAndRefusesConflictingStartupBeforeMarker(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inherit", "explicit conflict", "wrong cluster", "wrong bootstrap", "unjournaled successor"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			source := recoverySourceWithLimits(t)
			facade, operation := acceptedRecovery(t, source, nil)
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			options := authorityNodeOptions(t, source.StorageDir, "destination")
			options.Node = source.Bootstrap.Founders[0].Node
			options, err = quorum.RecoveryStartOptions(status, options)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "explicit conflict":
				options.Limits = topology.Limits{MaxNodes: 3}
			case "wrong cluster":
				options.Identity.ClusterID = "different-cluster"
			case "wrong bootstrap":
				options.Bootstrap.Join = true
				options.Bootstrap.Founders = nil
				options.Bootstrap.JoinSeeds = []string{"127.0.0.1:1"}
			case "unjournaled successor":
				if err := os.WriteFile(filepath.Join(source.StorageDir, authorityDBFilename), []byte("unexpected successor"), 0o600); err != nil {
					t.Fatal(err)
				}
			default:
			}
			participant, err := newNode(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopAuthorityNode(t, participant) })
			err = participant.Start(t.Context())
			if mode == "inherit" {
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := participant.WaitReady(ctx); err != nil {
					t.Fatal(err, participant.Status())
				}
				// Effective replicated capacity is verified by the runtime startup tests;
				// this public case verifies successful accepted startup and retained restart.
				stopAuthorityNode(t, participant)
				restart := authorityNodeOptions(t, source.StorageDir, "rules-restart")
				restart.Identity.ClusterID, restart.Identity.MemberID = status.Identity.ClusterID, status.Identity.MemberID
				restart.Bootstrap.Founders[0].MemberID = status.Identity.MemberID
				restarted, err := newNode(restart)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { stopAuthorityNode(t, restarted) })
				if err := restarted.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := restarted.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid startup accepted")
			}
			if mode == "unjournaled successor" {
				if !errors.Is(err, fault.ErrConflict) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatal(err)
			}
			stopAuthorityNode(t, participant)
			record, err := recoverydisk.Read(t.Context(), sourcePaths(facade), operation.RecoveryID)
			if err != nil || record.SuccessorStartedAt != nil || record.Phase != model.AwaitingStart {
				t.Fatal(record, err)
			}
			if _, err := os.Stat(filepath.Join(source.StorageDir, metaDBFilename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refusal acquired destination metadata", err)
			}
			if mode != "unjournaled successor" {
				if _, err := os.Stat(filepath.Join(source.StorageDir, authorityDBFilename)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal acquired destination authority", err)
				}
			}
			reported, err := facade.Status(t.Context(), operation.RecoveryID)
			if err != nil || reported.Phase != recovery.AwaitingStart {
				t.Fatal(reported, err)
			}
		})
	}
}

func TestRecoveryRestartResumesEachPreFoundingAcquisitionBoundary(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"authority only", "fresh unpinned meta", "pinned before v1", "acquired metadata missing", "foreign authority", "unpinned nonfresh meta"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			source := recoveryFixtureOptions(t, false)
			facade, operation := acceptedRecovery(t, source, nil)
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := recoverydisk.Acquire(t.Context(), sourcePaths(facade))
			if err != nil {
				t.Fatal(err)
			}
			journal, err := recoverydisk.Open(t.Context(), owner, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.MarkSuccessorStarted(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			identity := status.Identity
			identity.InstanceID = "interrupted-process"
			if boundary == "foreign authority" {
				identity.ClusterID = "foreign"
			}
			authority, err := authoritystore.Open(filepath.Join(source.StorageDir, "_authority.bolt"), identity, authoritystore.FreshEvidence)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "acquired metadata missing" {
				if err := authority.MarkMetadataAcquired(t.Context(), identity); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "fresh unpinned meta" || boundary == "pinned before v1" || boundary == "unpinned nonfresh meta" {
				meta, err := metastore.Open(filepath.Join(source.StorageDir, "_meta.bolt"))
				if err != nil {
					t.Fatal(err)
				}
				if boundary == "pinned before v1" {
					if err := meta.EnsureIdentity(identity.NodeName, identity.ClusterID, identity.MemberID); err != nil {
						t.Fatal(err)
					}
				}
				if boundary == "unpinned nonfresh meta" {
					if err := meta.SaveOverride(1); err != nil {
						t.Fatal(err)
					}
				}
				if err := meta.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Release the process's actual acquired files exactly at this crash boundary;
			// no source/destination file is deleted or repaired to make restart pass.
			if err := authority.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			options := authorityNodeOptions(t, source.StorageDir, "resumed-process")
			options.Node = source.Bootstrap.Founders[0].Node
			options, err = quorum.RecoveryStartOptions(status, options)
			if err != nil {
				t.Fatal(err)
			}
			participant, err := newNode(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopAuthorityNode(t, participant) })
			startErr := participant.Start(t.Context())
			if boundary == "acquired metadata missing" || boundary == "foreign authority" || boundary == "unpinned nonfresh meta" {
				if !errors.Is(startErr, fault.ErrUnreadable) {
					t.Fatal("contradictory acquisition was resumed", startErr)
				}
				return
			}
			if startErr != nil {
				t.Fatal(startErr)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := participant.WaitReady(ctx); err != nil {
				t.Fatal(err, participant.Status())
			}
		})
	}
}

func TestRecoveryResetStartupUsesOwnedArchiveHistoryBeforeFreshJoin(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	facade, err := newRecovery(source)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.ResetForJoin, ClusterID: "named-recovered",
		Actor: "operator", Comment: "explicit reset for admission"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil || status.Phase != recovery.JoinReady {
		t.Fatal(status, err)
	}
	backup := filepath.Join(source.StorageDir, recoverydisk.Namespace, operation.BackupName, "backup")
	original := sourceFileHashes(t, backup)
	var factories atomic.Int32
	for _, instance := range []string{"first-join", "interrupted-join-retry"} {
		options := authorityNodeOptions(t, source.StorageDir, instance)
		options.Node = source.Bootstrap.Founders[0].Node
		options.Loads = source.Loads
		options.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
			factories.Add(1)
			return load.Application{StateMachine: ensureMachine{}}, nil
		}
		options.Bootstrap.JoinSeeds = []string{"127.0.0.1:1"}
		options, err = quorum.RecoveryStartOptions(status, options)
		if err != nil {
			t.Fatal(err)
		}
		participant, err := newNode(options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stopAuthorityNode(t, participant) })
		// No second participant: unreachable seed keeps this in the ordinary join path.
		if err := participant.Start(t.Context()); err != nil {
			t.Fatal("own reset archive counted as live WAL", err)
		}
		if participant.Status().Ready || participant.Identity().ClusterID != "named-recovered" {
			t.Fatal(participant.Status())
		}
		stopAuthorityNode(t, participant)
		after, err := facade.Status(t.Context(), operation.RecoveryID)
		if err != nil || after.Phase != recovery.JoinReady {
			t.Fatal("interrupted admission was re-archived", after, err)
		}
		if !reflect.DeepEqual(original, sourceFileHashes(t, backup)) {
			t.Fatal("interrupted reset altered A backup")
		}
	}
	if factories.Load() != 0 {
		t.Fatal("reset replayed an A application", factories.Load())
	}
}

func TestNodePendingRecoveryCannotRefound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "pending")
	at := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	name, err := model.BackupName(at, "pending")
	if err != nil {
		t.Fatal(err)
	}
	r := recoverytest.Manifest(model.Paths{StorageDir: o.StorageDir}, "pending", at, model.Approved)
	data, err := model.EncodeCanonical(r)
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
	n, err := newNode(o)
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
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: o.StorageDir})
	if err != nil {
		t.Fatal("joined teardown retained root ownership", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}
