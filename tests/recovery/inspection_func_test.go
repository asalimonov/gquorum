//go:build functional

package recovery_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func recoveryFixtureOptions(t *testing.T, withLoad bool) quorum.NodeOptions {
	t.Helper()
	if !withLoad {
		return recoveryFixtureWithSpec(t, nil)
	}
	spec := singleReplicaSpec("work", nil)
	return recoveryFixtureWithSpec(t, &spec)
}

func recoveryFixtureWithSpec(t *testing.T, spec *quorum.GroupSpec) quorum.NodeOptions {
	t.Helper()
	return recoveryFixtureStorage(t, spec, "")
}

func recoveryFixtureStorage(t *testing.T, spec *quorum.GroupSpec, walRoot string) quorum.NodeOptions {
	t.Helper()
	withLoad := spec != nil
	root := t.TempDir()
	options := authorityNodeOptions(t, root, "recovery-source")
	if withLoad {
		if walRoot == "" {
			walRoot = filepath.Join(root, "loads")
		}
		loopback := &loopbackTransport{}
		options.Loads = quorum.LoadOptions{Transport: loopback, Storage: quorum.LoadStorageOptions{WAL: &quorum.WALOptions{RootDir: walRoot}},
			Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
				return load.Application{StateMachine: ensureMachine{}}, nil
			}}
	}
	node, err := newNode(options)
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
		created, err := node.Cluster().CreateGroup(ctx, *spec, quorum.MutationOptions{ExpectedVersion: 1, Actor: "fixture", Approve: true})
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
			facade, err := newRecovery(opts, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { callbacks.Add(1); return fault.ErrInvalid }})
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
			node, err := newNode(guardOptions)
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
	facade, err := newRecovery(opts)
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

func TestRecoveryApprovalRejectsStaleMetadataAndRequiresExactAttestations(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "exact evidence"})
	if err != nil {
		t.Fatal(err)
	}
	accept := recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	empty := plan.Request
	empty.RecoveryID = ""
	if _, err := facade.Approve(t.Context(), empty, accept); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("Approve generated an unreviewed ID", err)
	}
	for _, invalid := range []recovery.Acceptance{{PlanDigest: plan.Digest, OldDeploymentFenced: true}, {PlanDigest: plan.Digest, AcknowledgeDataLoss: true}} {
		if _, err := facade.Approve(t.Context(), plan.Request, invalid); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("missing attestation accepted", err)
		}
	}
	store, err := metastore.Open(filepath.Join(opts.StorageDir, "_meta.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOverride(1); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.Approve(t.Context(), plan.Request, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("stale logical metadata evidence accepted", err)
	}
	if _, err := os.Stat(filepath.Join(opts.StorageDir, recoverydisk.Namespace)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed approval published a guard", err)
	}
	fresh, err := facade.Inspect(t.Context(), plan.Request)
	if err != nil || fresh.Digest == plan.Digest {
		t.Fatal("logical source change not fingerprinted", fresh, err)
	}
	accept.PlanDigest = fresh.Digest
	if _, err := facade.Approve(t.Context(), fresh.Request, accept); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryConcurrentApprovalsShareOneGuardAndExactResult(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "concurrent exact retries"})
	if err != nil {
		t.Fatal(err)
	}
	accept := recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	type result struct {
		operation recovery.Operation
		err       error
	}
	const callers = 16
	results := make(chan result, callers)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range callers {
		workers.Go(func() {
			<-start
			operation, err := facade.Approve(t.Context(), plan.Request, accept)
			results <- result{operation, err}
		})
	}
	close(start)
	workers.Wait()
	close(results)
	var accepted recovery.Operation
	for result := range results {
		if result.err != nil {
			if !errors.Is(result.err, fault.ErrUnavailable) {
				t.Fatal("exact concurrent approval conflicted", result.err)
			}
			continue
		}
		if accepted.RecoveryID != "" && result.operation != accepted {
			t.Fatal("concurrent approvals allocated different operations")
		}
		accepted = result.operation
	}
	if accepted.RecoveryID == "" {
		t.Fatal("no caller acquired approval ownership")
	}
	listed, err := facade.List(t.Context())
	if err != nil || len(listed) != 1 || listed[0].Phase != recovery.Approved || listed[0].PlanDigest != accepted.PlanDigest {
		t.Fatal(listed, err)
	}
}

func TestRecoveryInspectionRejectsMemoryAndApprovalCannotReuseCancelledID(t *testing.T) {
	t.Parallel()
	spec := singleReplicaSpec("cache", nil)
	spec.Storage = topology.StorageMemory
	opts := recoveryFixtureWithSpec(t, &spec)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "must not empty missing data"})
	if !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("memory source was not explicitly unsupported", plan, err)
	}
	for _, group := range plan.Groups {
		if group.GroupID == "cache" && group.Usable {
			t.Fatal("memory group became recoverable", group)
		}
	}
	reset, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.ResetForJoin, ClusterID: "recovered", Actor: "operator", Comment: "reset ownership only"})
	if err != nil {
		t.Fatal(reset, err)
	}
	accept := recovery.Acceptance{PlanDigest: reset.Digest, AcknowledgeDataLoss: true}
	operation, err := facade.Approve(t.Context(), reset.Request, accept)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: opts.StorageDir, LoadWALDir: opts.Loads.Storage.WAL.RootDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	journal, err := recoverydisk.Open(t.Context(), owner, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	audit := model.ActionAudit{Actor: "operator", Reason: "cancel before archive", At: operation.AcceptedAt}
	if err := journal.BeginCancellation(t.Context(), audit); err != nil {
		t.Fatal(err)
	}
	if err := journal.CompleteCancellation(t.Context(), operation.AcceptedAt); err != nil {
		t.Fatal(err)
	}
	if err := journal.ClearCurrent(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.Approve(t.Context(), reset.Request, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("cancelled ID was reapproved", err)
	}
}

func TestRecoveryFingerprintIgnoresUnusedMetadataPages(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "semantic metadata fingerprint"}
	before, err := facade.Inspect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(opts.StorageDir, "_meta.bolt"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(make([]byte, 64<<10)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := facade.Inspect(t.Context(), before.Request)
	if err != nil || after.Digest != before.Digest {
		t.Fatal("unused bbolt allocation affected semantic source fingerprint", after, err)
	}
}

func TestRecoveryApprovalHistoricalRepeatUsesSavedEvidenceAndLatchedOptions(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "historical retry"})
	if err != nil {
		t.Fatal(err)
	}
	accept := recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	original, err := facade.Approve(t.Context(), plan.Request, accept)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: opts.StorageDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	journal, err := recoverydisk.Open(t.Context(), owner, plan.Request.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.SetPhase(t.Context(), model.Files, nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0); err != nil {
		t.Fatal(err)
	}
	if err := journal.SetPhase(t.Context(), model.Archiving, nil); err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.View().Archive {
		if err := journal.MarkArchive(t.Context(), entry.Root, entry.Name); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.SetPhase(t.Context(), model.AwaitingStart, nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkSuccessorStarted(t.Context(), original.AcceptedAt); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []model.Phase{model.Importing, model.Validating, model.Complete} {
		if err := journal.SetPhase(t.Context(), phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		if err := os.WriteFile(filepath.Join(opts.StorageDir, name), []byte("evolving B must not be read for old approval"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	repeated, err := facade.Approve(t.Context(), plan.Request, accept)
	if err != nil || repeated != original {
		t.Fatal("historical retry read evolving successor", repeated, err)
	}
	changed := plan.Request
	changed.Comment = "different audit"
	if _, err := facade.Approve(t.Context(), changed, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed audit accepted", err)
	}
	changed = plan.Request
	changed.ClusterID = "different-destination"
	if _, err := facade.Approve(t.Context(), changed, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed destination accepted", err)
	}
	withHook, err := newRecovery(opts, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { t.Error("approval invoked hook"); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withHook.Approve(t.Context(), plan.Request, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed hook declaration accepted", err)
	}
	withBounds, err := newRecovery(opts, recovery.Options{Resources: recovery.Resources{MaxRecordBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withBounds.Approve(t.Context(), plan.Request, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed explicit bounds accepted", err)
	}
}

func TestRecoveryInspectionRefusesBusyAndMismatchedSource(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: opts.StorageDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "source qualification"}
	if _, err := facade.Inspect(t.Context(), request); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("busy source was inspected", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*quorum.NodeOptions){func(o *quorum.NodeOptions) { o.Identity.NodeName = "wrong-node" }, func(o *quorum.NodeOptions) { o.Identity.ClusterID = "wrong-cluster" }} {
		wrong := opts
		change(&wrong)
		reader, err := newRecovery(wrong)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Inspect(t.Context(), request); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("configured source mismatch accepted", err)
		}
	}
}

func TestRecoveryInspectionReplaysOnlyCommitDespiteUnsyncedNewerTermTail(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, true)
	root := opts.Loads.Storage.WAL.RootDir
	source, err := walbackend.ReadRecovery(t.Context(), root, "work", model.ReadBounds{MaxRecordBytes: 4 << 20, MaxGroupBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	point := walpb.Snapshot{Index: source.Recovery.Snapshot.Metadata.Index, Term: source.Recovery.Snapshot.Metadata.Term}
	writer, err := wal.Open(zap.NewNop(), filepath.Join(root, "work", walbackend.WALDir), point)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := writer.ReadAll(); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	entry := raftpb.Entry{Index: source.LastLog.Index + 1, Term: source.Recovery.HardState.Term + 1, Data: []byte("not a committed engine envelope")}
	if err := writer.Save(raftpb.HardState{}, []raftpb.Entry{entry}); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	before := sourceFileHashes(t, opts.StorageDir)
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "recover retained committed prefix"})
	if err != nil {
		t.Fatal(plan, err)
	}
	found := false
	for _, issue := range plan.Issues {
		if issue.Code == "unsynced_tail_term" && issue.GroupID == "work" && !issue.Blocking {
			found = true
		}
	}
	if !found {
		t.Fatal("unsynced evidence was hidden", plan.Issues)
	}
	for _, group := range plan.Groups {
		if group.GroupID == "work" && (!group.Usable || group.Commit.Index != source.Recovery.HardState.Commit || group.LastLog.Index != entry.Index || group.LastLog.Term != entry.Term) {
			t.Fatal("commit was inferred or unsynced tail interpreted", group)
		}
	}
	if _, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
		t.Fatal("inspection or approval truncated unsynced source evidence")
	}
}

func TestRecoveryUnknownLiveWALBlocksAndReportsRetainedPositions(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, true)
	store, err := walbackend.Open(walbackend.Options{RootDir: opts.Loads.Storage.WAL.RootDir, NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	id := storage.Identity{ClusterID: "cluster", NodeName: "n1", GroupID: "unrecognized", ReplicaID: 77}
	if err := store.BindReplica(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveReady(t.Context(), id.GroupID, storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 1, Term: 1}}, HardState: raftpb.HardState{Term: 1, Vote: id.ReplicaID, Commit: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(t.Context(), id.GroupID, raftpb.Snapshot{Data: []byte("opaque snapshot"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{id.ReplicaID}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	facade, err := newRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "must classify every local WAL", RequiredGroups: []string{topology.MetaGroupID}})
	if err == nil {
		t.Fatal("unknown live WAL was silently archived", plan)
	}
	positions := false
	covered := false
	for _, group := range plan.Groups {
		if group.GroupID == id.GroupID && !group.Usable && group.Source == id && group.Snapshot.Index == 1 && group.Commit.Index == 1 && group.LastLog.Index == 1 {
			positions = true
		}
		if group.GroupID == "work" && group.Usable {
			covered = true
		}
	}
	if !positions || !covered {
		t.Fatal("unknown positions or additive approved coverage were omitted", plan.Groups)
	}
}

func TestRecoveryBoundsRefuseBeforeApprovalWithoutCallingApplication(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name   string
		bounds recovery.Resources
	}{
		{"metadata", recovery.Resources{MaxMetadataBytes: 1}},
		{"record", recovery.Resources{MaxRecordBytes: 1}},
		{"group", recovery.Resources{MaxGroupBytes: 1}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, true)
			before := sourceFileHashes(t, opts.StorageDir)
			opts.Loads.Factory = nil // Offline bounds checks cannot use application callbacks.
			handle, err := newRecovery(opts, recovery.Options{Resources: shape.bounds})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := handle.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "bounded source"})
			var refusal *recovery.Refusal
			if !errors.As(err, &refusal) || refusal.Issue.Code != "resource_bound" || !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("size budget was reported as damaged or missing data", plan, err)
			}
			if !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
				t.Fatal("size refusal changed source")
			}
			statuses, listErr := handle.List(t.Context())
			if listErr != nil || len(statuses) != 0 {
				t.Fatal("size refusal created an operation", statuses, listErr)
			}
		})
	}
}
