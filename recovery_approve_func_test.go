//go:build functional

package gquorum

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryApprovalRejectsStaleMetadataAndRequiresExactAttestations(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, false)
	facade, err := NewRecovery(opts)
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
	facade, err := NewRecovery(opts)
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
	facade, err := NewRecovery(opts)
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
	facade, err := NewRecovery(opts)
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
	facade, err := NewRecovery(opts)
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
	withHook, err := NewRecovery(opts, recovery.Options{Files: func(context.Context, recovery.FilesTask) error { t.Error("approval invoked hook"); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withHook.Approve(t.Context(), plan.Request, accept); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed hook declaration accepted", err)
	}
	withBounds, err := NewRecovery(opts, recovery.Options{Resources: recovery.Resources{MaxRecordBytes: 1}})
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
	facade, err := NewRecovery(opts)
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
	for _, change := range []func(*NodeOptions){func(o *NodeOptions) { o.Identity.NodeName = "wrong-node" }, func(o *NodeOptions) { o.Identity.ClusterID = "wrong-cluster" }} {
		wrong := opts
		change(&wrong)
		reader, err := NewRecovery(wrong)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Inspect(t.Context(), request); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("configured source mismatch accepted", err)
		}
	}
}
