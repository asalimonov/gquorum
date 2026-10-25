//go:build functional

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func recoveryFixtureGroup(t *testing.T, a *application) *load.Group {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	voters := topology.DefaultVoterConfig()
	voters.TargetVoters = 1
	result, err := a.currentCluster().CreateGroup(ctx, gquorum.GroupSpec{ID: "work", VoterConfig: &voters},
		gquorum.MutationOptions{ExpectedVersion: 1, Actor: "test", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.currentCluster().WaitOperation(ctx, result.OperationID); err != nil {
		t.Fatal(err)
	}
	group, found := a.currentNode().LoadGroup("work")
	if !found {
		t.Fatal("missing group")
	}
	apiEventually(t, group.IsLeader)
	raw, _ := json.Marshal(putCommand{RequestID: "saved", Key: "key", Value: "survives"})
	if _, err := group.Propose(ctx, load.Proposal{RequestID: "saved", Data: raw}); err != nil {
		t.Fatal(err)
	}
	if err := group.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	return group
}
func recoveryHTTP(t *testing.T, a *application, method, path string, body any, want int) *recoveryJob {
	t.Helper()
	response := fixtureRequest(t.Context(), a, method, path, body)
	if response.Code != want {
		t.Fatalf("%s %s: %d %s", method, path, response.Code, response.Body.String())
	}
	var job recoveryJob
	if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	return &job
}
func waitRecoveryJob(t *testing.T, a *application, ready func(*recoveryJob) bool) *recoveryJob {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for {
		job, err := a.recoveryJobs.view(ctx, "job")
		if err != nil {
			t.Fatal(err)
		}
		if ready(job) {
			return job
		}
		if job.Error != "" {
			t.Fatal("recovery failed", job.Phase, job.Error, job.Status)
		}
		select {
		case <-ctx.Done():
			t.Fatal("job timeout", job)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func prepareFixtureRecovery(t *testing.T, a *application) *recoveryJob {
	t.Helper()
	body := prepareRecoveryBody{ID: "job", Request: recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "permanent loss"}}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/prepare", body, 202)
	return waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "prepared" })
}
func TestRemoteRecoveryJobCompleteAndRetainedRestart(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	group := recoveryFixtureGroup(t, a)
	old := a.currentNode().Identity()
	runs := a.recoveryControls.runs.Load()
	if err := writeProjection(t.Context(), a.projectionPath(walKey{"work", group.ReplicaID()}), []byte(`{"owned":"data"}`)); err != nil {
		t.Fatal(err)
	}
	job := prepareFixtureRecovery(t, a)
	saved, err := readRecoveryJob(a.cfg.DataDir)
	if err != nil || !saved.Maintenance || saved.Request.RecoveryID == "" || saved.Plan.Digest != job.Plan.Digest {
		t.Fatal(saved, err)
	}
	// The request may vanish after the durable intent; the stable ID returns the
	// same plan and cannot accidentally generate another lineage.
	repeated := prepareFixtureRecovery(t, a)
	if repeated.Plan.Digest != job.Plan.Digest {
		t.Fatal("prepare retry changed identity")
	}
	if w := fixtureRequest(t.Context(), a, "GET", "/api/status", nil); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := fixtureRequest(t.Context(), a, "GET", "/health/live", nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := fixtureRequest(t.Context(), a, "PUT", "/internal/test/recovery/controls", map[string]bool{"hold_readiness": true}); w.Code != 200 {
		t.Fatal(w.Code)
	}
	acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/confirm", acceptance, 202)
	apiEventually(t, func() bool { return a.recoveryControls.readiness.Load() > 0 })
	if a.recoveryControls.runs.Load() != runs {
		t.Fatal("Run escaped validation")
	}
	if w := fixtureRequest(t.Context(), a, "GET", "/internal/test/recovery", nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := fixtureRequest(t.Context(), a, "PUT", "/internal/test/recovery/controls", map[string]bool{}); w.Code != 200 {
		t.Fatal(w.Code)
	}
	complete := waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "complete" })
	if complete.Maintenance || complete.Status.Phase != recovery.Complete || len(complete.Status.History) < 5 {
		t.Fatal(complete)
	}
	identity := a.currentNode().Identity()
	if identity.ClusterID == old.ClusterID || identity.MemberID <= old.MemberID || identity.InstanceID == old.InstanceID {
		t.Fatal(identity, old)
	}
	local := a.local("work")
	if local == nil || local.machine.values()["key"] != "survives" {
		t.Fatal("lost recovered values", local)
	}
	if a.recoveryControls.recoveredFactories.Load() != 1 {
		t.Fatal("factory recovery input missing")
	}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/confirm", acceptance, 202)
	if w := fixtureRequest(t.Context(), a, "POST", "/internal/test/node/stop", nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := fixtureRequest(t.Context(), a, "POST", "/internal/test/node/start", nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if a.currentNode().Identity().ClusterID != identity.ClusterID {
		t.Fatal("restart returned to A")
	}
}
func TestRemoteRecoveryCancellationJoinsBackupAndRestoresA(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	group := recoveryFixtureGroup(t, a)
	if err := writeProjection(t.Context(), a.projectionPath(walKey{"work", group.ReplicaID()}), []byte(`{"old":"projection"}`)); err != nil {
		t.Fatal(err)
	}
	original := a.currentNode().Identity()
	job := prepareFixtureRecovery(t, a)
	fixtureRequest(t.Context(), a, "PUT", "/internal/test/recovery/controls", map[string]bool{"hold_after_backup": true, "fail_undo_once": true})
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/confirm", recovery.Acceptance{
		PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}, 202)
	apiEventually(t, func() bool { return a.recoveryControls.backedUp.Load() == 1 })
	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, "established.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("backup did not move application files", err)
	}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/cancel", recoveryCancellation{Actor: "operator", Reason: "inspect again"}, 202)
	// Undo runs only after the interrupted Files callback has joined.
	apiEventually(t, func() bool { return a.recoveryJobs.snapshot().Error != "" })
	failed := a.recoveryJobs.snapshot()
	if !failed.Maintenance || failed.Phase != "cancelling" {
		t.Fatal("failed Undo released maintenance", failed)
	}
	if w := fixtureRequest(t.Context(), a, "POST", "/internal/test/node/start", nil); w.Code != 503 {
		t.Fatal("manual start bypassed maintenance", w.Code)
	}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/retry", nil, 202)

	if w := fixtureRequest(t.Context(), a, "GET", "/health/live", nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	fixtureRequest(t.Context(), a, "PUT", "/internal/test/recovery/controls", map[string]bool{})
	cancelled := waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "cancelled" })
	if cancelled.Status.Phase != recovery.Cancelled || cancelled.Maintenance {
		t.Fatal(cancelled)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if id := a.currentNode().Identity(); id.ClusterID != original.ClusterID || id.MemberID != original.MemberID {
		t.Fatal(id, original)
	}
	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, "established.json")); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(a.projectionPath(walKey{"work", group.ReplicaID()}))
	if err != nil || string(raw) != `{"old":"projection"}` {
		t.Fatal("Undo changed projection", string(raw), err)
	}
}
func TestRemoteRecoveryUnknownAndPreparationCancel(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryHTTP(t, a, "GET", "/internal/test/recovery/unknown", nil, 404)
	if _, err := a.recoveryJobs.view(ctx, "unknown"); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal(err)
	}
	prepareFixtureRecovery(t, a)
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/cancel", recoveryCancellation{Actor: "operator", Reason: "never accepted"}, 202)
	waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "cancelled" })
	handle, err := a.recoveryJobs.handle()
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := handle.List(ctx)
	if err != nil || len(statuses) != 0 {
		t.Fatal(statuses, err)
	}
}

func reopenRecoveryFixture(t *testing.T, old *application) *application {
	t.Helper()
	cfg := old.cfg
	cfg.InstanceID = "new-process"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := old.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var supplied listeners
	for _, entry := range []struct {
		address string
		target  *net.Listener
	}{
		{cfg.MetaAddress, &supplied.meta}, {cfg.LoadAddress, &supplied.load}, {cfg.HTTPAddress, &supplied.http},
	} {
		listener, err := net.Listen("tcp", entry.address)
		if err != nil {
			t.Fatal(err)
		}
		*entry.target = listener
	}
	next, err := newApplication(cfg, old.logger, &supplied)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := next.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := next.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return next
}
func TestRemoteRecoveryProcessRestartUsesMaintenanceBeforePinChecks(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	recoveryFixtureGroup(t, a)
	job := prepareFixtureRecovery(t, a)
	// A fresh caller process must serve management while holding A stopped. It
	// recovers the generated request and requires the original acceptance digest.
	a = reopenRecoveryFixture(t, a)
	current := recoveryHTTP(t, a, "GET", "/internal/test/recovery/job", nil, 200)
	if !current.Maintenance || !reflect.DeepEqual(current.Request, job.Request) {
		t.Fatal(current, job)
	}
	fixtureRequest(t.Context(), a, "PUT", "/internal/test/recovery/controls", map[string]bool{"hold_after_backup": true})
	acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/confirm", acceptance, 202)
	apiEventually(t, func() bool { return a.recoveryControls.backedUp.Load() > 0 })
	// The application affiliation file is now archived. Restart must consult the
	// job before recoverBootMode, and repeat the hook idempotently via public Run.
	a = reopenRecoveryFixture(t, a)
	complete := waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "complete" })
	if complete.Acceptance == nil || *complete.Acceptance != acceptance {
		t.Fatal("lost exact acceptance", complete)
	}
	if local := a.local("work"); local == nil || local.machine.values()["key"] != "survives" {
		t.Fatal("lost state after caller restart")
	}
	// A second ordinary process restart takes its affiliation from the completed
	// job instead of the unchanged container's original cluster argument.
	destination := complete.Status.Identity.ClusterID
	a = reopenRecoveryFixture(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if a.currentNode().Identity().ClusterID != destination {
		t.Fatal("booted old lineage")
	}
}

func TestRemoteRecoveryConfirmationQueuesBehindInspectOwner(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	a.recoveryJobs.afterInspect = func(ctx context.Context) error {
		close(held)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	job := prepareFixtureRecovery(t, a)
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	recoveryHTTP(t, a, "POST", "/internal/test/recovery/job/confirm", acceptance, 202)
	// Confirmation is durably saved while Inspect still owns this worker slot.
	saved, err := readRecoveryJob(a.cfg.DataDir)
	if err != nil || saved.Acceptance == nil || *saved.Acceptance != acceptance {
		t.Fatal(saved, err)
	}
	close(release)
	complete := waitRecoveryJob(t, a, func(j *recoveryJob) bool { return j.Phase == "complete" })
	if complete.Status == nil || complete.Status.Phase != recovery.Complete {
		t.Fatal(complete)
	}
}
