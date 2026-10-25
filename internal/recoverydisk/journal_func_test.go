//go:build functional

package recoverydisk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
	"github.com/asalimonov/gquorum/topology"
)

func journalOwner(t *testing.T) *Owner {
	t.Helper()
	paths := model.Paths{StorageDir: t.TempDir(), LoadWALDir: t.TempDir()}
	owner, err := Acquire(t.Context(), paths)
	if err != nil {
		t.Fatal(err)
	}
	owner.now = func() time.Time { return time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC) }
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		if err := os.WriteFile(filepath.Join(paths.StorageDir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return owner
}
func acceptedJournal(t *testing.T, owner *Owner, id string, files bool) (*Journal, model.AcceptedPlan, model.Acceptance) {
	t.Helper()
	p := recoverytest.Accepted(owner.paths(), id)
	p.FilesRequired = files
	digest, err := model.DigestAccepted(p)
	if err != nil {
		t.Fatal(err)
	}
	a := model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	j, err := Create(t.Context(), owner, p, a)
	if err != nil {
		t.Fatal(err)
	}
	return j, p, a
}
func mustJournal(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestApprovalPublicationFaultsPreserveSourceAndGuard(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"approval.namespace", "approval.mkdir", "manifest.temp_sync", "manifest.rename", "manifest.parent_sync", "current.temp_sync", "current.rename", "current.parent_sync"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			p := recoverytest.Accepted(owner.paths(), "fault")
			digest, err := model.DigestAccepted(p)
			mustJournal(t, err)
			a := model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
			injected := errors.New("interrupted durable publication")
			owner.boundary = func(at string) error {
				if at == point {
					return injected
				}
				return nil
			}
			j, err := Create(t.Context(), owner, p, a)
			if !errors.Is(err, injected) {
				t.Fatal("fault boundary not exercised", err)
			}
			owner.boundary = nil
			for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
				data, err := os.ReadFile(filepath.Join(owner.system.path, name))
				if err != nil || string(data) != name {
					t.Fatal("approval changed source", name, err)
				}
			}
			if point == "approval.namespace" || point == "approval.mkdir" || point == "manifest.temp_sync" || point == "manifest.rename" {
				if j != nil {
					t.Fatal("unpublished approval was adopted")
				}
				rows, err := List(t.Context(), owner.paths())
				mustJournal(t, err)
				if len(rows) != 0 {
					t.Fatal("unpublished approval became history")
				}
				mustJournal(t, CheckStartup(t.Context(), owner.paths()))
			} else {
				if j == nil {
					t.Fatal("published manifest was lost")
				}
				if err := CheckStartup(t.Context(), owner.paths()); !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal("approval failed to guard startup", err)
				}
			}
			retried, err := Create(t.Context(), owner, p, a)
			mustJournal(t, err)
			if j != nil && retried.View().AcceptedAt != j.View().AcceptedAt {
				t.Fatal("retry changed acceptance time")
			}
			current, err := Read(t.Context(), owner.paths(), "")
			mustJournal(t, err)
			if current.Request.RecoveryID != "fault" || current.BackupName != retried.View().BackupName {
				t.Fatal("retry did not reconcile pointer")
			}
		})
	}
}

func TestCancellationHistoryAndMatchingPointerClearing(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	j, p, a := acceptedJournal(t, owner, "cancel", true)
	mustJournal(t, j.SetPhase(t.Context(), model.Files, nil))
	mustJournal(t, j.MarkFiles(t.Context(), model.FilesBackup, "running", 1))
	audit := model.ActionAudit{Actor: "operator", Reason: "restore application files", At: owner.clock()}
	mustJournal(t, j.BeginCancellation(t.Context(), audit))
	if err := j.CompleteCancellation(t.Context(), audit.At); err == nil {
		t.Fatal("attempted backup cancelled without Undo")
	}
	mustJournal(t, j.MarkFiles(t.Context(), model.FilesUndo, "running", 1))
	mustJournal(t, j.MarkFiles(t.Context(), model.FilesUndo, "complete", 1))
	mustJournal(t, j.CompleteCancellation(t.Context(), audit.At.Add(time.Second)))
	mustJournal(t, CheckStartup(t.Context(), owner.paths()))
	injected := errors.New("pointer clear interrupted")
	owner.boundary = func(at string) error {
		if at == "current.rename" {
			return injected
		}
		return nil
	}
	if err := j.ClearCurrent(t.Context()); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	owner.boundary = nil
	current, err := Read(t.Context(), owner.paths(), "")
	mustJournal(t, err)
	if current.Phase != model.Cancelled {
		t.Fatal("stale pointer reactivated cancellation")
	}
	mustJournal(t, j.ClearCurrent(t.Context()))
	if _, err := Read(t.Context(), owner.paths(), ""); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("empty pointer guessed history", err)
	}
	if _, err := Create(t.Context(), owner, p, a); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("cancelled ID reused", err)
	}
	next, _, _ := acceptedJournal(t, owner, "next", false)
	mustJournal(t, j.ClearCurrent(t.Context()))
	current, err = Read(t.Context(), owner.paths(), "")
	mustJournal(t, err)
	if current.Request.RecoveryID != next.View().Request.RecoveryID {
		t.Fatal("old cancellation cleared another operation")
	}
	rows, err := List(t.Context(), owner.paths())
	mustJournal(t, err)
	if len(rows) != 2 || rows[0].Request.RecoveryID != "cancel" || rows[1].Request.RecoveryID != "next" {
		t.Fatal("historical ID/tie ordering lost", rows)
	}
	changed := audit
	changed.Actor = "another operator"
	if err := j.BeginCancellation(t.Context(), changed); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("cancellation audit changed", err)
	}
}

func TestPhasePublicationReopenAndConcurrentReaders(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	j, _, _ := acceptedJournal(t, owner, "progress", false)
	initial := j.View()
	initial.Resources.MaxGroupBytes++
	if err := j.Update(t.Context(), initial); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("immutable settings edited", err)
	}
	injected := errors.New("phase parent sync interrupted")
	owner.boundary = func(at string) error {
		if at == "manifest.parent_sync" {
			return injected
		}
		return nil
	}
	if err := j.SetPhase(t.Context(), model.Files, nil); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	owner.boundary = nil
	reopened, err := Open(t.Context(), owner, "progress")
	mustJournal(t, err)
	if reopened.View().Phase != model.Files {
		t.Fatal("uncertain atomic phase was not reconciled")
	}
	ctx, cancel := context.WithCancel(t.Context())
	var readers sync.WaitGroup
	failures := make(chan error, 1)
	readers.Go(func() {
		for ctx.Err() == nil {
			rows, err := List(ctx, owner.paths())
			if err != nil && !errors.Is(err, context.Canceled) {
				select {
				case failures <- err:
				default:
				}
				return
			}
			for _, row := range rows {
				if err := model.ValidateManifest(row); err != nil {
					select {
					case failures <- err:
					default:
					}
					return
				}
			}
		}
	})
	for range 10 {
		mustJournal(t, reopened.SetPhase(t.Context(), model.Files, nil))
	}
	cancel()
	readers.Wait()
	select {
	case err := <-failures:
		t.Fatal("reader observed partial manifest", err)
	default:
	}
	mustJournal(t, reopened.MarkFiles(t.Context(), model.FilesBackup, "complete", 0))
	mustJournal(t, reopened.SetPhase(t.Context(), model.Archiving, nil))
	if err := reopened.BeginCancellation(t.Context(), model.ActionAudit{Actor: "operator", Reason: "too late", At: owner.clock()}); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("durable archive boundary ignored", err)
	}
	for _, entry := range reopened.View().Archive {
		mustJournal(t, reopened.MarkArchive(t.Context(), entry.Root, entry.Name))
	}
	mustJournal(t, reopened.SetPhase(t.Context(), model.AwaitingStart, nil))
	if err := reopened.SetPhase(t.Context(), model.Importing, nil); err == nil {
		t.Fatal("successor files could precede marker")
	}
	mustJournal(t, reopened.MarkSuccessorStarted(t.Context(), owner.clock()))
	first := *reopened.View().SuccessorStartedAt
	mustJournal(t, reopened.MarkSuccessorStarted(t.Context(), owner.clock().Add(time.Hour)))
	if !reopened.View().SuccessorStartedAt.Equal(first) {
		t.Fatal("successor marker changed")
	}
	mustJournal(t, reopened.SetPhase(t.Context(), model.Importing, nil))
	mustJournal(t, reopened.SetPhase(t.Context(), model.Validating, nil))
	mustJournal(t, reopened.SetPhase(t.Context(), model.Complete, nil))
	if !model.StatusFromManifest(reopened.View()).Complete {
		t.Fatal("metadata-only handoff omitted completion")
	}
}

func TestResetJournalHasNoReplaySeedsAndRecordsExactJoin(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	p := recoverytest.Accepted(owner.paths(), "reset")
	p.Request.Mode = model.ResetForJoin
	p.Request.ClusterID = p.Identity.ClusterID
	p.Identity.MemberID = 0
	p.Sources = nil
	p.Approved.Groups = append(p.Approved.Groups, topology.Group{ID: "work", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	digest, err := model.DigestAccepted(p)
	mustJournal(t, err)
	j, err := Create(t.Context(), owner, p, model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true})
	mustJournal(t, err)
	if len(j.View().Seeds) != 0 {
		t.Fatal("reset created replay work")
	}
	mustJournal(t, j.SetPhase(t.Context(), model.Files, nil))
	mustJournal(t, j.MarkFiles(t.Context(), model.FilesBackup, "complete", 0))
	mustJournal(t, j.SetPhase(t.Context(), model.Archiving, nil))
	for _, entry := range j.View().Archive {
		mustJournal(t, j.MarkArchive(t.Context(), entry.Root, entry.Name))
	}
	mustJournal(t, j.SetPhase(t.Context(), model.JoinReady, nil))
	if !model.StatusFromManifest(j.View()).Complete {
		t.Fatal("join preparation not reported complete")
	}
	next := j.View()
	next.Phase = model.Joined
	next.JoinedIdentity = &topology.Identity{NodeName: "n1", ClusterID: p.Identity.ClusterID, MemberID: 99, InstanceID: "fresh"}
	if err := j.Update(t.Context(), next); err == nil {
		t.Fatal("reset join preceded durable successor marker")
	}
	mustJournal(t, j.MarkSuccessorStarted(t.Context(), owner.clock()))
	next = j.View()
	next.Phase = model.Joined
	next.JoinedIdentity = &topology.Identity{NodeName: "n1", ClusterID: p.Identity.ClusterID, MemberID: 99, InstanceID: "fresh"}
	mustJournal(t, j.Update(t.Context(), next))
	status := model.StatusFromManifest(j.View())
	if status.Identity.MemberID != 99 || status.Identity.InstanceID != "" || !status.Complete {
		t.Fatal(status)
	}
	next = j.View()
	next.JoinedIdentity.MemberID++
	if err := j.Update(t.Context(), next); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("joined identity changed", err)
	}
}
