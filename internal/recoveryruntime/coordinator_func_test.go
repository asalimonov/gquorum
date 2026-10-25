//go:build functional

package recoveryruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
	"github.com/asalimonov/gquorum/topology"
)

type recoveryMetaFixture struct {
	MetaPort
	manifest                                             model.LocalManifest
	record                                               model.ImportState
	attempts                                             map[topology.OperationID]topology.OperationStatus
	retried                                              []topology.OperationID
	importLost, completeLost                             bool
	pendingReads, unavailableReads, unavailableCompletes int
	imported                                             []model.ImportProposal
	importUnavailable                                    bool
	absentRead                                           bool
}

func (m *recoveryMetaFixture) Identity() topology.Identity {
	identity := m.manifest.Identity
	identity.InstanceID = "test-process"
	return identity
}
func (*recoveryMetaFixture) WaitRecoveryReplay(context.Context) error { return nil }
func (m *recoveryMetaFixture) ImportRecovery(_ context.Context, proposal model.ImportProposal) (topology.OperationID, error) {
	m.imported = append(m.imported, proposal)
	if m.importUnavailable {
		m.importUnavailable = false
		m.absentRead = true
		return "", fault.ErrUnavailable
	}
	if m.importLost {
		return "", errors.New("lost import reply")
	}
	return topology.MakeOperationID(2, 1), nil
}
func (m *recoveryMetaFixture) ReadRecovery(context.Context) (*model.ImportState, error) {
	if m.absentRead {
		m.absentRead = false
		return nil, nil
	}
	if m.unavailableReads > 0 {
		m.unavailableReads--
		return nil, fault.ErrUnavailable
	}
	owned := model.CloneImport(m.record)
	return &owned, nil
}
func (m *recoveryMetaFixture) Operation(_ context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	if status, found := m.attempts[id]; found {
		if m.pendingReads > 0 {
			m.pendingReads--
			status.State = topology.OperationRunning
		}
		return status, nil
	}
	return topology.OperationStatus{}, fault.ErrHistoryUnavailable
}
func (m *recoveryMetaFixture) RetryOperation(_ context.Context, id topology.OperationID) (topology.OperationID, error) {
	m.retried = append(m.retried, id)
	previous := m.attempts[id]
	next := topology.OperationStatus{Version: previous.Version, Attempt: previous.Attempt + 1, State: topology.OperationSucceeded}
	next.ID = topology.MakeOperationID(next.Version, next.Attempt)
	m.attempts[next.ID] = next
	return next.ID, nil
}
func (m *recoveryMetaFixture) CompleteRecovery(_ context.Context, proposal model.CompleteImportProposal) error {
	if m.unavailableCompletes > 0 {
		m.unavailableCompletes--
		return fault.ErrUnavailable
	}
	m.record.Phase, m.record.CompletedAt = "complete", &proposal.At
	if m.completeLost {
		return errors.New("lost completion reply")
	}
	return nil
}

func coordinatorFixture(t *testing.T, host HostPort) (*Coordinator, *recoveryMetaFixture) {
	t.Helper()
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	accepted := recoverytest.Accepted(owner.Paths(), "coordinator")
	digest, err := model.DigestAccepted(accepted)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := recoverydisk.Create(t.Context(), owner, accepted, model.Acceptance{
		PlanDigest: digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []model.Phase{model.Files, model.Archiving} {
		if err := journal.SetPhase(t.Context(), phase, nil); err != nil {
			t.Fatal(err)
		}
		if phase == model.Files {
			if err := journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, entry := range journal.View().Archive {
		if err := journal.MarkArchive(t.Context(), entry.Root, entry.Name); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.SetPhase(t.Context(), model.AwaitingStart, nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkSuccessorStarted(t.Context(), journal.View().AcceptedAt); err != nil {
		t.Fatal(err)
	}
	manifest := journal.View()
	requestDigest, err := model.ImportRequestDigest(model.ImportFromManifest(manifest))
	if err != nil {
		t.Fatal(err)
	}
	meta := &recoveryMetaFixture{manifest: manifest, attempts: map[topology.OperationID]topology.OperationStatus{
		topology.MakeOperationID(2, 1): {ID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, State: topology.OperationSucceeded},
	}, record: model.ImportState{Source: manifest.Source, RequestDigest: requestDigest, Schema: 1, RecoveryID: manifest.Request.RecoveryID, PlanDigest: manifest.PlanDigest,
		Survivor: model.ImportFromManifest(manifest).Survivor, Version: 2, Phase: "importing"}}
	coordinator, err := New(Config{Manifest: manifest, Journal: journal, Meta: meta, Host: host})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return coordinator, meta
}

func awaitRecoveryData(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	if err := coordinator.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-coordinator.done:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if err := coordinator.Error(); err != nil || !coordinator.DataReady() {
		t.Fatal(err)
	}
}

func TestRecoveryCoordinatorConfirmsLostRepliesBeforeDataAndLocalCompletion(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	meta.importLost, meta.completeLost = true, true
	awaitRecoveryData(t, coordinator)
	if coordinator.config.Journal.View().Phase != model.Validating {
		t.Fatal("replicated completion became local completion")
	}
	if err := coordinator.CommitLocalCompletion(t.Context(), meta.Identity()); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("completion preceded publication/readiness", err)
	}
	if err := coordinator.ReadyToPublish(t.Context(), meta.Identity()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitLocalCompletion(t.Context(), meta.Identity()); err != nil {
		t.Fatal(err)
	}
	if coordinator.config.Journal.View().Phase != model.Complete {
		t.Fatal("completion was not durable")
	}
}

func TestRecoveryCoordinatorFollowsRetainedRetriesAndCreatesOnlyOneSuccessor(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	for attempt := uint32(1); attempt <= 2; attempt++ {
		id := topology.MakeOperationID(2, attempt)
		meta.attempts[id] = topology.OperationStatus{ID: id, Version: 2, Attempt: attempt, State: topology.OperationFailed}
	}
	awaitRecoveryData(t, coordinator)
	if len(meta.retried) != 1 || meta.retried[0] != topology.MakeOperationID(2, 2) {
		t.Fatal(meta.retried)
	}
}

type heldPublication struct {
	HostPort
	entered, cancelled, release chan struct{}
	runs                        atomic.Int32
}

func (*heldPublication) SetRecovery(context.Context, *model.ImportState, model.LocalManifest) error {
	return nil
}
func (h *heldPublication) PublishRecovery(ctx context.Context, _ topology.Identity) error {
	close(h.entered)
	<-ctx.Done()
	close(h.cancelled)
	<-h.release // Deliberately late return keeps its owner through cancellation.
	return nil
}
func (h *heldPublication) ReleaseRecoveryRun(context.Context, topology.Identity) error {
	h.runs.Add(1)
	return nil
}

func TestRecoveryCoordinatorStopCancelsAndJoinsLatePublicationWithoutRun(t *testing.T) {
	t.Parallel()
	host := &heldPublication{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	coordinator, meta := coordinatorFixture(t, host)
	awaitRecoveryData(t, coordinator)
	published := make(chan error, 1)
	go func() { published <- coordinator.ReadyToPublish(t.Context(), meta.Identity()) }()
	<-host.entered
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := coordinator.Stop(cancelled); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("late publication detached", err)
	}
	<-host.cancelled
	close(host.release)
	if err := <-published; !errors.Is(err, fault.ErrStopped) {
		t.Fatal("late success published after stop", err)
	}
	if err := coordinator.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if host.runs.Load() != 0 || coordinator.config.Journal.View().Phase != model.Validating {
		t.Fatal("stop released Run")
	}
}

func TestRecoveryCoordinatorRejectsConflictingReadBackAfterLostImportReply(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	meta.importLost = true
	meta.record.RequestDigest = "different accepted import"
	if err := coordinator.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-coordinator.done
	if coordinator.Error() == nil || coordinator.DataReady() || coordinator.config.Journal.View().Phase != model.Importing {
		t.Fatal("conflicting prior import escaped recovery barrier")
	}
}

func TestRecoveryCoordinatorRetriesTransientControlAndWaitsForAsyncOperation(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	meta.importUnavailable, meta.unavailableReads, meta.unavailableCompletes, meta.pendingReads = true, 2, 2, 3
	awaitRecoveryData(t, coordinator)
	if len(meta.imported) != 2 {
		t.Fatal("transient import did not retry exact proposal", len(meta.imported))
	}
	first, _ := model.ImportRequestDigest(meta.imported[0])
	second, _ := model.ImportRequestDigest(meta.imported[1])
	if first != second || len(meta.retried) != 0 {
		t.Fatal("control retries changed identity or created factory attempts")
	}
}

func TestRecoveryCoordinatorRetryLatchRejectsAnotherFailedFactoryAttempt(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	first := topology.MakeOperationID(2, 1)
	status := meta.attempts[first]
	status.State = topology.OperationFailed
	meta.attempts[first] = status
	successor, err := coordinator.resumeExistingOperation(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	status = meta.attempts[successor]
	status.State = topology.OperationFailed
	meta.attempts[successor] = status
	if _, err := coordinator.resumeExistingOperation(t.Context(), first); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("failed factory entered automatic retry loop", err)
	}
	if len(meta.retried) != 1 {
		t.Fatal(meta.retried)
	}
}
