//go:build functional

//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/internal/metaservice"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
	"github.com/asalimonov/gquorum/topology"
)

type recoveryMetaFixture struct {
	MetaPort
	owner                                                *recoverydisk.Owner
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

func coordinatorFixture(t *testing.T, host HostPort, configure ...func(*model.AcceptedPlan)) (*Coordinator, *recoveryMetaFixture) {
	t.Helper()
	owner, err := recoverydisk.Acquire(t.Context(), model.Paths{StorageDir: t.TempDir(), LoadWALDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	accepted := recoverytest.Accepted(owner.Paths(), "coordinator")
	for _, change := range configure {
		change(&accepted)
	}
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
	meta := &recoveryMetaFixture{owner: owner, manifest: manifest, attempts: map[topology.OperationID]topology.OperationStatus{
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

// A held replay and lost reply belong to this private protocol boundary, rather
// than injection fields on the public Node's concrete implementation.
type heldReplayMeta struct {
	MetaPort
	entered, release chan struct{}
}

func (m *heldReplayMeta) WaitRecoveryReplay(ctx context.Context) error {
	close(m.entered)
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestRecoveryCoordinatorWaitsForReplayBeforeImport(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil)
	meta.importLost = true
	held := &heldReplayMeta{MetaPort: meta, entered: make(chan struct{}), release: make(chan struct{})}
	coordinator.config.Meta = held
	if err := coordinator.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-held.entered
	if coordinator.DataReady() || coordinator.config.Journal.View().Phase != model.AwaitingStart {
		t.Fatal("import crossed replay barrier")
	}
	close(held.release)
	<-coordinator.done
	if coordinator.Error() != nil || !coordinator.DataReady() || len(meta.imported) != 1 {
		t.Fatal("lost reply duplicated import", coordinator.Error(), len(meta.imported))
	}
}

type lostConsumptionMeta struct {
	*recoveryMetaFixture
	calls  int
	refuse bool
}

func (m *lostConsumptionMeta) RecordRecoveredGroup(_ context.Context, p model.GroupConsumedProposal) error {
	m.calls++
	if !m.refuse {
		m.record.Groups[0].Consumed = &model.Consumed{Identity: p.Checkpoint.Identity, Checkpoint: p.Checkpoint.Position, SnapshotDigest: p.Checkpoint.Digest}
	}
	return errors.New("lost committed consumption reply")
}
func TestRecoveryConsumptionLostReplyUsesExactCheckpoint(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, &heldPublication{}, func(p *model.AcceptedPlan) {
		source := p.Sources[0]
		source.GroupID = "work"
		source.Identity.GroupID = "work"
		p.Sources = append(p.Sources, source)
		p.Archive = append(p.Archive, model.ArchiveEntry{Root: "wal", Name: "work", Kind: "directory", Classification: "source", EvidenceDigest: source.Digest})
		group := p.Approved.Groups[0]
		group.ID = "work"
		group.NextReplicaID = 2
		p.Approved.Groups = append(p.Approved.Groups, group)
		p.Floors = append(p.Floors, model.AllocationFloor{GroupID: "work", NextID: 2})
	})
	source := meta.manifest.Sources[1]
	identity := source.Identity
	identity.ClusterID = meta.manifest.Identity.ClusterID
	identity.ReplicaID = 2
	meta.record.Groups = []model.ImportGroup{{GroupID: "work", Source: source, Destination: identity}}
	lost := &lostConsumptionMeta{recoveryMetaFixture: meta}
	coordinator.config.Meta = lost
	proof := model.CheckpointProof{Identity: identity, Position: model.Position{Index: 5, Term: 3}, Digest: strings.Repeat("a", 64)}
	p := model.GroupConsumedProposal{RecoveryID: meta.record.RecoveryID, PlanDigest: meta.record.PlanDigest, GroupID: "work", Checkpoint: proof, ProposalID: "consume"}
	if err := coordinator.recordGroup(t.Context(), p); err != nil || lost.calls != 1 {
		t.Fatal("lost reply repeated consumption", err, lost.calls)
	}
	p.Checkpoint.Digest = strings.Repeat("b", 64)
	lost.refuse = true
	if err := coordinator.recordGroup(t.Context(), p); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("different checkpoint borrowed existing consumption", err)
	}
	if meta.record.Groups[0].Consumed.SnapshotDigest != proof.Digest {
		t.Fatal("conflicting confirmation overwrote proof")
	}

}
func TestRecoveryStartupInheritsAcceptedLimits(t *testing.T) {
	t.Parallel()
	coordinator, meta := coordinatorFixture(t, nil, func(p *model.AcceptedPlan) { p.Rules.Limits.MaxNodes = 4 })
	manifest := coordinator.config.Manifest
	options := metaservice.Options{Identity: manifest.Identity, Limits: topology.Limits{}.WithDefaults(), Founders: []metaservice.FoundingMember{{Node: manifest.Approved.Nodes[0], MemberID: manifest.Identity.MemberID}}}
	journal, err := PrepareStartup(t.Context(), meta.owner, StartupConfig{Meta: &options, StartedAt: manifest.AcceptedAt})
	if err != nil || journal == nil || options.Limits != manifest.Rules.Limits {
		t.Fatal("accepted limits not inherited", options.Limits, err)
	}
	options.Limits = topology.Limits{}.WithDefaults()
	if _, err := PrepareStartup(t.Context(), meta.owner, StartupConfig{Meta: &options, LimitsExplicit: true}); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("conflicting explicit limits accepted", err)
	}
}

type failingRecoveryHost struct {
	HostPort
	failure error
}

func (h *failingRecoveryHost) SetRecovery(context.Context, *model.ImportState, model.LocalManifest) error {
	return h.failure
}
func TestRecoveryCoordinatorRetainsApplicationFailureCause(t *testing.T) {
	t.Parallel()
	failure := errors.New("owned application reconstruction failed")
	coordinator, _ := coordinatorFixture(t, &failingRecoveryHost{failure: failure})
	if err := coordinator.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-coordinator.done
	if !errors.Is(coordinator.Error(), failure) || coordinator.DataReady() || coordinator.config.Journal.View().Failure == nil {
		t.Fatal("application failure lost cause or crossed publication", coordinator.Error())
	}
}
