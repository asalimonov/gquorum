//go:build functional

package managed

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type collectionStore struct {
	storage.RetirementStore
	collect func(context.Context, storage.RetiredReplica) error
}

func (s *collectionStore) Collect(ctx context.Context, record storage.RetiredReplica) error {
	return s.collect(ctx, record)
}

func retiredHostCollection(t *testing.T, f *fixture) (*Host, storage.RetiredReplica) {
	t.Helper()
	_, initial := fixtureAuthority(t, f)
	f.cfg.RetirementInterval = time.Hour
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	f.setScope("work", 2, false, false)
	req := installFixtureHostAction(t, f, h, initial, topology.StepCleanupReplicaState, f.localID(1), 11)
	_, err = h.ApplyAction(deadline(t), req)
	must(t, err)
	rows, err := f.store.Retired(deadline(t))
	must(t, err)
	if len(rows) != 1 {
		t.Fatal("fixture did not retire exact storage", rows)
	}
	return h, rows[0]
}

func preparedHostCollection(t *testing.T, f *fixture) (*Host, authoritystore.CollectionRecord) {
	t.Helper()
	h, retired := retiredHostCollection(t, f)
	journal := f.cfg.Authority()
	proof, err := journal.CompletedRetirement(deadline(t), retired.Identity)
	must(t, err)
	if !proof.Found {
		t.Fatal("retirement did not persist its effect")
	}
	row, err := journal.PrepareCollection(deadline(t), authoritystore.CollectionRecord{Retired: retired,
		MemberID: proof.Record.Target.MemberID, Grant: proof.Record.Grant,
		Retirement: *proof.Record.Receipt, State: "prepared"})
	must(t, err)
	return h, row
}

func TestPreparedCollectionResumesWithoutEnumerationOrRetention(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var retention atomic.Int32
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		retention.Add(1)
		return lifecycle.RetentionDecision{}, fault.ErrUnavailable
	}
	h, row := preparedHostCollection(t, f)
	must(t, f.store.Collect(t.Context(), row.Retired)) // Crash after deletion, before journal completion.
	rows, err := f.store.Retired(t.Context())
	must(t, err)
	if len(rows) != 0 {
		t.Fatal("fixture still enumerates deleted data")
	}
	must(t, h.CollectRetired(t.Context()))
	got, err := f.cfg.Authority().Collection(t.Context(), row.Retired.Identity)
	must(t, err)
	if retention.Load() != 0 || !got.Found || got.Record.State != "collected" ||
		got.Record.Sequence != row.Sequence || got.Record.Retired != row.Retired {
		t.Fatal("prepared resume reran policy or changed proof", got, retention.Load())
	}
}

func TestCollectedJournalNeverRepeatsDeletionAfterMarkerRelease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var calls atomic.Int32
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(context.Context, storage.RetiredReplica) error {
			calls.Add(1)
			return errors.New("collected row must not delete again")
		}}
	h, row := preparedHostCollection(t, f)
	must(t, f.store.Collect(t.Context(), row.Retired))
	_, err := f.cfg.Authority().CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence, time.Now().UTC())
	must(t, err)
	must(t, f.store.ForgetCollection(t.Context(), row.Retired))
	must(t, h.CollectRetired(t.Context()))
	if calls.Load() != 0 {
		t.Fatal("collected row repeated deletion after proof release")
	}
}

func TestPreparedCollectionKeepsEvidenceAfterStoreFailure(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var fail atomic.Bool
	fail.Store(true)
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(ctx context.Context, record storage.RetiredReplica) error {
			if fail.Load() {
				return fault.ErrUnavailable
			}
			return f.store.Collect(ctx, record)
		}}
	h, row := preparedHostCollection(t, f)
	if err := h.CollectRetired(t.Context()); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	got, err := f.cfg.Authority().Collection(t.Context(), row.Retired.Identity)
	must(t, err)
	if got.Record.State != "prepared" || got.Record.CollectedAt != nil || got.Record.Sequence != row.Sequence {
		t.Fatal("failed attempt manufactured completion", got)
	}
	fail.Store(false)
	must(t, h.CollectRetired(t.Context()))
}

func TestCollectionPersistFailureRecoversPreparedProofAfterPhysicalDeletion(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	path := filepath.Join(t.TempDir(), "authority.bolt")
	journal, err := authoritystore.Open(path, f.node, authoritystore.FreshEvidence)
	must(t, err)
	var current atomic.Pointer[authoritystore.Store]
	current.Store(journal)
	f.cfg.Authority = current.Load
	t.Cleanup(func() { must(t, current.Load().Close()) })
	var fail atomic.Bool
	fail.Store(true)
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(ctx context.Context, record storage.RetiredReplica) error {
			if err := f.store.Collect(ctx, record); err != nil {
				return err
			}
			if fail.Swap(false) {
				return current.Load().Close()
			}
			return nil
		}}
	h, row := preparedHostCollection(t, f)
	if err := h.CollectRetired(t.Context()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("failed completion persistence was hidden", err)
	}
	reopened, err := authoritystore.Open(path, f.node, authoritystore.RetainedEvidence)
	must(t, err)
	current.Store(reopened)
	got, err := reopened.Collection(t.Context(), row.Retired.Identity)
	must(t, err)
	if !got.Found || got.Record.State != "prepared" || got.Record.Sequence != row.Sequence {
		t.Fatal("uncertain deletion lost its prepared proof", got)
	}
	must(t, h.CollectRetired(t.Context()))
	got, err = reopened.Collection(t.Context(), row.Retired.Identity)
	must(t, err)
	if got.Record.State != "collected" {
		t.Fatal("exact marker could not resolve uncertain completion", got)
	}
}

func TestCollectionCancellationKeepsCallbackOwnedUntilReturn(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(ctx context.Context, record storage.RetiredReplica) error {
			close(entered)
			<-release
			return f.store.Collect(ctx, record)
		}}
	h, row := preparedHostCollection(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.resumeCollections(ctx) }()
	waitSignal(t, entered)
	cancel()
	stop, end := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer end()
	if err := h.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("shutdown released blocked collection owner", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, h.Stop(deadline(t)))
	got, err := f.cfg.Authority().Collection(t.Context(), row.Retired.Identity)
	must(t, err)
	if !got.Found || got.Record.State != "prepared" {
		t.Fatal("canceled attempt lost durable proof", got)
	}
}
