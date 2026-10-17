//go:build functional

package managed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/storage"
)

func TestCollectionPreparesBeforeDeletionAndIgnoresLaterRetentionChanges(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var policyCalls, collects atomic.Int32
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		return lifecycle.RetentionDecision{Collect: policyCalls.Add(1) == 1}, nil
	}
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(ctx context.Context, retired storage.RetiredReplica) error {
			row, err := f.cfg.Authority().Collection(ctx, retired.Identity)
			if err != nil || !row.Found || row.Record.State != "prepared" || row.Record.Retired != retired {
				return errors.New("deletion ran without exact durable preparation")
			}
			if collects.Add(1) == 1 {
				return fault.ErrUnavailable
			}
			return f.store.Collect(ctx, retired)
		}}
	h, retired := retiredHostCollection(t, f)
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	row, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
	must(t, err)
	if !row.Found || row.Record.State != "prepared" || row.Record.CollectedAt != nil || collects.Load() != 1 {
		t.Fatal("failed deletion was completed or retried during the same scan", row, collects.Load())
	}
	h.Fence() // Accepted local deletion remains available to the excluded host.
	must(t, h.CollectRetired(deadline(t)))
	got, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
	must(t, err)
	if policyCalls.Load() != 1 || collects.Load() != 2 || got.Record.State != "collected" ||
		got.Record.Sequence != row.Record.Sequence {
		t.Fatal("prepared attempt reran retention or changed sequence", got)
	}
}

func TestCollectionDeferralAndPolicyFailurePreserveUnpreparedData(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"deferred", "not before", "callback error", "status error"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			future := time.Now().Add(time.Hour).UTC()
			var calls atomic.Int32
			f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
				calls.Add(1)
				decision := lifecycle.RetentionDecision{Reason: "hold for inspection"}
				if mode == "not before" {
					decision.Collect, decision.NotBefore = true, future
				}
				if mode == "callback error" {
					return decision, fault.ErrUnavailable
				}
				return decision, nil
			}
			statuses := make(chan CleanupStatus, 1)
			f.cfg.Collection.Status = func(_ context.Context, status CleanupStatus) error {
				statuses <- status
				if mode == "status error" {
					return fault.ErrUnavailable
				}
				return nil
			}
			h, retired := retiredHostCollection(t, f)
			err := h.CollectRetired(deadline(t))
			if (mode == "callback error" || mode == "status error") != errors.Is(err, fault.ErrUnavailable) {
				t.Fatal(err)
			}
			status := <-statuses
			if status.Identity != retired.Identity || status.MemberID != 1 || status.RetirementKey.Revision != 11 ||
				status.Blocked == nil || status.Blocked.Code != "retention" || calls.Load() != 1 ||
				mode == "not before" && (status.NotBefore == nil || !status.NotBefore.Equal(future)) {
				t.Fatal("deferred status lost exact proof or policy decision", status)
			}
			row, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
			must(t, err)
			left, err := f.store.Retired(deadline(t))
			must(t, err)
			if row.Found || len(left) != 1 || left[0] != retired {
				t.Fatal("unaccepted deletion changed durable evidence", row, left)
			}
		})
	}
}

func TestCollectionRequiresCompleteRetirementBeforePolicy(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	fixtureAuthority(t, f)
	var calls atomic.Int32
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		calls.Add(1)
		return lifecycle.RetentionDecision{Collect: true}, nil
	}
	must(t, f.store.BindReplica(deadline(t), f.localID(1)))
	must(t, f.store.Ensure(deadline(t), "work"))
	_, err := f.store.Quarantine(deadline(t), "work", 1)
	must(t, err)
	h := f.host(t)
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("quarantine alone authorized collection", err)
	}
	left, err := f.store.Retired(deadline(t))
	must(t, err)
	if calls.Load() != 0 || len(left) != 1 {
		t.Fatal("unverified retirement reached policy or deleted data")
	}
}

func TestCollectionRechecksLedgerAfterRetentionWait(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		f.mu.Lock()
		f.records["work"] = metastore.ReplicaRecord{ClusterID: f.node.ClusterID, NodeName: f.node.NodeName, ReplicaID: 1}
		f.mu.Unlock()
		return lifecycle.RetentionDecision{Collect: true}, nil
	}
	h, retired := retiredHostCollection(t, f)
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("collection ignored ledger reappearance while policy waited", err)
	}
	row, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
	must(t, err)
	left, err := f.store.Retired(deadline(t))
	must(t, err)
	if row.Found || len(left) != 1 {
		t.Fatal("conflicting ledger did not preserve unprepared evidence")
	}
}

func TestCollectionPreparationFailureCannotDelete(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var collects atomic.Int32
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		return lifecycle.RetentionDecision{Collect: true}, f.cfg.Authority().Close()
	}
	f.cfg.Retirement = &collectionStore{RetirementStore: f.store,
		collect: func(context.Context, storage.RetiredReplica) error {
			collects.Add(1)
			return nil
		}}
	h, retired := retiredHostCollection(t, f)
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("failed journal preparation was ignored", err)
	}
	left, err := f.store.Retired(deadline(t))
	must(t, err)
	if collects.Load() != 0 || len(left) != 1 || left[0] != retired {
		t.Fatal("preparation failure reached deletion", collects.Load(), left)
	}
}

func TestRetentionCancellationKeepsCallbackOwnedUntilActualReturn(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.cfg.Retention = func(ctx context.Context, _ lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		close(entered)
		<-release
		return lifecycle.RetentionDecision{Collect: true}, ctx.Err()
	}
	h, retired := retiredHostCollection(t, f)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.CollectRetired(ctx) }()
	waitSignal(t, entered)
	cancel()
	stop, stopCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer stopCancel()
	if err := h.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("callback ownership ended before actual return", err)
	}
	once.Do(func() { close(release) })
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, h.Stop(deadline(t)))
	row, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
	must(t, err)
	if row.Found {
		t.Fatal("canceled retention prepared deletion")
	}
}
