package managed

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
)

func TestReplicaObservationCannotCompleteEffectBeforeStartupPublishes(t *testing.T) {
	functional(t)
	f := newFixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	defer release()
	var h *Host
	scope := f.cfg.Scope
	f.cfg.Scope = func(ctx context.Context, id storage.Identity) (metaengine.LoadScope, error) {
		got, err := scope(ctx, id)
		if err != nil {
			return got, err
		}
		h.mu.Lock()
		slot := h.slots[id.GroupID]
		started := slot != nil && slot.core != nil && slot.core.Running()
		h.mu.Unlock()
		if started {
			close(entered)
			select {
			case <-proceed:
			case <-ctx.Done():
				return got, ctx.Err()
			}
		}
		return got, nil
	}
	h = f.host(t)
	ctx := deadline(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	done := make(chan error, 1)
	go func() { _, err := h.StartReplica(ctx, ref, StartExplicit); done <- err }()
	waitSignal(t, entered)
	query := load.ReplicaQuery{From: f.node, To: load.Peer{Node: f.node, Replica: ref}}
	observed, err := h.ReadReplica(ctx, query)
	must(t, err)
	if observed.Found || observed.Status.Found || observed.LocalState != "starting" {
		t.Fatal("unpublished engine attested a completed effect", observed)
	}
	if _, found := h.Lookup("work"); found {
		t.Fatal("startup reservation published before authorization")
	}
	release()
	must(t, <-done)
	observed, err = h.ReadReplica(ctx, query)
	must(t, err)
	if !observed.Found || !observed.Status.Found || observed.LocalState != "ready" {
		t.Fatal("published replica not observed", observed)
	}
}

// These assertions followed ownership from the former Runtime into Host.
func TestHostRefusesUnstartedAndForeignAssignmentsBeforeStoreMutation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var calls atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		calls.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h, err := New(f.cfg)
	must(t, err)
	defer h.Stop(context.Background())
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	if _, err := h.StartReplica(t.Context(), ref, StartExplicit); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("unstarted owner admitted a replica", err)
	}
	must(t, h.Start(t.Context()))
	f.mu.Lock()
	scope := f.scopes["work"]
	scope.Replica.NodeName = "foreign"
	f.scopes["work"] = scope
	f.mu.Unlock()
	if _, err := h.StartReplica(t.Context(), ref, StartExplicit); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign assignment accepted", err)
	}
	if probe, err := f.store.Probe(t.Context(), "work"); err != nil || probe.Exists || calls.Load() != 0 {
		t.Fatal("refused assignment mutated store or invoked factory", probe, err, calls.Load())
	}
}

func TestHostRegistryAndSharedOwnership(t *testing.T) {
	functional(t)
	f := newFixture(t)
	h := f.host(t)
	ctx := deadline(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	g, err := h.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	duplicate, err := h.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	if duplicate != g {
		t.Fatal("duplicate start replaced exact ownership")
	}
	_, err = h.StartReplica(ctx, load.ReplicaRef{GroupID: "other", ReplicaID: 1}, StartExplicit)
	must(t, err)
	ids := h.IDs()
	if !slices.Equal(ids, []string{"other", "work"}) {
		t.Fatal(ids)
	}
	ids[0] = "changed"
	if h.IDs()[0] != "other" {
		t.Fatal("registry alias")
	}
	if err := h.StopReplica(ctx, load.ReplicaRef{GroupID: "work", ReplicaID: 2}, StopExplicit); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if got, found := h.Lookup("work"); !found || got != g {
		t.Fatal("stale stop unregistered handle")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := h.StopReplica(canceled, ref, StopExplicit); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(h.Held()) != 0 {
		t.Fatal("canceled stop installed a hold")
	}
	must(t, h.StopReplica(ctx, ref, StopExplicit))
	next, err := h.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	if next == g || g.Core().Running() {
		t.Fatal("retained start reused old handle")
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if err := h.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(h.IDs()) != 0 {
		t.Fatal("teardown retained handles")
	}
	if probe, err := f.store.Probe(ctx, "work"); err != nil || !probe.HasLog {
		t.Fatal("host closed or wiped borrowed store", probe, err)
	}
	if _, err := h.StartReplica(ctx, ref, StartExplicit); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestHostForeignWALRetainedUntilExactQuarantine(t *testing.T) {
	functional(t)
	f := newFixture(t)
	ctx := deadline(t)
	foreign := f.localID(99)
	must(t, f.store.BindReplica(ctx, foreign))
	must(t, f.store.Ensure(ctx, "work"))
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	if _, err := h.StartReplica(ctx, ref, StartExplicit); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	if parked := h.Parked()["work"]; parked.Reason != ParkIdentityMismatch {
		t.Fatal(parked)
	}
	if probe, err := f.store.Probe(ctx, "work"); err != nil || !probe.Exists || probe.Identity != foreign {
		t.Fatal("foreign WAL moved or overwritten", probe, err)
	}
	if _, err := f.store.Quarantine(ctx, "work", 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrong quarantine identity accepted", err)
	}
	_, err := f.store.Quarantine(ctx, "work", 99)
	must(t, err)
	_, err = h.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	if len(h.Parked()) != 0 {
		t.Fatal("successful start did not clear parking")
	}
}

type rejectedRestore struct{ stateMachine }

func (*rejectedRestore) Restore(context.Context, []byte) error {
	return errors.New("invalid application snapshot")
}

func TestHostFailedRestoreParksAndRetriesWithoutLosingStore(t *testing.T) {
	functional(t)
	f := newFixture(t)
	var failed atomic.Bool
	var closed atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		var m load.StateMachine = &stateMachine{}
		if failed.CompareAndSwap(false, true) {
			m = &rejectedRestore{}
		}
		return load.Application{StateMachine: m, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	h := f.host(t)
	ctx := deadline(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	if _, err := h.StartReplica(ctx, ref, StartExplicit); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	if h.Parked()["work"].Reason != ParkStoreUnreadable || len(h.IDs()) != 0 || closed.Load() != 1 {
		t.Fatal("failed restore lost ownership or was published", h.Snapshot(), closed.Load())
	}
	if probe, err := f.store.Probe(ctx, "work"); err != nil || !probe.Exists || probe.HasLog {
		t.Fatal(probe, err)
	}
	_, err := h.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	if len(h.Parked()) != 0 {
		t.Fatal("successful retry remained parked")
	}
}

type heldProbe struct {
	storage.Store
	entered, canceled, release chan struct{}
}

func (s *heldProbe) Probe(ctx context.Context, id string) (storage.Probe, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.canceled)
	<-s.release // model a dependency that takes time to finish cancellation cleanup
	return s.Store.Probe(ctx, id)
}

func TestHostStopDuringProbeRetainsOwnershipAndJoins(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	held := &heldProbe{Store: f.store, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	f.cfg.Store = held
	h := f.host(t)
	var once sync.Once
	release := func() { once.Do(func() { close(held.release) }) }
	defer release()
	ctx := deadline(t)
	opened := make(chan error, 1)
	go func() {
		_, err := h.StartReplica(ctx, load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
		opened <- err
	}()
	waitSignal(t, held.entered)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := h.Stop(canceled); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if len(h.IDs()) != 0 || len(h.Snapshot()) != 1 {
		t.Fatal("in-flight constructor ownership lost", h.Snapshot())
	}
	if _, err := h.StartReplica(ctx, load.ReplicaRef{GroupID: "other", ReplicaID: 1}, StartExplicit); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	go func() { joined <- h.Stop(ctx) }()
	waitSignal(t, held.canceled)
	release()
	if err := <-opened; !errors.Is(err, context.Canceled) && !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	must(t, <-joined)
	if len(h.IDs()) != 0 {
		t.Fatal("constructor published during close")
	}
	if _, err := f.store.Probe(ctx, "work"); err != nil {
		t.Fatal("borrowed store closed", err)
	}
}

func TestParkedLoadOwnedObservationAndTypedError(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	bus, err := events.NewBus(events.BusOptions{})
	must(t, err)
	defer bus.Close(context.Background())
	f.cfg.Events = bus
	h, err := New(f.cfg)
	must(t, err)
	stream, err := bus.Subscribe(t.Context(), events.Filter{Types: []events.Type{events.RuntimeError}}, 4)
	must(t, err)
	slot := &replica{identity: f.localID(1), node: f.node}
	err = h.park(slot, ParkStoreMissing, fault.ErrUnreadable)
	var parked *ParkedError
	if !errors.As(err, &parked) || !errors.Is(err, fault.ErrReplicaNotReady) || !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	copy := h.Parked()
	original := copy["work"]
	delete(copy, "work")
	_ = h.park(slot, ParkStoreMissing, nil)
	if h.Parked()["work"] != original {
		t.Fatal("repeat replaced identity or timestamp")
	}
	select {
	case delivery := <-stream:
		if delivery.Event.InstanceID != f.node.InstanceID || delivery.Event.ReplicaID != 1 || delivery.Event.Code != "store_missing" {
			t.Fatal(delivery)
		}
	case <-deadline(t).Done():
		t.Fatal("no parked event")
	}
	must(t, h.Stop(deadline(t)))
	if !bus.Publish(events.Event{Type: events.NodeAlive}) {
		t.Fatal("host closed borrowed bus")
	}
}
