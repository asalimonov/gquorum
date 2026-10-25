//go:build !functional

package loadsnapshot

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
)

func TestServiceConstructionDoesNotResolve(t *testing.T) {
	t.Parallel()
	options, err := (snapshot.Options{MaxSnapshotBytes: 4 << 20}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	want := options
	calls := 0
	service := NewService(options, func() (*Coordinator, error) {
		calls++
		return nil, fault.ErrReplicaNotReady
	})
	options.MaxSnapshotBytes = 1
	observed := service.Options()
	observed.MaxIncoming = 99
	if calls != 0 || service.Options() != want {
		t.Fatal("constructor/options resolved resources or borrowed mutable settings")
	}
	if page := service.List(snapshot.Query{}); len(page.Items) != 0 || calls != 1 {
		t.Fatal("pre-host observation must be empty", page, calls)
	}
}

func TestServiceRefusesUnattachedCoordinator(t *testing.T) {
	t.Parallel()
	for _, resolve := range []Resolver{nil, func() (*Coordinator, error) { return nil, nil },
		func() (*Coordinator, error) { return new(Coordinator), nil }} {
		service := NewService(snapshot.Options{}, resolve)
		assertServiceRefusal(context.Background(), t, service, fault.ErrReplicaNotReady)
		select {
		case <-service.SessionDone(snapshot.Session{}):
		default:
			t.Fatal("unknown session signal must be closed")
		}
	}
}

func TestCanceledServiceWorkDoesNotResolve(t *testing.T) {
	t.Parallel()
	service := NewService(snapshot.Options{}, func() (*Coordinator, error) {
		panic("canceled work resolved an owner")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertServiceRefusal(ctx, t, service, context.Canceled)
}

func TestServiceFallbackNeverAdmitsWork(t *testing.T) {
	t.Parallel()
	coordinator := new(Coordinator)
	gone := false
	facade := NewService(snapshot.Options{}, func() (*Coordinator, error) {
		if gone {
			return nil, fault.ErrStopped
		}
		return coordinator, nil
	}).(*service)
	if facade.forObservation() != coordinator {
		t.Fatal("current coordinator not retained for observations")
	}
	gone = true
	if facade.forObservation() != coordinator {
		t.Fatal("terminal observations lost their last coordinator")
	}
	assertServiceRefusal(context.Background(), t, facade, fault.ErrStopped)
	select {
	case <-facade.SessionDone(snapshot.Session{}):
	default:
		t.Fatal("stopped owner must not borrow an old session signal")
	}
}

func TestServiceConcurrentObservations(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	coordinator := new(Coordinator)
	facade := NewService(snapshot.Options{}, func() (*Coordinator, error) {
		calls.Add(1)
		return coordinator, nil
	})
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			_ = facade.List(snapshot.Query{})
			_ = facade.Options()
			<-facade.SessionDone(snapshot.Session{})
		})
	}
	workers.Wait()
	if calls.Load() != 32 {
		t.Fatal("observations bypassed current-owner resolution", calls.Load())
	}
}

func assertServiceRefusal(ctx context.Context, t *testing.T, service snapshot.Service, want error) {
	t.Helper()
	calls := []func() error{
		func() error { _, err := service.Begin(ctx, snapshot.BeginRequest{}); return err },
		func() error { _, err := service.Write(ctx, snapshot.Chunk{}); return err },
		func() error { _, err := service.Finish(ctx, snapshot.Session{}); return err },
		func() error { return service.Detach(ctx, snapshot.Session{}) },
		func() error { return service.Abort(ctx, snapshot.Session{}) },
	}
	for _, call := range calls {
		if err := call(); !errors.Is(err, want) {
			t.Fatalf("expected refusal %v; got %v", want, err)
		}
	}
}

type legacyTransport struct{ load.Transport }
type capableTransport struct{ load.Transport }

func (*capableTransport) SendSnapshot(context.Context, snapshot.Send) (snapshot.Ack, error) {
	panic("capability detection called the transport")
}

func TestRequireTransportIsPureAndRejectsTypedNil(t *testing.T) {
	t.Parallel()
	for _, input := range []load.Transport{nil, legacyTransport{}, (*capableTransport)(nil)} {
		if capable, err := RequireTransport(input); capable != nil || !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("missing/typed-nil capability accepted", capable, err)
		}
	}
	input := &capableTransport{}
	capable, err := RequireTransport(input)
	if err != nil || capable != input {
		t.Fatal("real capability lost", capable, err)
	}
}

func TestRememberedSourceCoordinatorRetainsOnlyObservations(t *testing.T) {
	t.Parallel()
	options, err := (snapshot.Options{}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	c := NewCoordinator(options, Ports{})
	c.observe(snapshot.Status{Direction: "outgoing", Delivery: snapshot.DeliveryFailed, SnapshotIndex: 42})
	view := NewService(options, func() (*Coordinator, error) { return nil, fault.ErrStopped })
	RememberCoordinator(view, c)
	page := view.List(snapshot.Query{})
	if len(page.Items) != 1 || page.Items[0].SnapshotIndex != 42 {
		t.Fatal("source-only history lost", page)
	}
	if _, err = view.Begin(t.Context(), snapshot.BeginRequest{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("observation fallback admitted work", err)
	}
}
