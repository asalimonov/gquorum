//go:build functional

package managed

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestApplicationRunPublishedAndJoinedBeforeClose(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, canceled, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce atomic.Bool
	unblock := func() {
		if releaseOnce.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	var callbacks atomic.Int32
	var h *Host
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Run: func(ctx context.Context, g *load.Group) error {
			callbacks.Add(1)
			handle, ok := h.Lookup("work")
			if !ok || (*load.Group)(handle) != g {
				return errors.New("callback preceded exact handle publication")
			}
			watch, err := g.Watch(ctx)
			if err != nil {
				return err
			}
			observation := <-watch
			if !observation.Ready || observation.Identity != g.Identity() {
				return errors.New("callback preceded replay readiness")
			}
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return ctx.Err()
		}, Close: func(context.Context) error { close(closed); return nil }}, nil
	}
	h = f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	handle, err := h.StartReplica(t.Context(), ref, StartExplicit)
	must(t, err)
	waitSignal(t, entered)
	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err = h.StopReplica(short, ref, StopExplicit)
	cancel()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	waitSignal(t, canceled)
	select {
	case <-closed:
		t.Fatal("Close ran before Run returned")
	default:
	}
	if _, err := h.StartReplica(t.Context(), ref, StartExplicit); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("replacement overlapped old callback", err)
	}
	if callbacks.Load() != 1 || handle.Core().Running() {
		t.Fatal("callback duplicated or engine not stopped")
	}
	unblock()
	must(t, h.StopReplica(t.Context(), ref, StopExplicit))
	waitSignal(t, closed)
}

func TestApplicationRunFailureHoldsExactReplicaAndExplicitRetry(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	trigger := make(chan struct{})
	var callbacks, closes atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Run: func(ctx context.Context, _ *load.Group) error {
			if callbacks.Add(1) == 1 {
				select {
				case <-trigger:
					return errors.New("worker broke")
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			<-ctx.Done()
			return ctx.Err()
		}, Close: func(context.Context) error { closes.Add(1); return nil }}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	old, err := h.StartReplica(t.Context(), ref, StartExplicit)
	must(t, err)
	close(trigger)
	deadline := time.After(5 * time.Second)
	for {
		h.mu.Lock()
		held := h.holds[old.Identity()]
		slot := h.slots["work"]
		h.mu.Unlock()
		if held && slot == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("failed Run was not released and held")
		case <-time.After(time.Millisecond):
		}
	}
	status := h.Snapshot()
	if len(status) != 1 || status[0].State != "failed" || !strings.Contains(status[0].Error, "worker broke") || status[0].RunID != old.Core().RunID() {
		t.Fatal(status)
	}
	if _, err := h.StartReplica(t.Context(), ref, StartRecovery); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("recovery bypassed failure hold", err)
	}
	if closes.Load() != 1 {
		t.Fatal("failed run did not release once", closes.Load())
	}
	next, err := h.StartReplica(t.Context(), ref, StartExplicit)
	must(t, err)
	if old == next || old.Core().RunID() == next.Core().RunID() {
		t.Fatal("retry reused stopped engine")
	}
	must(t, h.StopReplica(t.Context(), ref, StopExplicit))
	if closes.Load() != 2 {
		t.Fatal("retry resources not released", closes.Load())
	}
}

func TestApplicationRunNilKeepsReplicaAndEngineFailureCancelsRun(t *testing.T) {
	t.Parallel()
	for _, nilReturn := range []bool{true, false} {
		t.Run(map[bool]string{true: "completion", false: "engine failure"}[nilReturn], func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			entered, returned := make(chan struct{}), make(chan struct{})
			f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
				return load.Application{StateMachine: &stateMachine{}, Run: func(ctx context.Context, _ *load.Group) error {
					close(entered)
					defer close(returned)
					if nilReturn {
						return nil
					}
					<-ctx.Done()
					return ctx.Err()
				}}, nil
			}
			h := f.host(t)
			handle, err := h.StartReplica(t.Context(), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
			must(t, err)
			waitSignal(t, entered)
			if nilReturn {
				waitSignal(t, returned)
				if !handle.Core().Running() {
					t.Fatal("nil Run stopped consensus")
				}
			} else {
				handle.Core().Fail(errors.New("injected engine failure"))
				waitSignal(t, returned) // no periodic recovery tick is needed for cancellation
			}
		})
	}
}

func TestApplicationRunWaitsForStartupAuthorizationAfterReplay(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var runs, closes atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Run: func(context.Context, *load.Group) error { runs.Add(1); return nil }, Close: func(context.Context) error { closes.Add(1); return nil }}, nil
	}
	h := f.host(t)
	must(t, h.reconcileStartupAuthority(t.Context(), load.ReplicaRef{GroupID: "work", ReplicaID: 1}))
	slot, err := h.acceptGuardedStart(t.Context(), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit, func(context.Context) error {
		f.mu.Lock()
		_, recorded := f.records["work"]
		f.mu.Unlock()
		// Recording precedes core.Start/WaitReady. The final authorization guard
		// must reject publication even though replay already finished successfully.
		if recorded {
			return fault.ErrOperatorAuthorization
		}
		return nil
	})
	must(t, err)
	if _, err := h.waitStarted(t.Context(), slot); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal(err)
	}
	if runs.Load() != 0 || closes.Load() != 1 {
		t.Fatal("unauthorized startup ran application or leaked resources", runs.Load(), closes.Load())
	}
}

func TestManagedLeadershipReplicasResolveNodeNames(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	observed := make(chan []load.ReplicaProgress, 1)
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Run: func(ctx context.Context, g *load.Group) error {
			return g.RunWhileLeader(ctx, func(scope context.Context, l *load.Leadership) error {
				replicas, err := l.Replicas(scope)
				if err != nil {
					return err
				}
				observed <- replicas
				return nil
			})
		}, Close: func(context.Context) error { return nil }}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	handle, err := h.StartReplica(t.Context(), ref, StartExplicit)
	must(t, err)
	var replicas []load.ReplicaProgress
	select {
	case replicas = <-observed:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if len(replicas) != 1 || replicas[0].ReplicaID != 1 || replicas[0].NodeName != f.node.NodeName || !replicas[0].Self || !replicas[0].Voter {
		t.Fatal("unexpected managed progress", replicas)
	}
	// Remote names come from the host's catalog resolver.
	if name := handle.Core().PeerName(t.Context(), 2); name != "b" {
		t.Fatal("managed engine did not resolve peer names", name)
	}
	must(t, h.StopReplica(t.Context(), ref, StopExplicit))
}
