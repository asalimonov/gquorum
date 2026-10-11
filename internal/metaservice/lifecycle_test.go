package metaservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestQuiesceBeforeStartRetainsStoreUntilStop(t *testing.T) {
	t.Parallel()
	g, err := New(optionsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	if err = g.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = g.store.LoadState(); err != nil {
		t.Fatal("quiescence closed database", err)
	}
	if err = g.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("started after quiescence", err)
	}
	if err = g.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := readStatus(t, g)
	if s.Lifecycle != "quiescent" || s.SessionLive {
		t.Fatal(s)
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = g.store.LoadState(); err == nil {
		t.Fatal("Stop retained owned database")
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestJoinContextCancelsAndJoinsOwnerHook(t *testing.T) {
	t.Parallel()
	for _, cancelOwner := range []bool{false, true} {
		parent, cancelParent := context.WithCancel(context.Background())
		owner, cancel := context.WithCancel(context.Background())
		ctx, cleanup := joinContext(parent, owner)
		if cancelOwner {
			cancel()
		} else {
			cancelParent()
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("missing combined cancellation")
		}
		cleanup()
		cancel()
		cancelParent()
	}
}
func TestLifecycleCallGatesRefuseQuiescentBeforeWait(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &Group{started: true, ctx: parent, workCtx: parent, quiescing: true, rpcClosed: true}
	if _, _, err := g.beginEffect(parent); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if _, _, err := g.beginRPC(parent); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := g.Authorize(parent, topology.AuthorizedStep{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("authorized frozen state", err)
	}
	g.sessionToken = make(chan struct{}, 1)
	if err := g.takeSessionToken(parent); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("session waiter passed quiescence gate", err)
	}
	if len(g.sessionToken) != 0 {
		t.Fatal("refused session leaked serial token")
	}
}
func TestQuiesceAndStopConcurrentBeforeStartJoinOnce(t *testing.T) {
	t.Parallel()
	g, err := New(optionsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			var err error
			if i%2 == 0 {
				err = g.Quiesce(context.Background())
			} else {
				err = g.Stop(context.Background())
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if readStatus(t, g).Lifecycle != "stopped" {
		t.Fatal("final close not visible")
	}
}

func TestSessionWaiterRechecksQuiescenceAfterSerialGate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g := &Group{started: true, ctx: ctx, sessionToken: make(chan struct{}, 1)}
	if err := g.takeSessionToken(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- g.takeSessionToken(ctx) }()
	g.mu.Lock()
	g.quiescing = true
	g.mu.Unlock()
	g.releaseSessionToken()
	if err := <-result; !errors.Is(err, fault.ErrStopped) {
		t.Fatal("session waiter passed the closing gate", err)
	}
	if len(g.sessionToken) != 0 {
		t.Fatal("refused waiter retained token")
	}
}
