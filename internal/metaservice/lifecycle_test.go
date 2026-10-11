//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

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
