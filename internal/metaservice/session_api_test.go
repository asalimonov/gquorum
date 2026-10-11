//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestSessionTokenWaitingHonorsContextAndStoppedParent(t *testing.T) {
	t.Parallel()
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	g := &Group{started: true, ctx: parent, sessionToken: make(chan struct{}, 1)}
	if err := g.takeSessionToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := g.takeSessionToken(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	stop()
	if err := g.takeSessionToken(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	g.releaseSessionToken()
}
