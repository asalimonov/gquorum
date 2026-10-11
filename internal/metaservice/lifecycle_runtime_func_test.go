//go:build functional

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

// Delays completion after observing cancellation to prove ownership/joining,
// rather than merely checking that a cancel signal was sent.
type slowEffectController struct {
	verifiedLoadController
	entered, canceled, release, returned chan struct{}
	once                                 sync.Once
}

func (c *slowEffectController) CreateGroup(ctx context.Context, a topology.AuthorizedStep) error {
	if _, err := c.authorized(ctx, a); err != nil {
		return err
	}
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	close(c.returned)
	return ctx.Err()
}
func TestMetaQuiesceJoinsEffectBeforeReturningAndKeepsOwnedState(t *testing.T) {
	c := &slowEffectController{verifiedLoadController: verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	groups := publicFoundingClusterWith(t, 1, func(o *Options) { o.LoadGroups = c })
	g := readyPublic(t, groups)
	c.mu.Lock()
	c.meta = groups
	c.mu.Unlock()
	// Ensure fixture cleanup can release the callback after any assertion failure.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(c.release) }) }
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := g.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{ExpectedVersion: 1, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.entered:
	case <-ctx.Done():
		t.Fatal("effect never started")
	}
	short, done := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = g.Quiesce(short)
	done()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("quiescence did not wait for effect completion", err)
	}
	select {
	case <-c.canceled:
	default:
		t.Fatal("effect not canceled")
	}
	select {
	case <-c.returned:
		t.Fatal("fixture effect returned before release")
	default:
	}
	if _, err := g.store.LoadState(); err != nil {
		t.Fatal("timeout closed owned state", err)
	}
	if _, err = g.Stage(ctx, r.Document, 2); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("accepted mutation during quiescence", err)
	}
	if err = g.Authorize(ctx, topology.AuthorizedStep{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("accepted external effect authorization during quiescence", err)
	}
	release()
	if err = g.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.returned:
	default:
		t.Fatal("effect outlived successful quiescence")
	}
	if _, err := g.store.LoadState(); err != nil {
		t.Fatal("successful quiescence closed database", err)
	}
	if s := readStatus(t, g); s.Lifecycle != "quiescent" || s.SessionLive {
		t.Fatal(s)
	}
	if err = g.WaitReady(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("quiescent handle appeared ready", err)
	}
	if _, err = g.Activate(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("quiescent handle activated", err)
	}
	if err = g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.store.LoadState(); err == nil {
		t.Fatal("Stop did not close owned state")
	}
}
func TestMetaQuiesceAcknowledgesOwnSessionRevocation(t *testing.T) {
	g := readyPublic(t, publicFoundingCluster(t, 1))
	s, err := g.Activate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = g.AcquireLock(context.Background(), "application", s.ID); err != nil {
		t.Fatal(err)
	}
	if err = g.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, exists := g.state().Session(s.ID); exists {
		t.Fatal("quiescence did not commit graceful revoke")
	}
	if _, held, err := g.LockHolder(context.Background(), "application"); err != nil || held {
		t.Fatal(held, err)
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestMetaStartRacingWithStopHasNoLateWorkers(t *testing.T) {
	for range 12 {
		g, err := New(optionsForTest(t))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() { <-start; result <- g.Start(context.Background()) }()
		close(start)
		if err = g.Stop(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		err = <-result
		if err != nil && !errors.Is(err, fault.ErrStopped) {
			cancel()
			t.Fatal(err)
		}
		if g.engine.Load().Running() {
			cancel()
			t.Fatal("late worker started after stop")
		}
		if _, err := g.store.LoadState(); err == nil {
			cancel()
			t.Fatal("database remained open")
		}
		cancel()
	}
}
