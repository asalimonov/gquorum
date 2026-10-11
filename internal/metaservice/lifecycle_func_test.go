//go:build functional

package metaservice

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/asalimonov/gquorum/fault"
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
