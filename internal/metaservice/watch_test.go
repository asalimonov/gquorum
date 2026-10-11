//go:build !functional

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

func TestMetaWatchSeedIsNotRepeatedByLateApply(t *testing.T) {
	t.Parallel()
	r := newWatchRegistry(2)
	defer r.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seed := topology.Document{Version: 3}
	ch, err := r.watch(ctx, topology.Latest, func() topology.Document { return seed })
	if err != nil {
		t.Fatal(err)
	}
	if receiveUpdate(t, ch).Document.Version != 3 {
		t.Fatal("seed")
	}
	r.publish(topology.Document{Version: 2}, topology.Document{}, topology.Document{})
	r.publish(seed, topology.Document{}, topology.Document{})
	assertWatchQuiet(t, ch)
	r.publish(topology.Document{Version: 4}, topology.Document{}, topology.Document{})
	if receiveUpdate(t, ch).Document.Version != 4 {
		t.Fatal("seed repeated")
	}
}

func TestMetaWatchDropOldestKeepsNewestWithoutBlocking(t *testing.T) {
	t.Parallel()
	r := newWatchRegistry(1)
	defer r.close()
	ch, err := r.watch(context.Background(), topology.Latest, func() topology.Document { return topology.Document{} })
	if err != nil {
		t.Fatal(err)
	}
	for v := uint64(1); v <= 128; v++ {
		r.publish(topology.Document{Version: v}, topology.Document{}, topology.Document{})
	}
	if len(ch) != 1 || receiveUpdate(t, ch).Document.Version != 128 {
		t.Fatal("watch did not coalesce newest")
	}
}

func TestMetaWatchCancelAndBound(t *testing.T) {
	t.Parallel()
	r := newWatchRegistry(2)
	defer r.close()
	ctx, cancel := context.WithCancel(context.Background())
	drop, err := r.watch(ctx, topology.Latest, func() topology.Document { return topology.Document{} })
	if err != nil {
		t.Fatal(err)
	}
	keep, err := r.watch(context.Background(), topology.Latest, func() topology.Document { return topology.Document{} })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.watch(context.Background(), topology.Latest, func() topology.Document { return topology.Document{} }); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-drop:
		if ok {
			t.Fatal("canceled watch still open")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not close watch")
	}
	r.publish(topology.Document{Version: 1}, topology.Document{}, topology.Document{})
	receiveUpdate(t, keep)
	if _, err := r.watch(context.Background(), topology.Active, func() topology.Document { return topology.Document{} }); err != nil {
		t.Fatal("canceled watcher retained capacity", err)
	}
}

func TestMetaWatchConcurrentSubscribePublishCancelAndClose(t *testing.T) {
	t.Parallel()
	r := newWatchRegistry(256)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch, err := r.watch(ctx, topology.Latest, func() topology.Document { return topology.Document{Version: 1} })
			if err != nil && !errors.Is(err, fault.ErrStopped) {
				t.Error(err)
			}
			if err == nil {
				for range ch {
				}
			}
		})
	}
	for v := uint64(2); v < 128; v++ {
		r.publish(topology.Document{Version: v}, topology.Document{}, topology.Document{})
	}
	r.close()
	wg.Wait()
	r.close()
}
