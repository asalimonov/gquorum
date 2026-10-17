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
	r.publish(topology.ReadView{Latest: topology.Document{Version: 2}})
	r.publish(topology.ReadView{Latest: seed})
	assertWatchQuiet(t, ch)
	r.publish(topology.ReadView{Latest: topology.Document{Version: 4}})
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
		r.publish(topology.ReadView{Latest: topology.Document{Version: v}})
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
	r.publish(topology.ReadView{Latest: topology.Document{Version: 1}})
	receiveUpdate(t, keep)
	if _, err := r.watch(context.Background(), topology.Target, func() topology.Document { return topology.Document{} }); err != nil {
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
		r.publish(topology.ReadView{Latest: topology.Document{Version: v}})
	}
	r.close()
	wg.Wait()
	r.close()
}

func TestMetaWatchPublishesCapturedRebasePair(t *testing.T) {
	t.Parallel()
	r := newWatchRegistry(2)
	defer r.close()
	channels := make(map[topology.View]<-chan topology.Update)
	for _, view := range []topology.View{topology.Latest, topology.Target} {
		ch, err := r.watch(t.Context(), view, func() topology.Document { return topology.Document{} })
		if err != nil {
			t.Fatal(err)
		}
		channels[view] = ch
	}
	views := topology.ReadView{AppliedIndex: 42,
		Target: topology.Document{Version: 11, ParentVersion: 9, Comment: "repair"},
		Latest: topology.Document{Version: 12, ParentVersion: 11, RebasedFrom: 10, Comment: "preserved intent"}}
	r.publish(views)
	views.Latest.Comment = "caller mutation"
	latest := receiveUpdate(t, channels[topology.Latest])
	target := receiveUpdate(t, channels[topology.Target])
	if latest.Document.ParentVersion != target.Document.Version || latest.Document.RebasedFrom != 10 ||
		latest.Document.Comment != "preserved intent" || target.Document.Comment != "repair" {
		t.Fatal("watch mixed or aliased the captured pair", latest, target)
	}
	r.publish(topology.ReadView{Target: topology.Document{Version: 10}, Latest: topology.Document{Version: 11}})
	assertWatchQuiet(t, channels[topology.Latest])
	assertWatchQuiet(t, channels[topology.Target])
}
