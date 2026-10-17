//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaWatchSeparatesLatestAndTarget(t *testing.T) {
	t.Parallel()
	groups := singleMetaFixture(t)
	g := readyPublic(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	channels := map[topology.View]<-chan topology.Update{}
	for _, view := range []topology.View{topology.Latest, topology.Target} {
		ch, err := g.Watch(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		channels[view] = ch
		u := receiveUpdate(t, ch)
		if u.View != view || u.Document.Version != 1 {
			t.Fatal(u)
		}
	}
	doc := g.state().Latest()
	doc.Version++
	doc.ParentVersion = 1
	doc.Comment = "watch staging"
	if _, err := g.Stage(ctx, doc, 1); err != nil {
		t.Fatal(err)
	}
	if u := receiveUpdate(t, channels[topology.Latest]); u.Document.Version != 2 {
		t.Fatal(u)
	}
	assertWatchQuiet(t, channels[topology.Target])
	ex, err := g.Approve(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.WaitOperation(ctx, ex); err != nil {
		t.Fatal(err)
	}
	for _, view := range []topology.View{topology.Target} {
		if u := receiveUpdate(t, channels[view]); u.View != view || u.Document.Version != 2 {
			t.Fatal(u)
		}
	}
	for _, view := range []topology.View{topology.Latest, topology.Target} {
		assertWatchQuiet(t, channels[view])
	}
}

func TestMetaStopJoinsAndClosesWatchers(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ch, err := g.Watch(context.Background(), topology.Latest)
	if err != nil {
		t.Fatal(err)
	}
	receiveUpdate(t, ch)
	if err := g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("stop left watch open")
	}
	if _, err := g.Watch(context.Background(), topology.Latest); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if doc, err := g.Latest(context.Background()); err != nil || doc.Version != 1 {
		t.Fatal("stop lost owned document reads", doc, err)
	}
}

func TestMetaWatchRejectsCanceledAndInvalidView(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Watch(ctx, topology.Latest); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, view := range []topology.View{"active", "other"} {
		if _, err := g.Watch(context.Background(), view); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(view, err)
		}
	}
}
