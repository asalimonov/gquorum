//go:build functional

package metaservice

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaWatchSeparatesLatestTargetAndActive(t *testing.T) {
	groups := publicFoundingCluster(t, 1)
	g := readyPublic(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	channels := map[topology.View]<-chan topology.Update{}
	for _, view := range []topology.View{topology.Latest, topology.Target, topology.Active} {
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
	assertWatchQuiet(t, channels[topology.Active])
	ex, err := g.Apply(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.WaitOperator(ctx, ex); err != nil {
		t.Fatal(err)
	}
	for _, view := range []topology.View{topology.Target, topology.Active} {
		if u := receiveUpdate(t, channels[view]); u.View != view || u.Document.Version != 2 {
			t.Fatal(u)
		}
	}
	for _, view := range []topology.View{topology.Latest, topology.Target, topology.Active} {
		assertWatchQuiet(t, channels[view])
	}
}

func TestMetaWatchFanoutIsOwnedAndDurablyAppliedOnFollowers(t *testing.T) {
	for _, size := range []int{3, 5, 7} {
		t.Run(strconv.Itoa(size)+"-nodes", func(t *testing.T) {
			testMetaWatchFanout(t, size)
		})
	}
}

func testMetaWatchFanout(t *testing.T, size int) {
	t.Helper()
	groups := publicFoundingClusterWith(t, size, func(o *Options) { o.Limits.MaxNodes = size })
	boot, cancelBoot := context.WithTimeout(t.Context(), 15*time.Second)
	for _, node := range groups {
		if err := node.WaitReady(boot); err != nil {
			cancelBoot()
			t.Fatal("watch fixture readiness", node.Identity(), node.LocalStatus(), err)
		}
	}
	cancelBoot()
	var g *Group
	for _, node := range groups {
		if node.IsLeader() {
			g = node
			break
		}
	}
	if g == nil {
		t.Fatal("watch fixture has no meta leader")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := map[topology.View][]<-chan topology.Update{}
	for _, node := range groups {
		for _, view := range []topology.View{topology.Latest, topology.Target, topology.Active} {
			ch, err := node.Watch(ctx, view)
			if err != nil {
				t.Fatal(err)
			}
			if u := receiveUpdate(t, ch); u.View != view || u.Document.Version != 1 {
				t.Fatal("watch lost its seed scope", u)
			}
			streams[view] = append(streams[view], ch)
		}
	}
	doc := g.state().Latest()
	doc.Version++
	doc.ParentVersion = 1
	doc.Nodes[0].Labels = map[string]string{"rack": "one"}
	if _, err := g.Stage(ctx, doc, 1); err != nil {
		t.Fatal("watch staging", err)
	}
	assertUpdate := func(i int, view topology.View) {
		t.Helper()
		u := receiveUpdate(t, streams[view][i])
		if u.View != view || u.Document.Version != 2 || u.Document.Nodes[0].Labels["rack"] != "one" {
			t.Fatal("watch lost approved scope/content", u)
		}
		// Delivery follows the materialized/application transaction, including
		// follower state. Subscriber mutations cannot touch any other observer.
		data, err := groups[i].store.LoadState()
		if err != nil || len(data) == 0 {
			t.Fatal(err)
		}
		persisted := metaengine.NewMachine(groups[i].rules, nil)
		if err := persisted.Restore(ctx, data); err != nil {
			t.Fatal(err)
		}
		stored := map[topology.View]topology.Document{topology.Latest: persisted.Latest(), topology.Target: persisted.Target(), topology.Active: persisted.Active()}[view]
		if stored.Version != u.Document.Version || stored.Nodes[0].Labels["rack"] != "one" {
			t.Fatal("watch preceded durable follower application", err)
		}
		u.Document.Nodes[0].Labels["rack"] = "changed"
		current := groups[i].state().Latest()
		if current.Nodes[0].Labels["rack"] != "one" {
			t.Fatal("watch aliased FSM")
		}
	}
	for i := range groups {
		assertUpdate(i, topology.Latest)
		assertWatchQuiet(t, streams[topology.Target][i])
		assertWatchQuiet(t, streams[topology.Active][i])
	}
	ex, err := g.Apply(ctx, 2)
	if err != nil {
		t.Fatal("watch approval", err)
	}
	if _, err := g.WaitOperator(ctx, ex); err != nil {
		t.Fatal("watch execution", err)
	}
	for i := range groups {
		assertUpdate(i, topology.Target)
		assertUpdate(i, topology.Active)
		for _, view := range []topology.View{topology.Latest, topology.Target, topology.Active} {
			assertWatchQuiet(t, streams[view][i])
		}
	}
}

func TestMetaStopJoinsAndClosesWatchers(t *testing.T) {
	g := readyPublic(t, publicFoundingCluster(t, 1))
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
	g := readyPublic(t, publicFoundingCluster(t, 1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Watch(ctx, topology.Latest); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := g.Watch(context.Background(), topology.View("other")); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
