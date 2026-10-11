package metaservice

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func removalFixture(t *testing.T, callback func(context.Context, string) error) (*Group, <-chan string) {
	t.Helper()
	root, cancel := context.WithCancel(context.Background())
	work, stop := context.WithCancel(root)
	fired := make(chan string, 4)
	g := &Group{identity: topology.Identity{NodeName: "n2", ClusterID: "cluster", InstanceID: "instance", MemberID: 2}, memberSeen: true, started: true, ctx: root, workCtx: work, workCancel: stop, removalSignal: make(chan struct{}, 1)}
	g.cancel = cancel
	g.quiesced, g.stopped = make(chan struct{}), make(chan struct{})
	g.opts.OnSelfRemoved = func(ctx context.Context, reason string) error {
		fired <- reason
		if callback != nil {
			return callback(ctx, reason)
		}
		return nil
	}
	g.removalWG.Add(1)
	go g.notifyRemoval(root)
	t.Cleanup(func() { cancel(); g.removalWG.Wait() })
	return g, fired
}

func awaitRemoval(t *testing.T, fired <-chan string) string {
	t.Helper()
	select {
	case reason := <-fired:
		return reason
	case <-time.After(time.Second):
		t.Fatal("missing removal notification")
		return ""
	}
}

func refuteRemoval(t *testing.T, g *Group) {
	t.Helper()
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.selfRemoval != nil {
		t.Fatal("unexpected self-removal", g.selfRemoval)
	}
}
