//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSelfRemovalCallbackFailureStaysFencedAndIsLocallyObservable(t *testing.T) {
	t.Parallel()
	bus, journal := eventFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := bus.Subscribe(ctx, events.Filter{}, 32)
	if err != nil {
		t.Fatal(err)
	}
	g, fired := removalFixture(t, func(context.Context, string) error { return errors.New("application teardown failed") })
	g.opts.Events = bus
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	awaitRemoval(t, fired)
	g.removalWG.Wait()
	g.mu.RLock()
	status := *g.selfRemoval
	g.mu.RUnlock()
	if !status.Fenced || !status.Notified || status.ErrorCode != "callback_failed" || g.OwnSessionLive() {
		t.Fatal(status)
	}
	if _, err := g.BeginEffect(context.Background(), topology.AuthorizedStep{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("callback failure restored authority", err)
	}
	eventBarrier(t, bus, stream)
	var removal, failure bool
	for _, e := range journalEvents(t, journal, events.Filter{}) {
		if e.Type == events.SelfRemoved {
			removal = e.MemberID == 2 && e.GroupID == "_meta" && e.Version == 4
		}
		if e.Type == events.RuntimeError && e.Code == "self_removal_callback_failed" {
			failure = true
		}
	}
	if !removal || !failure {
		t.Fatal("missing local removal events", removal, failure)
	}
}
