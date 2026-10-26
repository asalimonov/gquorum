//go:build functional

package raftengine

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestLocalSnapshotAllowanceWarningOnlyOnOversizeTransition(t *testing.T) {
	t.Parallel()
	bus, err := events.NewBus(events.BusOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	stream, err := bus.Subscribe(t.Context(), events.Filter{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	g := &Group{opts: Options{NodeName: "source", InstanceID: "process", ClusterID: "cluster", GroupID: "work", ReplicaID: 1, Events: bus, SnapshotPolicy: SnapshotPolicy{MaxSnapshotBytes: 64}}}
	// Existing oversized state must warn at run start too, without waiting for
	// another application snapshot request or a lagging peer.
	g.ms = raft.NewMemoryStorage()
	if err := g.ms.ApplySnapshot(raftpb.Snapshot{Data: make([]byte, 65), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}); err != nil {
		t.Fatal(err)
	}
	g.warnSnapshotSize(64)
	g.warnResidentSnapshotSize()
	g.warnResidentSnapshotSize()
	for _, size := range []uint64{65, 66, 70, 63, 65, 90} {
		g.warnSnapshotSize(size)
	}
	if !bus.Publish(events.Event{Type: events.NodeAlive, Code: "barrier"}) {
		t.Fatal("barrier refused")
	}
	count := 0
	for {
		select {
		case delivery := <-stream:
			if delivery.Event.Code == "barrier" {
				if count != 2 {
					t.Fatal("missing or repeated allowance warning", count)
				}
				return
			}
			count++
			if delivery.Event.Code != "snapshot_size_limit" || delivery.Event.State != "local_snapshot_oversize" || delivery.Event.GroupID != "work" {
				t.Fatal(delivery)
			}
		case <-testContext(t).Done():
			t.Fatal("snapshot warning missing")
		}
	}
}
