//go:build !functional

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
)

func TestSnapshotReceiptCountersSeparateNativeSkipAndFullAcceptance(t *testing.T) {
	t.Parallel()
	app := &application{clusterHooks: newClusterHooks()}
	service := &fixtureSnapshotService{app: app, receipts: make(map[snapshot.Key]fixtureSnapshotReceipt)}
	key := snapshot.Key{DeliveryID: 1, To: load.Peer{Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 1}}}
	skipped := snapshot.Ack{Session: snapshot.Session{Key: key}, State: snapshot.DeliveryNotNeeded}
	if err := service.recordReply(t.Context(), skipped); err != nil {
		t.Fatal(err)
	}
	if service.notNeeded != 1 || app.clusterHooks.snapshots["work"] != 0 {
		t.Fatal("header-only skip counted full data")
	}
	accepted := skipped
	accepted.State = snapshot.DeliveryAccepted
	accepted.NextOffset = 8
	for range 2 {
		if err := service.recordReply(t.Context(), accepted); err != nil {
			t.Fatal(err)
		}
	}
	if app.clusterHooks.snapshots["work"] != 1 {
		t.Fatal("replayed acceptance counted twice")
	}
	lost := errors.New("receipt lost")
	service.faults.reply = func(context.Context, snapshot.Ack) error { return lost }
	accepted.Session.Key.DeliveryID++
	if err := service.recordReply(t.Context(), accepted); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	service.faults.reply = nil
	if err := service.recordReply(t.Context(), accepted); err != nil {
		t.Fatal(err)
	}
	if app.clusterHooks.snapshots["work"] != 2 {
		t.Fatal("lost reply changed accepted handoff count")
	}
	// These counters attest to ingress only. Installation remains observable
	// through the real Node snapshot service and application Restore state.
}
