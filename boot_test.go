package gquorum

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
)

func TestLoadBootCurrentFormatMatrix(t *testing.T) {
	t.Parallel()
	id := storage.Identity{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 7}
	good := storage.Probe{Exists: true, HasLog: true, Identity: id}
	foreign := good
	foreign.Identity.ReplicaID = 99
	empty := good
	empty.HasLog = false
	cases := []struct {
		name     string
		record   uint64
		probe    storage.Probe
		scope    metaservice.LoadReplicaScope
		origin   string
		reason   ParkReason
		previous uint64
	}{
		{name: "fresh", origin: "fresh"},
		{name: "missing known store", record: 7, reason: ParkStoreMissing},
		{name: "fresh reseat", record: 3, origin: "reseat", previous: 3},
		{name: "adopt matching store", probe: good, origin: "adopt"},
		{name: "unrecorded foreign store retained", probe: foreign, reason: ParkIdentityMismatch},
		{name: "resume", record: 7, probe: good, origin: "resume"},
		{name: "replacement landed before ledger", record: 3, probe: good, origin: "reseat", previous: 3},
		{name: "recorded foreign store retained", record: 7, probe: foreign, reason: ParkIdentityMismatch},
		{name: "prepared empty learner resumes", record: 7, probe: empty, scope: metaservice.LoadReplicaScope{Started: true}, origin: "resume"},
		{name: "empty learner adoption", probe: empty, scope: metaservice.LoadReplicaScope{Started: true}, origin: "adopt"},
		{name: "active proof survives lost ledger", scope: metaservice.LoadReplicaScope{Started: true, Established: true}, reason: ParkStoreMissing},
		{name: "active proof refuses empty WAL", probe: empty, scope: metaservice.LoadReplicaScope{Started: true, Established: true}, reason: ParkStoreMissing},
		{name: "replacement already started then lost", record: 3, scope: metaservice.LoadReplicaScope{Started: true}, reason: ParkStoreMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, reason := decideLoadBoot(id, metaservice.ReplicaRecord{ReplicaID: tc.record}, tc.record != 0, tc.probe, tc.scope)
			if reason != tc.reason || reason == "" && (got.origin != tc.origin || got.previous != tc.previous) {
				t.Fatal(got, reason)
			}
		})
	}
}

func TestParkedLoadOwnedObservationAndTypedError(t *testing.T) {
	t.Parallel()
	bus, err := events.NewBus(events.BusOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := bus.Subscribe(ctx, events.Filter{Types: []events.Type{events.RuntimeError}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := New(Options{Events: bus})
	defer r.Close(context.Background())
	opts := raftengine.Options{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 7, InstanceID: "process", Events: bus}
	err = r.parkLoad(opts, ParkStoreMissing, fault.ErrUnreadable)
	var parked *LoadParkedError
	if !errors.As(err, &parked) || !errors.Is(err, fault.ErrReplicaNotReady) || !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	copy := r.ParkedLoads()
	original := copy["work"]
	delete(copy, "work")
	_ = r.parkLoad(opts, ParkStoreMissing, nil)
	if r.ParkedLoads()["work"] != original {
		t.Fatal("repeat replaced identity or timestamp")
	}
	select {
	case delivery := <-stream:
		if delivery.Event.InstanceID != "process" || delivery.Event.ReplicaID != 7 || delivery.Event.Code != "store_missing" {
			t.Fatal(delivery)
		}
	case <-testDeadline(t):
		t.Fatal("no parked event")
	}
	if !bus.Publish(events.Event{Type: events.NodeAlive}) {
		t.Fatal("runtime closed caller bus")
	}
}
