//go:build functional

package loadsnapshot

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/load/snapshot"
)

func TestSnapshotRefusalEventsCoalesceAndRateLimit(t *testing.T) {
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
	deliveries, err := bus.Subscribe(t.Context(), events.Filter{}, 32)
	if err != nil {
		t.Fatal(err)
	}
	f := newSendFixture(t, snapshot.Options{MaxSnapshotBytes: 8})
	f.c.mu.Lock()
	f.c.ports.Events = bus
	f.c.mu.Unlock()
	for i := 0; i < 20; i++ {
		_, err = f.c.PrepareSend(f.owner, f.offer)
		requireReceiveReason(t, err, snapshot.ReasonSizeLimit)
	}
	if !bus.Publish(events.Event{Type: events.NodeAlive, Code: "barrier"}) {
		t.Fatal("barrier refused")
	}
	count := 0
	for {
		select {
		case delivery := <-deliveries:
			if delivery.Event.Code == "barrier" {
				goto counted
			}
			count++
			if delivery.Event.Code != "snapshot_size_limit" || delivery.Event.State != "send_refused" || delivery.Event.GroupID != "group" {
				t.Fatal(delivery)
			}
		case <-receiveContext(t).Done():
			t.Fatal("event missing")
		}
	}
counted:
	if count != 1 {
		t.Fatal("repeated refusal event flood", count)
	}
	f.c.mu.Lock()
	f.c.ports.Transport = nil
	f.c.mu.Unlock()
	_, err = f.c.PrepareSend(f.owner, f.offer)
	requireReceiveReason(t, err, snapshot.ReasonUnsupported)
	select {
	case event := <-deliveries:
		if event.Event.Code != "snapshot_unsupported" {
			t.Fatal(event)
		}
	case <-receiveContext(t).Done():
		t.Fatal("reason change event missing")
	}
	// A persistent reason is emitted again only after the repeat interval.
	f.c.observeSendRefusal(f.owner.key, f.offer, &snapshot.TransferError{Reason: snapshot.ReasonUnsupported}, time.Now().Add(time.Minute))
	select {
	case event := <-deliveries:
		if event.Event.Code != "snapshot_unsupported" {
			t.Fatal(event)
		}
	case <-receiveContext(t).Done():
		t.Fatal("minute reminder missing")
	}
}

func TestSnapshotRefusalEventFloodCannotBypassBoundByCacheChurn(t *testing.T) {
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
	deliveries, err := bus.Subscribe(t.Context(), events.Filter{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	f := newSendFixture(t, snapshot.Options{MaxSnapshotBytes: 8})
	f.c.mu.Lock()
	f.c.ports.Events = bus
	f.c.mu.Unlock()
	for i := uint64(2); i < 602; i++ {
		offer := f.offer
		offer.Message.To = i
		offer.Attempt.ReplicaID = i
		_, err = f.c.PrepareSend(f.owner, offer)
		requireReceiveReason(t, err, snapshot.ReasonSizeLimit)
	}
	if !bus.Publish(events.Event{Type: events.NodeAlive, Code: "barrier"}) {
		t.Fatal("barrier refused")
	}
	count := 0
	for {
		select {
		case delivery := <-deliveries:
			if delivery.Event.Code == "barrier" {
				if count != snapshotHistoryLimit {
					t.Fatal("refusal event traffic exceeded bound", count)
				}
				return
			}
			count++
		case <-receiveContext(t).Done():
			t.Fatal("event barrier missing")
		}
	}
}

func TestSnapshotSend_RemoteSizeRefusalRetainsDimensions(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	f.c.ports.Transport = snapshotSendTransportFunc(func(_ context.Context, s snapshot.Send) (snapshot.Ack, error) {
		return snapshot.Ack{}, &snapshot.TransferError{Reason: snapshot.ReasonSizeLimit, Size: s.Manifest.TotalBytes, Limit: 8}
	})
	job := prepareSend(t, f)
	job.Run(t.Context())
	report := awaitSendReport(t, f)
	requireReceiveReason(t, report.err, snapshot.ReasonSizeLimit)
	page := f.c.List(snapshot.Query{})
	if len(page.Items) != 1 {
		t.Fatal(page)
	}
	row := page.Items[0]
	if row.ErrorSize != uint64(len(f.offer.Message.Snapshot.Data)) || row.ErrorLimit != 8 || row.AdmissionRefused || row.Key.DeliveryID == 0 {
		t.Fatal(row)
	}
	if page.SendBytes != 0 || page.Outgoing != 0 {
		t.Fatal("remote refusal retained reservation", page)
	}
}
