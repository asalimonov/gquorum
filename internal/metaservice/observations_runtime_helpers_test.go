package metaservice

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
)

func eventBarrier(t *testing.T, b *events.Bus, ch <-chan events.Delivery) {
	t.Helper()
	const code = "test_dispatch_barrier"
	if !b.Publish(events.Event{Type: events.RuntimeError, Code: code}) {
		t.Fatal("barrier rejected")
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				t.Fatal("subscription closed")
			}
			if d.LostBefore != 0 {
				t.Fatal("lost local events", d)
			}
			if d.Event.Code == code {
				return
			}
		case <-timer.C:
			t.Fatal("dispatch barrier timed out")
		}
	}
}
