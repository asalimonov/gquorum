//go:build !functional

package managed

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

func deliveryFixture() (*Host, *replica) {
	slot := &replica{identity: storage.Identity{GroupID: "work"}, ctx: context.Background(), deliveries: make(map[uint64]delivery)}
	return &Host{cfg: Config{EffectTimeout: time.Second}, slots: map[string]*replica{"work": slot}}, slot
}

func TestDeliveryWindowRetainsSnapshotsAndNewestOrdinary(t *testing.T) {
	t.Parallel()
	h, slot := deliveryFixture()
	snapshot, err := h.registerDelivery(slot, delivery{snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	var newest uint64
	for range deliveryWindow * 4 {
		newest, err = h.registerDelivery(slot, delivery{})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(slot.deliveries) != deliveryWindow || !slot.deliveries[snapshot].snapshot {
		t.Fatal("lost snapshot or exceeded window")
	}
	for id := newest - deliveryWindow + 2; id <= newest; id++ {
		if _, ok := slot.deliveries[id]; !ok {
			t.Fatalf("lost recent ordinary delivery %d", id)
		}
	}
}

func TestDeliveryWindowSkipsCompletedTokens(t *testing.T) {
	t.Parallel()
	h, slot := deliveryFixture()
	first, _ := h.registerDelivery(slot, delivery{})
	for range deliveryWindow * 4 {
		id, err := h.registerDelivery(slot, delivery{})
		if err != nil {
			t.Fatal(err)
		}
		// Completion, send failure and expiry all remove the exact map entry.
		delete(slot.deliveries, id)
		if len(slot.deliveryOrder) > 2*deliveryWindow {
			t.Fatal("completed token bookkeeping grew without bound")
		}
	}
	if _, ok := slot.deliveries[first]; !ok {
		t.Fatal("evicted live token before window filled")
	}
	for range deliveryWindow {
		if _, err := h.registerDelivery(slot, delivery{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := slot.deliveries[first]; ok {
		t.Fatal("oldest live token not evicted")
	}
}

func TestDeliveryWindowFullOfSnapshotsRefusesNewDelivery(t *testing.T) {
	t.Parallel()
	h, slot := deliveryFixture()
	for range deliveryWindow {
		if _, err := h.registerDelivery(slot, delivery{snapshot: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.registerDelivery(slot, delivery{}); err != fault.ErrOverloaded {
		t.Fatal(err)
	}
	delete(slot.deliveries, 1)
	if _, err := h.registerDelivery(slot, delivery{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.registerDelivery(slot, delivery{snapshot: true}); err != nil {
		t.Fatal(err)
	}
	if len(slot.deliveries) != deliveryWindow {
		t.Fatal("incorrect window size")
	}
}

func BenchmarkRegisterDelivery(b *testing.B) {
	h, slot := deliveryFixture()
	for range deliveryWindow {
		_, _ = h.registerDelivery(slot, delivery{})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = h.registerDelivery(slot, delivery{})
	}
}
