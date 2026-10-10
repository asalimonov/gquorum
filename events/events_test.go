package events

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

func testJournal(t *testing.T, opts JournalOptions) *Journal {
	t.Helper()
	if opts.Path == "" {
		opts.Path = filepath.Join(t.TempDir(), "journal.bolt")
	}
	j, err := OpenJournal(opts)
	must(t, err)
	t.Cleanup(func() { must(t, j.Close()) })
	return j
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func appendTest(t *testing.T, j *Journal, e Event) Event {
	t.Helper()
	result, err := j.Append(e)
	must(t, err)
	return result
}
func listTest(t *testing.T, j *Journal, q Query) []Event {
	t.Helper()
	result, err := j.List(context.Background(), q)
	must(t, err)
	return result
}
func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestOpen_CreatesExplicitStoreAndSurvivesReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "caller-name.db")
	j := testJournal(t, JournalOptions{Path: path})
	e := appendTest(t, j, Event{Type: LeaderChange, NodeName: "a", InstanceID: "boot", ClusterID: "cluster", GroupID: "_meta"})
	info, err := os.Stat(path)
	must(t, err)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("permissions %o", info.Mode().Perm())
	}
	if !j.db.NoSync {
		t.Fatal("journal should be best effort")
	}
	must(t, j.Close())
	j = testJournal(t, JournalOptions{Path: path})
	if got := listTest(t, j, Query{}); len(got) != 1 || got[0].Seq != e.Seq || got[0].InstanceID != "boot" {
		t.Fatalf("reopen: %+v", got)
	}
	if next := appendTest(t, j, Event{Type: NodeAlive}); next.Seq != e.Seq+1 {
		t.Fatal("sequence reset on reopen")
	}
}
func TestJournal_AppendListOrdering(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	for range 5 {
		e := appendTest(t, j, Event{Type: NodeAlive, Seq: 999})
		if e.At.IsZero() || e.At.Location() != time.UTC {
			t.Fatal("missing UTC observer time")
		}
	}
	got := listTest(t, j, Query{})
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("local order: %+v", got)
		}
	}
}
func TestJournal_KeysetPagination(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	for i := range 8 {
		typ := NodeAlive
		if i%2 == 1 {
			typ = NodeDead
		}
		appendTest(t, j, Event{Type: typ, GroupID: "load"})
	}
	first := listTest(t, j, Query{Limit: 3})
	second := listTest(t, j, Query{AfterSeq: first[2].Seq, Limit: 3})
	if len(second) != 3 || second[0].Seq != 4 || second[2].Seq != 6 {
		t.Fatalf("page gap: %+v %+v", first, second)
	}
	filtered := listTest(t, j, Query{Filter: Filter{GroupID: "load", Types: []Type{NodeDead}}, Limit: 2})
	if len(filtered) != 2 || filtered[0].Seq != 2 || filtered[1].Seq != 4 {
		t.Fatalf("filter: %+v", filtered)
	}
}
func TestJournal_AgeRotation(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{Retention: time.Hour})
	now := time.Now().UTC()
	j.now = func() time.Time { return now }
	for range 200 {
		appendTest(t, j, Event{At: now.Add(-2 * time.Hour)})
	}
	appendTest(t, j, Event{At: now})
	appendTest(t, j, Event{At: now.Add(-2 * time.Hour)})
	must(t, j.Prune(context.Background()))
	if got := listTest(t, j, Query{Limit: 500}); len(got) != 1 || !got[0].At.Equal(now) {
		t.Fatalf("expired rows survived multi-page pruning: %d", len(got))
	}
}
func TestJournal_AmortizedRotation(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{Retention: time.Hour})
	for range 63 {
		appendTest(t, j, Event{At: time.Now().Add(-2 * time.Hour)})
	}
	if got := listTest(t, j, Query{}); len(got) != 63 {
		t.Fatalf("pruned before rotation: %d", len(got))
	}
	appendTest(t, j, Event{Type: NodeAlive})
	if got := listTest(t, j, Query{}); len(got) != 1 {
		t.Fatalf("rotation did not prune age: %d", len(got))
	}
}
func TestJournal_SizeCapRotation(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{MaxLiveBytes: 1 << 20})
	for range 100 {
		appendTest(t, j, Event{Type: NodeAlive})
	}
	j.liveSize = func() int64 { return 2 << 20 }
	must(t, j.Prune(context.Background()))
	if got := listTest(t, j, Query{}); len(got) != 90 || got[0].Seq != 11 {
		t.Fatalf("oldest fraction: count=%d first=%d", len(got), got[0].Seq)
	}
}
func TestRotate_IgnoresFileSizeAndOtherBuckets(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{MaxLiveBytes: 1 << 20})
	must(t, j.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("other"))
		if err != nil {
			return err
		}
		return b.Put([]byte("big"), make([]byte, 2<<20))
	}))
	for range 100 {
		appendTest(t, j, Event{Type: NodeAlive})
	}
	must(t, j.Prune(context.Background()))
	if got := listTest(t, j, Query{}); len(got) != 100 {
		t.Fatal("unrelated bucket affected journal retention")
	}
}
func TestRotate_SizePruneConvergesOnLiveBytes(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{MaxLiveBytes: 8 << 10})
	for range 500 {
		appendTest(t, j, Event{NodeName: "node", Type: NodeAlive})
	}
	var size int
	for range 100 {
		must(t, j.Prune(context.Background()))
		must(t, j.db.View(func(tx *bolt.Tx) error {
			s := tx.Bucket(bucketEvents).Stats()
			size = s.BranchInuse + s.LeafInuse
			return nil
		}))
		if size <= 8<<10 {
			return
		}
	}
	t.Fatalf("live-byte retention did not converge: %d", size)
}
func TestRotate_StatsWalkIsGatedButStillPrunes(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	for range 100 {
		appendTest(t, j, Event{Type: NodeAlive})
	}
	if j.lastLiveBytes != 0 || j.appendedSinceStats <= 0 {
		t.Fatal("small journal measured on every rotation")
	}
	j.opts.MaxLiveBytes = 1
	must(t, j.Prune(context.Background()))
	if j.lastLiveBytes <= 0 || j.appendedSinceStats != 0 {
		t.Fatal("size trigger failed to remeasure")
	}
}
func TestJournal_NilSafeAndAppendAfterClose(t *testing.T) {
	t.Parallel()
	var absent *Journal
	_, err := absent.Append(Event{})
	must(t, err)
	must(t, absent.Close())
	must(t, absent.Prune(context.Background()))
	if got := listTest(t, absent, Query{}); got != nil {
		t.Fatal("nil journal returned events")
	}
	j := testJournal(t, JournalOptions{})
	must(t, j.Close())
	if e, err := j.Append(Event{Seq: 123}); err == nil || e.Seq != 0 {
		t.Fatalf("failed append claimed persistence: %+v %v", e, err)
	}
}

func testBus(t *testing.T, opts BusOptions) *Bus {
	t.Helper()
	b, err := NewBus(opts)
	must(t, err)
	t.Cleanup(func() { must(t, b.Close(deadline(t))) })
	return b
}
func delivery(t *testing.T, ch <-chan Delivery) Delivery {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatal("subscription closed")
		}
		return d
	case <-deadline(t).Done():
		t.Fatal("event delivery timed out")
		return Delivery{}
	}
}
func TestBus_JournalBeforeDeliveryAndOwnedCopies(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	b := testBus(t, BusOptions{Journal: j})
	a, err := b.Subscribe(deadline(t), Filter{}, 4)
	must(t, err)
	c, err := b.Subscribe(deadline(t), Filter{}, 4)
	must(t, err)
	n := 2
	if !b.Publish(Event{Type: LeaderChange, HealthyVoters: &n}) {
		t.Fatal("publish rejected")
	}
	n = 99
	first := delivery(t, a)
	*first.Event.HealthyVoters = 88
	other := delivery(t, c)
	if other.Event.Seq == 0 || *other.Event.HealthyVoters != 2 {
		t.Fatalf("shared or unjournaled delivery: %+v", other)
	}
	if got := listTest(t, j, Query{}); len(got) != 1 || got[0].Seq != other.Event.Seq {
		t.Fatal("subscriber outran journal append")
	}
}
func TestBus_JournalFailureIsLiveOnly(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	b := testBus(t, BusOptions{Journal: j})
	ch, err := b.Subscribe(deadline(t), Filter{}, 2)
	must(t, err)
	must(t, j.Close())
	b.Publish(Event{Seq: 999, Type: RuntimeError})
	if d := delivery(t, ch); d.Event.Seq != 0 {
		t.Fatal("failed journal write had a persisted sequence")
	}
}
func TestBus_BoundedIngressAndSubscriberLoss(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	b := testBus(t, BusOptions{Journal: j, Buffer: 1})
	ch, err := b.Subscribe(deadline(t), Filter{}, 1)
	must(t, err)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() { finished <- j.db.Update(func(tx *bolt.Tx) error { close(entered); <-release; return nil }) }()
	<-entered
	b.Publish(Event{Type: NodeAlive})
	ctx := deadline(t)
	for len(b.in) > 0 {
		select {
		case <-ctx.Done():
			t.Fatal("dispatcher did not consume ingress")
		case <-time.After(time.Millisecond):
		}
	}
	if !b.Publish(Event{Type: NodeAlive}) || b.Publish(Event{Type: NodeAlive}) || b.Dropped() != 1 {
		t.Fatal("ingress bound not enforced")
	}
	close(release)
	must(t, <-finished)
	// Wait until both accepted events have been dispatched into the bounded subscriber.
	for {
		b.mu.Lock()
		pendingLoss := uint64(0)
		for s := range b.subs {
			pendingLoss = s.lost
		}
		b.mu.Unlock()
		if pendingLoss == 1 && len(listTest(t, j, Query{})) >= 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("subscriber did not record overflow")
		case <-time.After(time.Millisecond):
		}
	}
	first := delivery(t, ch)
	if first.LostBefore != 1 {
		t.Fatalf("ingress gap: %+v", first)
	}
	b.Publish(Event{Type: NodeAlive})
	if next := delivery(t, ch); next.LostBefore != 1 {
		t.Fatalf("subscriber gap: %+v", next)
	}
}
func TestBus_FilterCancelAndCloseOwnership(t *testing.T) {
	t.Parallel()
	j := testJournal(t, JournalOptions{})
	b := testBus(t, BusOptions{Journal: j})
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Subscribe(ctx, Filter{GroupID: "x", Types: []Type{NodeDead}}, 4)
	must(t, err)
	b.Publish(Event{GroupID: "y", Type: NodeDead})
	b.Publish(Event{GroupID: "x", Type: NodeAlive})
	b.Publish(Event{GroupID: "x", Type: NodeDead})
	if d := delivery(t, ch); d.Event.Type != NodeDead || d.Event.GroupID != "x" {
		t.Fatal("filter mismatch")
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected filtered event")
		}
	case <-deadline(t).Done():
		t.Fatal("cancelled subscriber was not closed")
	}
	must(t, b.Close(deadline(t)))
	appendTest(t, j, Event{Type: NodeAlive})
	if b.Publish(Event{}) {
		t.Fatal("closed bus accepted event")
	}
	if _, err := b.Subscribe(context.Background(), Filter{}, 1); !errors.Is(err, fault.ErrStopped) {
		t.Fatalf("subscribe after close: %v", err)
	}
}
