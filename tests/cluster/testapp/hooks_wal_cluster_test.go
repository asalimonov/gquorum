//go:build !functional

package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

type recordingWAL struct {
	storage.Store
	writes atomic.Int64
}

func (s *recordingWAL) SaveReady(context.Context, string, storage.ReadyWrite) error {
	s.writes.Add(1)
	return nil
}
func TestControlledWALCancellationAndExactDurableAccounting(t *testing.T) {
	t.Parallel()
	h := newClusterHooks()
	a := &application{clusterHooks: h}
	base := &recordingWAL{}
	wrapped := a.clusterStore(base, "work", 7)
	key := walKey{"work", 7}
	h.mu.Lock()
	h.wals[key].release = make(chan struct{})
	h.wals[key].counters.Blocked = true
	h.wals[key].counters.RemainingFailures = 2
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	joined := make(chan error, 1)
	write := storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}}
	go func() { joined <- wrapped.SaveReady(ctx, "work", write) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for h.walMetrics(key).Active != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("write did not enter real adapter gate")
		}
	}
	if base.writes.Load() != 0 {
		t.Fatal("blocked write reached durability before release")
	}
	cancel()
	if err := <-joined; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled writer did not join", err)
	}
	h.mu.Lock()
	close(h.wals[key].release)
	h.wals[key].release = nil
	h.wals[key].counters.Blocked = false
	h.mu.Unlock()
	for range 2 {
		if err := wrapped.SaveReady(t.Context(), "work", write); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("transient fault was lost", err)
		}
	}
	if err := wrapped.SaveReady(t.Context(), "work", write); err != nil {
		t.Fatal(err)
	}
	metrics := h.walMetrics(key)
	if metrics.Attempts != 4 || metrics.EntryAttempts != 4 || metrics.Failures != 3 || metrics.Active != 0 || metrics.Entries != 2 || metrics.EntryWrites != 1 || metrics.RemainingFailures != 0 || base.writes.Load() != 1 {
		t.Fatal("failed/canceled writes polluted durable accounting or retained owned calls", metrics, base.writes.Load())
	}
	if err := wrapped.SaveReady(t.Context(), "other", write); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrapper wrote another group", err)
	}
	if h.walMetrics(walKey{"work", 8}) != (walCounters{}) {
		t.Fatal("replacement replica inherited fault state")
	}
}
