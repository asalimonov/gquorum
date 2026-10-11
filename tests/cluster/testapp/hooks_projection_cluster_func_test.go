//go:build functional

package main

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
)

func TestCallerProjectionStallCatchesUpFullStateAndJoins(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	a := &application{cfg: config{NodeName: "node1", DataDir: t.TempDir()}, workCtx: ctx, clusterHooks: newClusterHooks()}
	key := walKey{"work", 7}
	control := &projectionControl{release: make(chan struct{}), metrics: projectionMetrics{Blocked: true}}
	a.clusterHooks.projections[key] = control
	m := newMachine()
	put := func(id string, index uint64) {
		data, err := json.Marshal(putCommand{RequestID: id, Key: id, Value: id})
		if err != nil {
			t.Fatal(err)
		}
		result, err := m.Apply(ctx, load.Command{RequestID: id, Index: index, Data: data})
		if err != nil || result.Code != "ok" {
			t.Fatal(result, err)
		}
	}
	put("before", 1)
	a.workWG.Go(func() { a.projectionLoop(key, m, func() bool { return true }, control) })
	t.Cleanup(func() { cancel(); a.workWG.Wait() })
	deadline, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	projectionPoll(t, deadline, func() bool {
		view, err := a.projectionView(key)
		return err == nil && view.Active == 1 && view.Attempts == 1
	})
	if _, err := os.Stat(a.projectionPath(key)); !os.IsNotExist(err) {
		t.Fatal("blocked projection reached storage", err)
	}
	put("during", 2)
	a.clusterHooks.mu.Lock()
	close(control.release)
	control.release = nil
	control.metrics.Blocked = false
	a.clusterHooks.mu.Unlock()
	projectionPoll(t, deadline, func() bool {
		view, err := a.projectionView(key)
		return err == nil && view.Writes >= 2 && view.State.AppliedIndex == 2 && len(view.State.Requests) == 2 && maps.Equal(view.State.Values, map[string]string{"before": "before", "during": "during"})
	})
	cancel()
	a.workWG.Wait()
	view, err := a.projectionView(key)
	if err != nil || !view.Stopped || view.Active != 0 || view.Failures != 0 {
		t.Fatal("caller worker did not release owned writes", view, err)
	}
}

func TestCallerProjectionBlockedWriterCancellationAndRetirement(t *testing.T) {
	t.Parallel()
	for _, retirement := range []bool{false, true} {
		name := "cancellation"
		if retirement {
			name = "retirement"
		}
		t.Run(name, func(t *testing.T) { blockedProjectionStops(t, retirement) })
	}
}
func blockedProjectionStops(t *testing.T, retirement bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	a := &application{cfg: config{DataDir: t.TempDir()}, workCtx: ctx, clusterHooks: newClusterHooks()}
	key := walKey{"work", 9}
	control := &projectionControl{release: make(chan struct{}), metrics: projectionMetrics{Blocked: true}}
	a.clusterHooks.projections[key] = control
	var running atomic.Bool
	running.Store(true)
	a.workWG.Go(func() { a.projectionLoop(key, newMachine(), running.Load, control) })
	t.Cleanup(func() { cancel(); a.workWG.Wait() })
	deadline, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	projectionPoll(t, deadline, func() bool { view, err := a.projectionView(key); return err == nil && view.Active == 1 })
	if retirement {
		running.Store(false)
	} else {
		cancel()
	}
	projectionPoll(t, deadline, func() bool {
		view, err := a.projectionView(key)
		return err == nil && view.Stopped && view.Active == 0 && view.Failures == 1 && view.Writes == 0
	})
	a.workWG.Wait()
	if _, err := os.Stat(a.projectionPath(key)); !os.IsNotExist(err) {
		t.Fatal("retired projection wrote stale state", err)
	}
}

func projectionPoll(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("projection did not reach required state", ctx.Err())
		}
	}
}
