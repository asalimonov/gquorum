//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestCallerProjectionStallDoesNotShedLocalWALThreeNodes(t *testing.T) { projectionStall(t, 3) }
func TestCallerProjectionStallDoesNotShedLocalWALFiveNodes(t *testing.T)  { projectionStall(t, 5) }
func TestCallerProjectionStallDoesNotShedLocalWALSevenNodes(t *testing.T) { projectionStall(t, 7) }

func projectionStall(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"warm": "warm"}
	if err := c.Put(ctx, "node1", "work", "warm", "warm", "warm"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	replicas := created.Document.Group("work").Replicas
	for _, r := range replicas {
		if _, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "PUT", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, r := range replicas {
			p, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "GET", false)
			if err != nil || p.Writes == 0 || !maps.Equal(p.State.Values, expected) {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	var armed []topology.Replica
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	var draining atomic.Bool
	var wg sync.WaitGroup
	t.Cleanup(func() { cancelWorkers(); wg.Wait() })
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, r := range armed {
			if _, err := c.Projection(cleanup, r.NodeName, "work", r.ReplicaID, "DELETE", false); err != nil {
				t.Error("release projection fault", err)
			}
		}
	})
	baseline := map[string]ProjectionObservation{}
	for _, r := range replicas {
		p, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "GET", false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "PUT", true); err != nil {
			t.Fatal(err)
		}
		baseline[r.NodeName] = p
		armed = append(armed, r)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, r := range replicas {
			p, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "GET", false)
			if err != nil || !p.Waiting || p.Attempts <= baseline[r.NodeName].Attempts {
				return false, err
			}
			// A previously admitted file write may finish after the arm call.
			// The single writer is now waiting, so this is a stable disk baseline.
			old := baseline[r.NodeName]
			old.Writes = p.Writes
			baseline[r.NodeName] = old
		}
		return true, nil
	}); err != nil {
		t.Fatal("projection did not enter the durable-write gate", err)
	}
	type workerResult struct {
		values map[string]string
		err    error
	}
	results := make([]workerResult, 8)
	for worker := range results {
		results[worker].values = map[string]string{}
		wg.Go(func() {
			for i := 0; !draining.Load() && workerCtx.Err() == nil; i++ {
				key := fmt.Sprintf("projection-%d-%03d", worker, i)
				if err := c.Put(workerCtx, c.Spec.Nodes[worker%size].Name, "work", key, key, key); err != nil {
					results[worker].err = err
					return
				}
				results[worker].values[key] = key
				timer := time.NewTimer(400 * time.Millisecond)
				select {
				case <-timer.C:
				case <-workerCtx.Done():
					timer.Stop()
					return
				}
			}
		})
	}
	started := time.Now()
	var peak time.Duration
	var samples int
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for time.Since(started) < 20*time.Second {
		for _, r := range replicas {
			local, err := c.Local(ctx, r.NodeName, "work")
			if err != nil || !local.Found || local.WAL.Failures != 0 || local.WAL.Blocked {
				t.Fatal("projection outage reached Raft durability", r.NodeName, local, err)
			}
			peak = max(peak, time.Duration(local.WALStallNS))
			p, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "GET", false)
			if err != nil || !p.Blocked || p.Stopped {
				t.Fatal("projection did not remain frozen", p, err)
			}
			if time.Since(started) >= time.Second && (!p.Waiting || p.Active != 1 || p.Attempts <= baseline[r.NodeName].Attempts || p.Writes != baseline[r.NodeName].Writes || !maps.Equal(p.State.Values, expected)) {
				t.Fatal("projection fault did not reach a real write or changed stored state", p, baseline[r.NodeName])
			}
		}
		samples++
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Finish admitted requests without cancellation; every result enters the
	// closed expected set, including requests that straddle projection recovery.
	draining.Store(true)
	wg.Wait()
	for i, result := range results {
		if result.err != nil || len(result.values) < 20 {
			t.Fatal("healthy Raft shed/failed/short workload during unrelated outage", i, len(result.values), result.err)
		}
		maps.Copy(expected, result.values)
	}
	if samples < 50 || peak >= 500*time.Millisecond {
		t.Fatal("empty/slow WAL observation during projection outage", samples, peak)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	for _, r := range replicas {
		if _, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "DELETE", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, r := range replicas {
			p, err := c.Projection(ctx, r.NodeName, "work", r.ReplicaID, "GET", false)
			if err != nil || p.Blocked || p.Stopped || p.Writes <= baseline[r.NodeName].Writes || p.State.SchemaVersion != 1 || len(p.State.Requests) != len(expected) || !maps.Equal(p.State.Values, expected) {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("caller projections did not catch up automatically", err)
	}
	t.Logf("METRIC nodes=%d projection_stall_ms=20000 workers=8 probe_gap_ms=400 samples=%d peak_wal_stall_ms=%.3f exact_commands=%d shed=0 projection_recovered=true", size, samples, float64(peak)/float64(time.Millisecond), len(expected))
}
