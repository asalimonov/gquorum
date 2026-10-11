//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type writeProfile struct {
	entries, writes uint64
	perReplica      map[string]uint64
}

func TestWALCoalescingPreservesExactCommandsAndRaisesBatchingThreeNodes(t *testing.T) {
	walCoalescing(t, 3)
}
func TestWALCoalescingPreservesExactCommandsAndRaisesBatchingFiveNodes(t *testing.T) {
	walCoalescing(t, 5)
}
func TestWALCoalescingPreservesExactCommandsAndRaisesBatchingSevenNodes(t *testing.T) {
	walCoalescing(t, 7)
}
func walProfile(ctx context.Context, c *Cluster, doc topology.Document) (writeProfile, error) {
	p := writeProfile{perReplica: map[string]uint64{}}
	for _, r := range doc.Group("work").Replicas {
		local, err := c.Local(ctx, r.NodeName, "work")
		if err != nil {
			return p, err
		}
		if !local.Found || local.ReplicaID != r.ReplicaID {
			return p, fmt.Errorf("profile is not the owning replica: %s", r.NodeName)
		}
		p.entries += local.WAL.Entries
		p.writes += local.WAL.EntryWrites
		p.perReplica[r.NodeName] = local.Status.Applied
	}
	return p, nil
}
func walCoalescing(t *testing.T, size int) {
	t.Helper()
	started := time.Now()
	t.Cleanup(func() {
		if elapsed := time.Since(started); elapsed >= 2*time.Minute {
			t.Errorf("coalescing case exceeded two minutes: %s", elapsed)
		}
	})
	profiles := make([]writeProfile, 2)
	for arm, wait := range []time.Duration{0, 5 * time.Millisecond} {
		if !t.Run(fmt.Sprintf("wait-%s", wait), func(t *testing.T) {
			spec := matrixSpec(size)
			spec.WALWriteWait = wait
			spec.SnapshotThreshold = 100000
			ctx, c := performanceClusterSpec(t, spec)
			created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
			if err != nil {
				t.Fatal(err)
			}
			// Keep the durability owner and ingress pattern identical in both arms.
			response, err := c.Request(ctx, "node2", "POST", "/api/groups/work/leader/node1", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				t.Fatal(err)
			}
			if err := Poll(ctx, func() (bool, error) {
				for _, r := range created.Document.Group("work").Replicas {
					local, err := c.Local(ctx, r.NodeName, "work")
					if err != nil || local.Status.LeaderID != replicaID(created.Document, "work", "node1") {
						return false, err
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := c.Put(ctx, "node1", "work", "warm", "warm", "warm"); err != nil {
				t.Fatal(err)
			}
			if err := exactOwnedValues(ctx, c, created.Document, "work", map[string]string{"warm": "warm"}); err != nil {
				t.Fatal(err)
			}

			if err := Poll(ctx, func() (bool, error) {
				for _, r := range created.Document.Group("work").Replicas {
					value, err := c.Get(ctx, r.NodeName, "work", "warm")
					if err != nil {
						return false, err
					}
					local, err := c.Local(ctx, r.NodeName, "work")
					if err != nil {
						return false, err
					}
					if local.Status.Applied < value.Index {
						return false, nil
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			before, err := walProfile(ctx, c, created.Document)
			if err != nil {
				t.Fatal(err)
			}
			const count, concurrency = 48, 8
			results := make([]error, count)
			var workers sync.WaitGroup
			start := make(chan struct{})
			for worker := range concurrency {
				workers.Go(func() {
					<-start
					for i := worker; i < count; i += concurrency {
						key := fmt.Sprintf("key-%02d", i)
						results[i] = c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", key, key, key)
					}
				})
			}
			close(start)
			workers.Wait()
			for i, err := range results {
				if err != nil {
					t.Fatal("coalescing lost acknowledged request", i, err)
				}
			}
			expected := map[string]string{"warm": "warm"}
			for i := range count {
				key := fmt.Sprintf("key-%02d", i)
				expected[key] = key
			}
			if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
				t.Fatal(err)
			}
			after, err := walProfile(ctx, c, created.Document)
			if err != nil {
				t.Fatal(err)
			}
			delta := writeProfile{entries: after.entries - before.entries, writes: after.writes - before.writes, perReplica: map[string]uint64{}}
			for name, index := range after.perReplica {
				delta.perReplica[name] = index - before.perReplica[name]
				if delta.perReplica[name] != count {
					t.Fatal("coalescing changed exact local Raft entry accounting", name, delta.perReplica[name])
				}
			}
			if delta.entries == 0 || delta.writes == 0 {
				t.Fatal("empty durability profile", delta)
			}
			profiles[arm] = delta
			t.Logf("METRIC nodes=%d wait=%s requests=%d durable_entries=%d durable_entry_writes=%d entries_per_write=%.3f", size, wait, count, delta.entries, delta.writes, float64(delta.entries)/float64(delta.writes))
		}) {
			t.FailNow()
		}
	}
	off, on := profiles[0], profiles[1]
	if off.entries != on.entries || off.entries != 48*3 {
		t.Fatal("write wait changed replicated persisted entries", off, on)
	}
	offMean, onMean := float64(off.entries)/float64(off.writes), float64(on.entries)/float64(on.writes)
	if onMean <= offMean {
		t.Fatal("write wait did not raise entries per durable write", offMean, onMean)
	}
}
