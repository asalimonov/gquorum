//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// Each matrix member is a separate test with its own whole-case time limit.
// Cases use independent pool leases; make test-perf limits parallelism for comparable measurements.
func TestPerf_ConcurrentHealthThreeNodes(t *testing.T) {
	t.Parallel()
	perfConcurrentHealth(t, 3)
}
func TestPerf_ConcurrentWritesThreeNodes(t *testing.T) {
	t.Parallel()
	perfConcurrentWrites(t, 3)
}

func performanceCluster(t *testing.T, size int) (context.Context, *Cluster) {
	t.Helper()
	return performanceClusterSpec(t, matrixSpec(size))
}

func performanceClusterSpec(t *testing.T, spec Spec) (context.Context, *Cluster) {
	t.Helper()
	started := time.Now()
	// Registration precedes acquire's release callback, so this measurement
	// includes deployment, correctness verification and generation teardown.
	t.Cleanup(func() {
		elapsed := time.Since(started)
		t.Logf("METRIC case_elapsed_ms=%.3f", float64(elapsed)/float64(time.Millisecond))
		if elapsed >= 2*time.Minute {
			t.Errorf("performance case exceeded two minutes: %s", elapsed)
		}
	})
	ctx, cancel := context.WithDeadline(t.Context(), started.Add(80*time.Second))
	t.Cleanup(cancel)
	return ctx, acquire(t, spec).Cluster
}

type measuredOperation struct {
	duration  time.Duration
	err       error
	conflicts int
}

func reportPerformance(t *testing.T, name string, results []measuredOperation, elapsed time.Duration, minRate float64, maxP95 time.Duration) {
	t.Helper()
	if len(results) == 0 || elapsed <= 0 {
		t.Fatal("performance workload is empty", name)
	}
	durations := make([]time.Duration, len(results))
	var total time.Duration
	conflicts, failures := 0, 0
	for i, result := range results {
		durations[i] = result.duration
		total += result.duration
		conflicts += result.conflicts
		if result.err != nil {
			failures++
			t.Errorf("%s operation %d: %v", name, i, result.err)
		}
	}
	slices.Sort(durations)
	p95 := durations[(95*len(durations)+99)/100-1]
	rate := float64(len(results)-failures) / elapsed.Seconds()
	t.Logf("METRIC %s_operations=%d %s_failures=%d %s_rate=%.3f %s_avg_ms=%.3f %s_p95_ms=%.3f %s_elapsed_ms=%.3f cas_conflicts=%d", name, len(results), name, failures, name, rate, name, float64(total)/float64(len(results))/float64(time.Millisecond), name, float64(p95)/float64(time.Millisecond), name, float64(elapsed)/float64(time.Millisecond), conflicts)
	if rate < minRate {
		t.Errorf("%s rate %.3f is below %.3f operations/second", name, rate, minRate)
	}
	if p95 >= maxP95 {
		t.Errorf("%s p95 %s exceeds %s", name, p95, maxP95)
	}
	if failures > 0 {
		t.FailNow()
	}
}

func perfConcurrentHealth(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	base, err := c.Document(ctx, "node1", "active")
	if err != nil {
		t.Fatal(err)
	}
	const concurrency, requestsPerWorker = 10, 50
	results := make([]measuredOperation, concurrency*requestsPerWorker)
	start := make(chan struct{})
	var workers sync.WaitGroup
	started := time.Now()
	for worker := range concurrency {
		workers.Go(func() {
			<-start
			for request := range requestsPerWorker {
				node := c.Spec.Nodes[(worker+request)%size].Name
				operationStart := time.Now()
				response, err := c.Request(ctx, node, "GET", "/health/ready", nil, 0)
				if err = requireStatus(response, err, 200); err == nil {
					var ready struct {
						Node          string `json:"node"`
						SessionLive   bool   `json:"session_live"`
						TargetVersion uint64 `json:"target_version"`
					}
					if err = response.Decode(&ready); err == nil && (ready.Node != node || !ready.SessionLive || ready.TargetVersion != base.Version) {
						err = fmt.Errorf("health reply has another identity or target version: %+v", ready)
					}
				}
				// Keep concurrent real cluster status handling in this workload, with a
				// mandatory stable node count rather than an optional/missing metric.
				if err == nil && request%10 == 0 {
					status, statusErr := c.Status(ctx, node)
					err = statusErr
					if err == nil && (len(status.Nodes) != size || status.MetaLeaderID == 0 || status.FullyConverged.Version != base.Version || !status.SessionLive) {
						err = fmt.Errorf("health/status lost stable cluster membership: %+v", status)
					}
				}
				results[worker*requestsPerWorker+request] = measuredOperation{duration: time.Since(operationStart), err: err}
			}
		})
	}
	close(start)
	workers.Wait()
	reportPerformance(t, "health", results, time.Since(started), 100, 500*time.Millisecond)
	t.Logf("METRIC status_reads=%d", concurrency*(requestsPerWorker/10))
}

func perfConcurrentWrites(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	const commands = 15
	results := make([]measuredOperation, commands)
	versions := make([]uint64, commands)
	start := make(chan struct{})
	var workers sync.WaitGroup
	started := time.Now()
	for i := range commands {
		workers.Go(func() {
			<-start
			operationStart := time.Now()
			node := c.Spec.Nodes[i%size].Name
			key, value := fmt.Sprintf("perf.write.%02d", i), fmt.Sprintf("value-%02d", i)
			var operationErr error
			conflicts := 0
			for attempt := 0; attempt < 128; attempt++ {
				candidate, err := c.Document(ctx, node, "latest")
				if err != nil {
					operationErr = err
					break
				}
				candidate.ParentVersion = candidate.Version
				candidate.Node("node1").Labels[key] = value
				response, err := c.Request(ctx, node, "PUT", "/api/topology", candidate, candidate.Version)
				if err != nil {
					operationErr = err
					break
				}
				if response.Status == 409 {
					var conflict struct {
						Code     string `json:"code"`
						Expected uint64 `json:"expected_version"`
						Current  uint64 `json:"current_version"`
					}
					if err := response.Decode(&conflict); err != nil || conflict.Code != "version_conflict" || conflict.Expected != candidate.Version || conflict.Current <= candidate.Version {
						operationErr = fmt.Errorf("invalid CAS retry result: %+v: %v", conflict, err)
						break
					}
					conflicts++
					timer := time.NewTimer(5 * time.Millisecond)
					select {
					case <-timer.C:
					case <-ctx.Done():
						operationErr = ctx.Err()
					}
					timer.Stop()
					if operationErr != nil {
						break
					}
					continue
				}
				if err := requireStatus(response, nil, 200); err != nil {
					operationErr = err
					break
				}
				var committed gquorum.MutationResult
				if err := response.Decode(&committed); err != nil || committed.Document.Version != candidate.Version+1 || committed.Document.ParentVersion != candidate.Version || committed.Document.Node("node1") == nil || committed.Document.Node("node1").Labels[key] != value {
					operationErr = fmt.Errorf("write returned a different commit: %+v: %v", committed, err)
					break
				}
				versions[i] = committed.Document.Version
				break
			}
			if operationErr == nil && versions[i] == 0 {
				operationErr = fmt.Errorf("write exhausted CAS retry bound")
			}
			results[i] = measuredOperation{duration: time.Since(operationStart), err: operationErr, conflicts: conflicts}
		})
	}
	close(start)
	workers.Wait()
	reportPerformance(t, "meta_write", results, time.Since(started), 5, 3*time.Second)
	sorted := slices.Clone(versions)
	slices.Sort(sorted)
	for i, version := range sorted {
		if version != base.Version+uint64(i)+1 {
			t.Fatal("concurrent writes lost or duplicated a committed version", sorted)
		}
	}
	finalVersion := base.Version + commands
	verifyStart := time.Now()
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			doc, err := c.Document(ctx, node.Spec.Name, "latest")
			if err != nil || doc.Version != finalVersion {
				return false, err
			}
			for i := range commands {
				if doc.Node("node1").Labels[fmt.Sprintf("perf.write.%02d", i)] != fmt.Sprintf("value-%02d", i) {
					return true, fmt.Errorf("committed concurrent write %d was overwritten on %s", i, node.Spec.Name)
				}
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("acknowledged meta writes did not converge", err)
	}
	syncElapsed := time.Since(verifyStart)
	t.Logf("METRIC meta_convergence_ms=%.3f", float64(syncElapsed)/float64(time.Millisecond))
	if syncElapsed >= 10*time.Second {
		t.Fatal("meta convergence exceeded ten seconds", syncElapsed)
	}
	if _, err := c.Approve(ctx, c.Spec.Nodes[size-1].Name, finalVersion); err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	group := created.Document.Group("work")
	if group == nil || len(group.Replicas) != 3 {
		t.Fatal("performance fixture lacks the nonempty three-replica load group", created.Document)
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil {
			t.Fatal(err)
		}
		for i := range commands {
			if doc.Node("node1").Labels[fmt.Sprintf("perf.write.%02d", i)] != fmt.Sprintf("value-%02d", i) {
				t.Fatal("approval lost acknowledged metadata", node.Spec.Name, i)
			}
		}
	}
	loadResults := make([]measuredOperation, commands)
	started = time.Now()
	for i := range commands {
		workers.Go(func() {
			operationStart := time.Now()
			key := fmt.Sprintf("write-%02d", i)
			err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", key, key, fmt.Sprintf("value-%02d", i))
			loadResults[i] = measuredOperation{duration: time.Since(operationStart), err: err}
		})
	}
	workers.Wait()
	reportPerformance(t, "load_write", loadResults, time.Since(started), 5, 3*time.Second)
	verifyStart = time.Now()
	for _, replica := range group.Replicas {
		if err := Poll(ctx, func() (bool, error) {
			own, err := c.Local(ctx, replica.NodeName, "work")
			if err != nil || !own.Found || own.ReplicaID != replica.ReplicaID {
				return false, err
			}
			for i := range commands {
				if own.Values[fmt.Sprintf("write-%02d", i)] != fmt.Sprintf("value-%02d", i) {
					return false, fmt.Errorf("%s has not applied acknowledged load command %d", replica.NodeName, i)
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal("own load replica lost concurrent acknowledged state", replica.NodeName, err)
		}
	}
	syncElapsed = time.Since(verifyStart)
	t.Logf("METRIC load_convergence_ms=%.3f", float64(syncElapsed)/float64(time.Millisecond))
	if syncElapsed >= 10*time.Second {
		t.Fatal("load convergence exceeded ten seconds", syncElapsed)
	}
}
