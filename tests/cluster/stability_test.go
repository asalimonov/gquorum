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

func TestStability_HealthAndExactLocalReadsThreeNodes(t *testing.T) {
	t.Parallel()
	stabilityHealthAndReads(t, 3)
}
func TestStability_RetainedFollowerThreeCyclesThreeNodes(t *testing.T) {
	t.Parallel()
	stabilityFollowerCycles(t, 3)
}

func waitClusterHealth(ctx context.Context, c *Cluster, stopped string) error {
	return Poll(ctx, func() (bool, error) {
		for _, observer := range c.Nodes() {
			if !observer.running.Load() {
				continue
			}
			status, err := c.Status(ctx, observer.Spec.Name)
			if err != nil {
				return false, err
			}
			if len(status.Nodes) != len(c.Nodes()) {
				return false, fmt.Errorf("%s lost declared node count", observer.Spec.Name)
			}
			for _, node := range status.Nodes {
				expected := topology.HealthAlive
				if node.Node.Name == stopped {
					expected = topology.HealthDead
				}
				if node.Health != expected {
					return false, fmt.Errorf("%s observes %s as %s, expected %s", observer.Spec.Name, node.Node.Name, node.Health, expected)
				}
			}
		}
		return true, nil
	})
}

func stabilityHealthAndReads(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.Limits.MaxReplicas = size
	for i := range spec.Nodes {
		spec.Nodes[i].Type = "worker"
	}
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: size}})
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Document.Group("work").Replicas) != size {
		t.Fatal("read stability requires every tested node to own the FSM")
	}
	for i := range 10 {
		key := fmt.Sprintf("record-%02d", i)
		if err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", key, key, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range c.Nodes() {
		for i := range 10 {
			key := fmt.Sprintf("record-%02d", i)
			if err := c.WaitValue(ctx, node.Spec.Name, "work", key, key); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := waitClusterHealth(ctx, c, ""); err != nil {
		t.Fatal(err)
	}
	type samples struct {
		count   int
		elapsed time.Duration
		err     error
	}
	reads, health := make([]samples, size), make([]samples, size)
	start := make(chan struct{})
	started := time.Now()
	var workers sync.WaitGroup
	for index, node := range c.Nodes() {
		workers.Go(func() {
			<-start
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			var result samples
			defer func() { result.elapsed = time.Since(started); reads[index] = result }()
			for time.Since(started) < 15*time.Second {
				for i := range 10 {
					key := fmt.Sprintf("record-%02d", i)
					value, err := c.Get(ctx, node.Spec.Name, "work", key)
					if err != nil {
						result.err = err
						return
					}
					if !value.Found || value.Value != key || value.Node != node.Spec.Name || value.Consistency != "local" || value.Index == 0 {
						result.err = fmt.Errorf("own local read changed acknowledged value/identity: %+v", value)
						return
					}
					result.count++
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					result.err = ctx.Err()
					return
				}
			}
		})
		workers.Go(func() {
			<-start
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			var result samples
			defer func() { result.elapsed = time.Since(started); health[index] = result }()
			for time.Since(started) < 20*time.Second {
				response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
				if err := requireStatus(response, err, 200); err != nil {
					result.err = err
					return
				}
				var ready struct {
					Node        string `json:"node"`
					SessionLive bool   `json:"session_live"`
					Version     uint64 `json:"active_version"`
				}
				if err := response.Decode(&ready); err != nil || ready.Node != node.Spec.Name || !ready.SessionLive || ready.Version != created.Document.Version {
					result.err = fmt.Errorf("readiness lost its exact serving identity/version: %+v: %v", ready, err)
					return
				}
				status, err := c.Status(ctx, node.Spec.Name)
				if err != nil {
					result.err = err
					return
				}
				if len(status.Nodes) != size || status.ActiveVersion != created.Document.Version || !status.SessionLive {
					result.err = fmt.Errorf("healthy cluster changed node count/serving version: %+v", status)
					return
				}
				for _, member := range status.Nodes {
					if member.Health != topology.HealthAlive {
						result.err = fmt.Errorf("stable member %s became %s", member.Node.Name, member.Health)
						return
					}
				}
				result.count++
				select {
				case <-ticker.C:
				case <-ctx.Done():
					result.err = ctx.Err()
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
	for i, node := range c.Nodes() {
		read, ready := reads[i], health[i]
		if read.err != nil || read.count < 100 || read.elapsed < 15*time.Second {
			t.Errorf("%s sustained reads failed/empty/short: %+v", node.Spec.Name, read)
		}
		if ready.err != nil || ready.count < 20 || ready.elapsed < 20*time.Second {
			t.Errorf("%s sustained health failed/empty/short: %+v", node.Spec.Name, ready)
		}
		t.Logf("METRIC node=%s exact_local_reads=%d read_elapsed_ms=%.3f healthy_checks=%d health_elapsed_ms=%.3f", node.Spec.Name, read.count, float64(read.elapsed)/float64(time.Millisecond), ready.count, float64(ready.elapsed)/float64(time.Millisecond))
	}
}

func stabilityFollowerCycles(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := requestLeaderTransfer(ctx, c, "node2", "work", "node1"); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", "work")
		return err == nil && status.LeaderID == replicaID(created.Document, "work", "node1"), err
	}); err != nil {
		t.Fatal(err)
	}
	own, err := c.Local(ctx, "node3", "work")
	if err != nil || !own.Found {
		t.Fatal(own, err)
	}
	originalMember, originalReplica := own.Identity.MemberID, own.ReplicaID
	previousInstance := own.Identity.InstanceID
	for cycle := range 3 {
		started := time.Now()
		if err := c.Node("node3").Stop(ctx); err != nil {
			t.Fatal("stop", cycle, err)
		}
		if err := waitClusterHealth(ctx, c, "node3"); err != nil {
			t.Fatal("every survivor must detect the exact stopped follower", cycle, err)
		}
		t.Logf("METRIC cycle=%d departure_detection_ms=%.3f", cycle+1, float64(time.Since(started))/float64(time.Millisecond))
		for i := range 10 {
			key := fmt.Sprintf("cycle-%d-%02d", cycle, i)
			if err := c.Put(ctx, "node2", "work", key, key, key); err != nil {
				for _, current := range c.Nodes() {
					if !current.running.Load() {
						continue
					}
					diagnostic, stop := context.WithTimeout(context.Background(), time.Second)
					local, inspectErr := c.Local(diagnostic, current.Spec.Name, "work")
					t.Logf("failed majority local %s: identity=%+v replica=%d status=%+v values=%v storage=%+v err=%v", current.Spec.Name, local.Identity, local.ReplicaID, local.Status, local.Values, local.Storage, inspectErr)
					stop()
				}
				t.Fatal("retained majority failed to commit", cycle, err)
			}
		}
		restarted := time.Now()
		if err := c.Node("node3").Start(ctx); err != nil {
			t.Fatal("restart", cycle, err)
		}
		if err := c.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		if err := waitClusterHealth(ctx, c, ""); err != nil {
			t.Fatal("every process must observe recovered membership", cycle, err)
		}
		if err := Poll(ctx, func() (bool, error) {
			own, err := c.Local(ctx, "node3", "work")
			if err != nil || !own.Found {
				return false, err
			}
			if own.Identity.MemberID != originalMember || own.ReplicaID != originalReplica || own.Identity.InstanceID == previousInstance || !slices.Contains(own.Status.ConfState.Voters, originalReplica) {
				return true, fmt.Errorf("retained restart changed replica identity or reused process identity: %+v", own.Identity)
			}
			for previous := 0; previous <= cycle; previous++ {
				for i := range 10 {
					key := fmt.Sprintf("cycle-%d-%02d", previous, i)
					if own.Values[key] != key {
						return false, fmt.Errorf("recovering process lacks acknowledged %s", key)
					}
				}
			}
			previousInstance = own.Identity.InstanceID
			return true, nil
		}); err != nil {
			t.Fatal("own recovering replica lost its acknowledged state", cycle, err)
		}
		for _, node := range c.Nodes() {
			doc, err := c.Document(ctx, node.Spec.Name, "latest")
			if err != nil || doc.Version != created.Document.Version || doc.MemberID("node3") != originalMember || replicaID(doc, "work", "node3") != originalReplica {
				t.Fatal("normal restart churned topology or allocated replacement identities", node.Spec.Name, doc, err)
			}
		}
		t.Logf("METRIC cycle=%d retained_recovery_ms=%.3f acknowledged_commands=%d", cycle+1, float64(time.Since(restarted))/float64(time.Millisecond), (cycle+1)*10)
	}
}
