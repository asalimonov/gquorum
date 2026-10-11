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
	docker "github.com/moby/moby/client"
)

func TestMultiGroupControllerShutdownThreeNodes(t *testing.T) {
	t.Parallel()
	const size = 3
	spec := matrixSpec(3)
	spec.Limits.MaxNodes, spec.Limits.MaxReplicas = size, size
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	_, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "baseline"})
	if err != nil {
		t.Fatal(err)
	}
	groups := []string{"baseline", "shutdown-a", "shutdown-b"}
	for _, id := range groups[1:] {
		if _, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range groups {
		for i := range 12 {
			key := fmt.Sprintf("command-%02d", i)
			if err := c.Put(ctx, fmt.Sprintf("node%d", i%size+1), id, key, key, id+key); err != nil {
				t.Fatal(id, err)
			}
		}
	}
	for _, node := range c.Nodes() {
		status, err := groupStatus(ctx, c, node.Spec.Name, topology.MetaGroupID)
		if err != nil || len(status.ConfState.Voters) != size || len(status.ConfState.Learners) != 0 {
			t.Fatal("meta membership did not settle", node.Spec.Name, status, err)
		}
		for _, id := range groups {
			if err := Poll(ctx, func() (bool, error) {
				own, err := c.Local(ctx, node.Spec.Name, id)
				if err != nil || !own.Found || len(own.Status.ConfState.Voters) != size || len(own.Status.ConfState.Learners) != 0 {
					return false, err
				}
				for i := range 12 {
					key := fmt.Sprintf("command-%02d", i)
					if own.Values[key] != id+key {
						return false, fmt.Errorf("%s/%s has not applied %s", node.Spec.Name, id, key)
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal("own host lost acknowledged multi-group membership/state", node.Spec.Name, id, err)
			}
		}
	}
	stopCtx, stopCancel := context.WithTimeout(ctx, 30*time.Second)
	defer stopCancel()
	type stopped struct {
		node *Node
		err  error
	}
	results := make(chan stopped, len(c.Nodes()))
	var workers sync.WaitGroup
	start := time.Now()
	for _, node := range c.Nodes() {
		workers.Go(func() { results <- stopped{node, node.Stop(stopCtx)} })
	}
	// Always join admitted stops before the lease can be released, including
	// deadline failure. Docker Stop observes the bounded context.
	workers.Wait()
	close(results)
	if err := stopCtx.Err(); err != nil {
		t.Fatal("concurrent multi-group shutdown exceeded thirty seconds", err)
	}
	for result := range results {
		if result.err != nil {
			t.Fatal("stop", result.node.Spec.Name, result.err)
		}
		inspected, err := c.docker.ContainerInspect(ctx, result.node.Container.GetContainerID(), docker.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		state := inspected.Container.State
		if state == nil || state.Running || state.ExitCode != 0 || state.OOMKilled {
			t.Fatal("shutdown required a kill or failed to release resources", result.node.Spec.Name, state)
		}
	}
	t.Logf("%d nodes with %d groups stopped cleanly in %s", size, len(groups), time.Since(start))
}
