//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
)

func TestMultiGroupControllerShutdownAtFiveAndSevenNodes(t *testing.T) {
	for _, size := range []int{5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			spec := matrixSpec(3)
			spec.Limits.MaxNodes, spec.Limits.MaxReplicas = size, size
			c := acquire(t, spec).Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
			defer cancel()
			created, err := c.CreateGroup(ctx, "node2", metaservice.GroupSpec{ID: "baseline"})
			if err != nil {
				t.Fatal(err)
			}
			desired := created.Document.Clone()
			desired.Placement.MaxReplicas = size
			staged, err := c.Stage(ctx, "node2", desired)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
				t.Fatal(err)
			}
			// Grow through actual learner admissions, as in the source five-node case.
			for i := 4; i <= size; i++ {
				node, err := c.PrepareJoinNode(ctx, NodeSpec{Name: fmt.Sprintf("node%d", i), Type: "worker"})
				if err != nil {
					t.Fatal(err)
				}
				base, err := c.Document(ctx, "node2", "latest")
				if err != nil {
					t.Fatal(err)
				}
				response, err := c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": joiningNode(node.Spec), "mutation": metaservice.MutationOptions{Apply: true, Actor: "cluster-test"}}, base.Version)
				if err := requireStatus(response, err, 202); err != nil {
					t.Fatal(err)
				}
				var admitted metaservice.MutationResult
				if err := response.Decode(&admitted); err != nil || admitted.OperatorID == "" {
					t.Fatal("missing admission execution", admitted, err)
				}
				if err := node.Start(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.WaitOperator(ctx, "node2", admitted.OperatorID); err != nil {
					t.Fatal(err)
				}
				if err := c.WaitActive(ctx, admitted.Document.Version); err != nil {
					t.Fatal(err)
				}
				if err := c.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
			}
			groups := []string{"baseline", "shutdown-a", "shutdown-b"}
			for _, id := range groups[1:] {
				if _, err := c.CreateGroup(ctx, "node2", metaservice.GroupSpec{ID: id}); err != nil {
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
					t.Fatal("meta admission did not settle", node.Spec.Name, status, err)
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
		})
	}
}
