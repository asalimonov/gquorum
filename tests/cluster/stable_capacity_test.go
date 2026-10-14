//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

// Seven hosts exceed the default five total load replicas. These cases cover
// explicit capacity and atomic overflow, not a larger repeat of the fault suite.
func TestStableLearnerSevenHostsRequireExplicitReplicaCapacity(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{5, 7} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			t.Parallel()
			spec := matrixSpec(7)
			spec.Limits.MaxReplicas = capacity
			c := acquire(t, spec).Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Second)
			defer cancel()
			before, err := c.Document(ctx, "node7", "active")
			if err != nil {
				t.Fatal(err)
			}
			if len(before.MetaGroup().Replicas) != 7 || len(before.VotersOf(topology.MetaGroupID)) != 3 {
				t.Fatal("meta must cover every host with three voters", before)
			}
			if capacity == 5 {
				response, err := c.Request(ctx, "node7", "POST", "/api/groups", map[string]any{
					"group": gquorum.GroupSpec{ID: "all"}, "mutation": gquorum.MutationOptions{Approve: true},
				}, before.Version)
				if err := requireStatus(response, err, 400); err != nil {
					t.Fatal("default capacity silently omitted load hosts", err)
				}
				assertAPIViews(ctx, t, c, before)
				for _, node := range c.Nodes() {
					local, err := c.Local(ctx, node.Spec.Name, "all")
					if err != nil || local.Found {
						t.Fatal("rejected intent started a replica", node.Spec.Name, local, err)
					}
				}
				return
			}
			created, err := c.CreateGroup(ctx, "node7", gquorum.GroupSpec{ID: "all"})
			if err != nil {
				t.Fatal(err)
			}
			if len(created.Document.Group("all").Replicas) != 7 || len(created.Document.VotersOf("all")) != 3 {
				t.Fatal("load hosts confused with voter capacity", created.Document)
			}
			if err := Poll(ctx, func() (bool, error) {
				for _, node := range c.Nodes() {
					for _, id := range []string{topology.MetaGroupID, "all"} {
						local, err := c.Local(ctx, node.Spec.Name, id)
						if err != nil {
							return false, err
						}
						if !local.Found || local.ReplicaID != replicaID(created.Document, id, node.Spec.Name) || len(local.Status.ConfState.Voters) != 3 || len(local.Status.ConfState.Learners) != 4 {
							return false, nil
						}
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal("seven replicas did not converge with a three-voter quorum", err)
			}
		})
	}
}
