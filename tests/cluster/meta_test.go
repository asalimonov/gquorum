//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
)

func matrixSpec(size int) Spec {
	spec := Spec{Limits: topology.Limits{MaxNodes: size, MaxReplicas: 5}}
	for i := range size {
		typ := topology.NodeType("worker")
		if i >= 3 {
			typ = "observer"
		}
		spec.Nodes = append(spec.Nodes, NodeSpec{Name: fmt.Sprintf("node%d", i+1), Type: typ})
	}
	return spec
}
func TestMetaConsensusLoadReplicationAndRetainedLeaderRecovery(t *testing.T) {
	for _, size := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			lease := acquire(t, matrixSpec(size))
			c := lease.Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 85*time.Second)
			defer cancel()
			doc, err := c.Document(ctx, "node1", "active")
			if err != nil {
				t.Fatal(err)
			}
			if len(doc.Nodes) != size || len(doc.MetaGroup().Replicas) != size || doc.Version != 1 {
				t.Fatal("founding topology", doc)
			}
			for _, node := range c.Nodes() {
				status, err := c.Status(ctx, node.Spec.Name)
				if err != nil {
					t.Fatal(err)
				}
				if !status.SessionLive || status.ActiveVersion != doc.Version || len(status.Groups) != 1 || len(status.Groups[0].ConfState.Voters) != size {
					t.Fatal("founder did not join meta", node.Spec.Name, status)
				}
			}
			spec := metaservice.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}}
			result, err := c.CreateGroup(ctx, c.Spec.Nodes[size-1].Name, spec)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Document.Group("work").Replicas) != 3 {
				t.Fatal("subset placement", result.Document)
			}
			for i := range 24 {
				if err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", fmt.Sprintf("key-%d", i), fmt.Sprintf("command-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
					t.Fatal("cross-node proposal", err)
				}
			}
			for _, replica := range result.Document.Group("work").Replicas {
				for i := range 24 {
					if err := c.WaitValue(ctx, replica.NodeName, "work", fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			status, err := c.Status(ctx, "node1")
			if err != nil {
				t.Fatal(err)
			}
			oldLeader := status.MetaLeader
			var stopped *Node
			for _, node := range c.Nodes() {
				if result.Document.MemberID(node.Spec.Name) == oldLeader {
					stopped = node
					break
				}
			}
			if stopped == nil {
				t.Fatal("unknown meta leader")
			}
			if err := stopped.Kill(ctx); err != nil {
				t.Fatal(err)
			}
			var survivor string
			for _, node := range c.Nodes() {
				if node != stopped {
					survivor = node.Spec.Name
					break
				}
			}
			if err := Poll(ctx, func() (bool, error) {
				s, err := c.Status(ctx, survivor)
				return s.MetaLeader != 0 && s.MetaLeader != oldLeader, err
			}); err != nil {
				t.Fatal("leader replacement", err)
			}
			// Prove the replacement meta leader can commit through a surviving
			// follower. A staged label has no load lifecycle effects while a host
			// is down; the recovering process must replay this new document.
			staged := result.Document.Clone()
			staged.Node(survivor).Labels["test.after_leader"] = "committed"
			var stagedResult metaservice.MutationResult
			if err := Poll(ctx, func() (bool, error) {
				response, err := c.Request(ctx, survivor, "PUT", "/api/topology", staged, staged.Version)
				if err != nil {
					return false, err
				}
				if response.Status == 200 {
					return true, response.Decode(&stagedResult)
				}
				if response.Status == 409 {
					latest, err := c.Document(ctx, survivor, "latest")
					if err == nil && latest.Version == staged.Version+1 && latest.Node(survivor).Labels["test.after_leader"] == "committed" {
						stagedResult.Document = latest
						return true, nil
					}
				}
				return false, requireStatus(response, nil, 200)
			}); err != nil {
				t.Fatal("replacement meta leader cannot commit", err)
			}
			if err := Poll(ctx, func() (bool, error) {
				err := c.Put(ctx, survivor, "work", "during-restart", "during-restart", "acknowledged")
				return err == nil, err
			}); err != nil {
				t.Fatal(err)
			}
			if err := stopped.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			recovered, err := c.Document(ctx, stopped.Spec.Name, "active")
			if err != nil {
				t.Fatal(err)
			}
			if recovered.MemberID(stopped.Spec.Name) != oldLeader || recovered.Version != result.Document.Version {
				t.Fatal("restart changed approved identity/lineage", recovered)
			}
			if err := Poll(ctx, func() (bool, error) {
				latest, err := c.Document(ctx, stopped.Spec.Name, "latest")
				return latest.Version == stagedResult.Document.Version && latest.Node(survivor) != nil && latest.Node(survivor).Labels["test.after_leader"] == "committed", err
			}); err != nil {
				t.Fatal("recovering meta replica lost the acknowledged edit", err)
			}
			hosts := result.Document.Group("work").Replicas
			if slices.ContainsFunc(hosts, func(r topology.Replica) bool { return r.NodeName == stopped.Spec.Name }) {
				for i := range 24 {
					if err := c.WaitValue(ctx, stopped.Spec.Name, "work", fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
						t.Fatal("recovering node lost acknowledged data", err)
					}
				}
				if err := c.WaitValue(ctx, stopped.Spec.Name, "work", "during-restart", "acknowledged"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
