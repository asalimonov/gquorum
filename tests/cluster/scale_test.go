//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

// Covers declaration/approval, cross-node propagation, fifteen distinct load
// groups and exact caller-owned cleanup. Source workload-specific reserved IDs
// are replaced by the library's single reserved group, _meta.
func TestFifteenLoadGroupsCommitIndependentlyAndDeleteEveryLocalStore(t *testing.T) {
	c := acquire(t, threeNodes()).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	if _, err := c.CreateGroup(ctx, "node3", gquorum.GroupSpec{ID: "baseline"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "baseline", "state", "command", "baseline-value"); err != nil {
		t.Fatal(err)
	}
	doc, err := c.Document(ctx, "node2", "latest")
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := c.Request(ctx, "node2", "DELETE", "/api/groups/"+topology.MetaGroupID, gquorum.MutationOptions{Approve: true}, doc.Version)
	if err := requireStatus(reserved, err, 400); err != nil {
		t.Fatal("reserved meta deletion", err)
	}
	var ids []string
	for i := range 15 {
		id := fmt.Sprintf("scale-%02d", i)
		ids = append(ids, id)
		if _, err := c.CreateGroup(ctx, c.Spec.Nodes[i%len(c.Spec.Nodes)].Name, gquorum.GroupSpec{ID: id}); err != nil {
			t.Fatal(id, err)
		}
		// Identical command/key IDs across groups must not share state or deduplication.
		if err := c.Put(ctx, "node3", id, "state", "command", id); err != nil {
			t.Fatal(id, err)
		}
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil {
			t.Fatal(err)
		}
		if len(doc.LoadGroups()) != 16 || doc.Group("baseline") == nil {
			t.Fatal("scale declaration did not propagate", node.Spec.Name, doc)
		}
		for _, id := range ids {
			group := doc.Group(id)
			if group == nil || len(group.Replicas) != 3 {
				t.Fatal("missing approved placement", node.Spec.Name, id)
			}
			if err := c.WaitValue(ctx, node.Spec.Name, id, "state", id); err != nil {
				t.Fatal(err)
			}
			if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/" + id}); err != nil {
				t.Fatal("replicated value has no persistent local store", id, err)
			}
		}
	}
	for i, id := range ids {
		if _, err := c.Mutate(ctx, c.Spec.Nodes[i%len(c.Spec.Nodes)].Name, "DELETE", "/api/groups/"+id, gquorum.MutationOptions{Approve: true}); err != nil {
			t.Fatal(id, err)
		}
		for _, node := range c.Nodes() {
			doc, err := c.Document(ctx, node.Spec.Name, "active")
			if err != nil || doc.Group(id) != nil {
				t.Fatal("deletion did not propagate", node.Spec.Name, id, err)
			}
			_, err = c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/" + id})
			if !errdefs.IsNotFound(err) {
				t.Fatal("succeeded execution retained local state", node.Spec.Name, id, err)
			}
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/load/"+id+"/keys/state", nil, 0)
			if err := requireStatus(response, err, 503); err != nil {
				t.Fatal("deleted group still serves", err)
			}
		}
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || len(doc.LoadGroups()) != 1 || doc.MetaGroup() == nil {
			t.Fatal("scale teardown affected retained groups", node.Spec.Name, doc, err)
		}
		if err := c.WaitValue(ctx, node.Spec.Name, "baseline", "state", "baseline-value"); err != nil {
			t.Fatal(err)
		}
	}
}
