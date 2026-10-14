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

// A memory replica loses its Raft state with its process. The restarted node
// reseats its own replica under a fresh ID and catches up from the leader,
// while its persistent replica on the same node keeps its identity.
func TestMemoryGroupReseatsARestartedReplicaThreeNodes(t *testing.T) {
	t.Parallel()
	lease := acquire(t, matrixSpec(3))
	c := lease.Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	placement := &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}
	if _, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "durable", Placement: placement}); err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "memory", Placement: placement, Storage: topology.StorageMemory})
	if err != nil || created.Document.Group("memory").Storage != topology.StorageMemory || created.Document.Group("durable").Storage != topology.StorageDefault {
		t.Fatal("storage classes not recorded", created, err)
	}
	expected := map[string]map[string]string{"memory": {}, "durable": {}}
	for i := range 6 {
		for group, values := range expected {
			key := fmt.Sprintf("before-%d", i)
			if err := c.Put(ctx, c.Spec.Nodes[i%3].Name, group, key, group+"-"+key, key); err != nil {
				t.Fatal(group, err)
			}
			values[key] = key
		}
	}
	for group, values := range expected {
		if err := exactOwnedValues(ctx, c, created.Document, group, values); err != nil {
			t.Fatal(group, err)
		}
	}
	oldMemory, oldDurable := replicaID(created.Document, "memory", "node3"), replicaID(created.Document, "durable", "node3")
	victim := c.Node("node3")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := time.Now()
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// No operator acts: the restarted node requests its own reseat.
	var active topology.Document
	if err := Poll(ctx, func() (bool, error) {
		doc, err := c.Document(ctx, "node1", "active")
		active = doc
		return err == nil && replicaID(doc, "memory", "node3") > oldMemory, err
	}); err != nil {
		t.Fatal("restarted memory replica was not reseated", active, err)
	}
	reseated := time.Since(restarted)
	if replicaID(active, "durable", "node3") != oldDurable {
		t.Fatal("persistent replica was reseated", active)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for group, values := range expected {
		if err := exactOwnedValues(ctx, c, active, group, values); err != nil {
			t.Fatal("restarted node did not recover", group, err)
		}
	}
	for group, values := range expected {
		if err := c.Put(ctx, "node3", group, "after", group+"-after", "after"); err != nil {
			t.Fatal("restarted node does not accept writes", group, err)
		}
		values["after"] = "after"
		if err := exactOwnedValues(ctx, c, active, group, values); err != nil {
			t.Fatal(group, err)
		}
	}
	local, err := c.Local(ctx, "node3", "memory")
	if err != nil || !local.Found || local.Parked != nil || local.ReplicaID != replicaID(active, "memory", "node3") {
		t.Fatal("restarted node does not serve its fresh memory replica", local, err)
	}
	t.Logf("METRIC old_replica=%d new_replica=%d reseat_ms=%d", oldMemory, local.ReplicaID, reseated.Milliseconds())
}
