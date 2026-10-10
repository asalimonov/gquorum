//go:build cluster

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func assertDrained(t *testing.T, ctx context.Context, c *Cluster, name, group string, version uint64) {
	t.Helper()
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || doc.Version != version || doc.Node(name) == nil || doc.Node(name).State != topology.NodeDraining || len(doc.MetaGroup().Replicas) != 3 || len(doc.Group(group).Replicas) != 2 {
			t.Fatal("drain did not retain meta and shrink load membership", node.Spec.Name, doc, err)
		}
		status, err := c.Status(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(status.Nodes, func(n topology.NodeStatus) bool {
			return n.Node.Name == name && n.Node.State == topology.NodeDraining && n.EffectiveState == topology.NodeDrained
		}) {
			t.Fatal("drained must be derived without an extra document", node.Spec.Name, status)
		}
		observed, err := groupStatus(ctx, c, node.Spec.Name, topology.MetaGroupID)
		if err != nil || len(observed.ConfState.Voters) != 3 || len(observed.ConfState.Learners) != 0 {
			t.Fatal("drain changed actual meta membership", observed, err)
		}
		observed, err = groupStatus(ctx, c, node.Spec.Name, group)
		if err != nil || len(observed.ConfState.Voters) != 2 || len(observed.ConfState.Learners) != 0 {
			t.Fatal("drain did not converge actual load membership", observed, err)
		}
	}
	response, err := c.Request(ctx, name, "GET", "/api/load/"+group+"/keys/before-drain", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("drained node still serves load", err)
	}
	if _, err := c.docker.ContainerStatPath(ctx, c.Node(name).Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/" + group}); !errdefs.IsNotFound(err) {
		t.Fatal("drained node retained removed load store", err)
	}
}

func TestDrainViaNonLeaderPreservesMetaMembership(t *testing.T) {
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node3", "work", "before-drain", "before-drain", "retained"); err != nil {
		t.Fatal(err)
	}
	drained, err := c.Mutate(ctx, "node3", "POST", "/api/nodes/node3/drain", meta.MutationOptions{Apply: true, Actor: "cluster-test"})
	if err != nil || drained.ExecutionID == "" {
		t.Fatal("follower did not forward both stage and apply", drained, err)
	}
	assertDrained(t, ctx, c, "node3", "work", drained.Document.Version)
	status, err := c.Status(ctx, "node3")
	if err != nil || status.MetaLeader != created.Document.MemberID("node1") {
		t.Fatal("issuer became leader instead of forwarding", status, err)
	}
	path := fmt.Sprintf("/api/topology/%d/apply", drained.Document.Version)
	for _, request := range []struct {
		query string
		body  any
	}{{body: map[string]bool{"dry_run": true}}, {query: "?dry_run"}, {query: "?apply=false"}, {query: "?preview="}} {
		response, err := c.Request(ctx, "node3", "POST", path+request.query, request.body, 0)
		if err := requireStatus(response, err, 400); err != nil || !bytes.Contains(response.Data, []byte("/plan")) {
			t.Fatal("follower apply silently ignored a preview flag", response, err)
		}
	}
	// Empty options remain compatible; repeated apply returns the exact attempt.
	response, err := c.Request(ctx, "node3", "POST", path, struct{}{}, 0)
	if err := requireStatus(response, err, 202); err != nil || !bytes.Contains(response.Data, []byte(drained.ExecutionID)) {
		t.Fatal("repeated apply created another attempt", response, err)
	}
	activated, err := c.Mutate(ctx, "node3", "POST", "/api/nodes/node3/activate", meta.MutationOptions{Apply: true, Actor: "cluster-test"})
	if err != nil || len(activated.Document.Group("work").Replicas) != 3 || activated.Document.MemberID("node3") != created.Document.MemberID("node3") {
		t.Fatal("activation changed meta identity or failed to regrow", activated, err)
	}
	oldID, newID := uint64(0), uint64(0)
	for _, r := range created.Document.Group("work").Replicas {
		if r.NodeName == "node3" {
			oldID = r.ReplicaID
		}
	}
	for _, r := range activated.Document.Group("work").Replicas {
		if r.NodeName == "node3" {
			newID = r.ReplicaID
		}
	}
	if oldID == 0 || newID <= oldID {
		t.Fatal("activation reused removed load identity", oldID, newID)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "work", "before-drain", "retained"); err != nil {
			t.Fatal("activation failed to replay acknowledged data", err)
		}
	}
}

func TestDrainApprovalChecksObservedActualMetaQuorum(t *testing.T) {
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 95*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/node2/drain", meta.MutationOptions{Actor: "cluster-test"}, created.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var staged meta.MutationResult
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	// Synchronize with the leader's observation, rather than racing expiry.
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Status(ctx, "node1")
		return slices.ContainsFunc(status.Nodes, func(n topology.NodeStatus) bool { return n.Node.Name == "node3" && n.Health == topology.HealthDead }), err
	}); err != nil {
		t.Fatal("leader did not observe stopped voter", err)
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "POST", fmt.Sprintf("/api/topology/%d/apply", staged.Document.Version), nil, 0)
		if err != nil || response.Status == 503 {
			return false, requireStatus(response, err, 422)
		}
		if err := requireStatus(response, nil, 422); err != nil {
			return true, err // Any approval or other refusal is a real failure.
		}
		if !bytes.Contains(response.Data, []byte("actual voters")) {
			return true, fmt.Errorf("drain did not check actual meta majority: %s", response.Data)
		}
		return true, nil
	}); err != nil {
		t.Fatal("drain ignored actual meta live majority", err)
	}
	for _, name := range []string{"node1", "node2"} {
		for _, view := range []string{"target", "active"} {
			doc, err := c.Document(ctx, name, view)
			if err != nil || doc.Version != created.Document.Version || doc.Node("node2").State != topology.NodeActive {
				t.Fatal("refused drain changed approved lineage", name, view, doc, err)
			}
		}
	}
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal("safe retained-voter recovery could not apply original staged version", err)
	}
	assertDrained(t, ctx, c, "node2", "work", staged.Document.Version)
}
