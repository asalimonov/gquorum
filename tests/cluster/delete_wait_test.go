//go:build clustertest

package cluster

import (
	"context"
	"github.com/asalimonov/gquorum"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

func TestDeletionWaitsForUnreachableFormerHostBeforeDeletingState(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "delete-wait"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "delete-wait", "retained", "retained", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node2")
	if err != nil {
		t.Fatal(err)
	}
	var stopped *Node
	for _, node := range c.Nodes() {
		if created.Document.MemberID(node.Spec.Name) != status.MetaLeaderID {
			stopped = node
			break
		}
	}
	if stopped == nil {
		t.Fatal("no nonleader former host")
	}
	if err := stopped.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	ingress := "node1"
	if stopped.Spec.Name == ingress {
		ingress = "node3"
	}
	response, err := c.Request(ctx, ingress, "DELETE", "/api/groups/delete-wait", gquorum.MutationOptions{Actor: "cluster-test"}, created.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var staged gquorum.MutationResult
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	operatorID := startApply(ctx, t, c, ingress, staged.Document.Version)
	if err := Poll(ctx, func() (bool, error) {
		operator, err := c.Operator(ctx, ingress, operatorID)
		if err != nil {
			return false, err
		}
		if operator.State.Terminal() {
			t.Fatalf("deletion finished without unreachable former host: %+v", operator)
		}
		return operator.State.Waiting() && operator.NodeName == stopped.Spec.Name, nil
	}); err != nil {
		t.Fatal("deletion did not wait for the exact unreachable host", err)
	}
	for _, node := range c.Nodes() {
		if !node.running.Load() {
			continue
		}
		active, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || active.Version != created.Document.Version || active.Group("delete-wait") == nil {
			t.Fatal("deletion activated before every stop acknowledgement", active, err)
		}
		target, err := c.Document(ctx, node.Spec.Name, "target")
		if err != nil || target.Version != staged.Document.Version || target.Group("delete-wait") != nil {
			t.Fatal("approved deletion target was not retained", target, err)
		}
		controls, err := c.Controls(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		for _, receipt := range controls.Receipts {
			if receipt.OperatorID == operatorID && receipt.Kind == topology.StepDeleteGroupState {
				t.Fatal("state deletion effect ran before every host stopped", receipt)
			}
		}
	}
	if _, err := c.docker.ContainerStatPath(ctx, stopped.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/delete-wait"}); err != nil {
		t.Fatal("unreachable host lost its retained state before stop acknowledgement", err)
	}
	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, ingress, operatorID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		_, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/delete-wait"})
		if !errdefs.IsNotFound(err) {
			t.Fatal("completed deletion retained former-host state", node.Spec.Name, err)
		}
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/load/delete-wait/keys/retained", nil, 0)
		if err := requireStatus(response, err, 503); err != nil {
			t.Fatal("deleted group still serves", err)
		}
	}
}
