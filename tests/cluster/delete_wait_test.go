//go:build clustertest

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

func TestFailureIsolation_DeletionStopProof(t *testing.T) {
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
		operator, err := c.Operation(ctx, ingress, operatorID)
		if err != nil {
			return false, err
		}
		if operator.State.Terminal() {
			t.Fatalf("deletion finished without unreachable former host: %+v", operator)
		}
		for _, block := range operator.Blocks {
			if block.Blocked != nil && block.Blocked.NodeName == stopped.Spec.Name {
				return operator.State.Waiting(), nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("deletion did not wait for the exact unreachable host", err)
	}
	// A second group's founding must proceed while every exact stop for this
	// deletion is still required. Host it only on the two available processes.
	var survivors []string
	for _, node := range c.Nodes() {
		if node.running.Load() {
			survivors = append(survivors, node.Spec.Name)
		}
	}
	response, err = c.Request(ctx, ingress, "POST", "/api/groups", map[string]any{
		"group": gquorum.GroupSpec{ID: "independent", Nodes: survivors,
			Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 2}, VoterConfig: voterConfig(2)},
		"mutation": gquorum.MutationOptions{Actor: "isolation-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var independent gquorum.MutationResult
	if err := response.Decode(&independent); err != nil {
		t.Fatal(err)
	}
	independent.OperationID = startApply(ctx, t, c, ingress, independent.Document.Version)
	if err := c.WaitOperation(ctx, ingress, independent.OperationID); err != nil {
		t.Fatal("deletion stop blocked independent founding", err)
	}
	// Operation success is not the caller's local serving-observation barrier.
	// Retry only that transient refusal, preserving the exact command identity.
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, ingress, "PUT", "/api/load/independent/keys/during-delete",
			map[string]string{"request_id": "during-delete", "value": "available"}, 0)
		if err == nil && response.Status == 503 {
			var refusal struct {
				Code string `json:"code"`
			}
			if decodeErr := response.Decode(&refusal); decodeErr != nil {
				return false, decodeErr
			}
			if refusal.Code == "replica_not_ready" {
				return false, nil
			}
		}
		return true, requireStatus(response, err, 200)
	}); err != nil {
		t.Fatal(err)
	}
	old := storage.Identity{ClusterID: c.ID, GroupID: "delete-wait", NodeName: stopped.Spec.Name,
		ReplicaID: replicaID(created.Document, "delete-wait", stopped.Spec.Name)}
	for _, name := range survivors {
		page, err := c.Progress(ctx, name, topology.ProgressQuery{GroupID: "delete-wait"})
		debt, found := exactCleanup(page, old)
		if err != nil || page.FullyConverged.Version != created.Document.Version || page.TargetVersion != independent.Document.Version ||
			len(page.Groups) != 1 || !page.Groups[0].Deleting || page.Groups[0].Approved != nil ||
			page.Groups[0].ConvergedVersion != created.Document.Version || page.Groups[0].OperationID != operatorID ||
			!found || debt.StoppedAt != nil || debt.RetiredAt != nil || debt.CollectedAt != nil {
			t.Fatal("deletion manufactured exact stop or collection proof", name, page, err)
		}
		op, err := c.Operation(ctx, name, operatorID)
		if err != nil || op.State.Terminal() {
			t.Fatal("later group completion changed pending deletion", op, err)
		}
	}
	if _, err := c.docker.ContainerStatPath(ctx, stopped.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/delete-wait"}); err != nil {
		t.Fatal("unreachable host lost its retained state before stop acknowledgement", err)
	}
	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperation(ctx, ingress, operatorID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitFullyConverged(ctx, independent.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		// Required stops converge the declaration. Physical collection is later
		// detached work and must be observed separately on each retained volume.
		if err := Poll(ctx, func() (bool, error) {
			_, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/delete-wait"})
			if errdefs.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Fatal("collection did not release former-host state", node.Spec.Name, err)
		}
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/load/delete-wait/keys/retained", nil, 0)
		if err := requireStatus(response, err, 503); err != nil {
			t.Fatal("deleted group still serves", err)
		}
	}
}
