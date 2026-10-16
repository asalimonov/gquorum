//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

func TestAuthorityHostRevocationWaitsForExactRetainedProcessReturnThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, original := readProcessCluster(t, "node3")
	if err := transferMeta(ctx, c, "node2", "node2", original.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	owner, err := c.Local(ctx, "node1", "work")
	if err != nil || !owner.Found {
		t.Fatal(owner, err)
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepStopReplica, true); err != nil {
		t.Fatal(err)
	}
	held := true
	t.Cleanup(func() {
		if !held {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.SetBarrier(cleanup, "node1", "work", topology.StepStopReplica, false); err != nil {
			t.Error("host action barrier cleanup", err)
		}
	})
	response, err := c.Request(ctx, "node2", "DELETE", "/api/groups/work", gquorum.MutationOptions{Actor: "cluster-test"}, original.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var deleted gquorum.MutationResult
	if err := response.Decode(&deleted); err != nil {
		t.Fatal(err)
	}
	old := startApply(ctx, t, c, "node2", deleted.Document.Version)
	var delayed load.ActionRequest
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Authority(ctx, "node1", "work")
		for _, action := range view.Actions {
			if action.Authorization.OperatorID == old && action.Authorization.Kind == topology.StepStopReplica {
				delayed = action
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("old stop did not reach exact host delivery", err)
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "node1"}
	if delayed.To.Node != owner.Identity || delayed.LocalTarget.ReplicaID != owner.ReplicaID || len(delayed.Grant.Resources) != 1 || delayed.Grant.Resources[0] != resource {
		t.Fatal("old stop lost exact host/storage scope", delayed)
	}
	victim := c.Node("node1")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	held = false // The terminated process owns the only barrier; no replacement inherits it.
	if _, err := c.docker.ContainerStatPath(ctx, victim.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/work"}); err != nil {
		t.Fatal("unconsumed stop lost retained application storage", err)
	}
	// A fresh approval supersedes the held effect while keeping deletion intent.
	// A deleted group ID is permanently retired and cannot be restored/reused.
	newStage, err := c.Stage(ctx, "node2", deleted.Document)
	if err != nil {
		t.Fatal(err)
	}
	newOp := startApply(ctx, t, c, "node2", newStage.Document.Version)
	for _, node := range []string{"node2", "node3"} {
		if err := Poll(ctx, func() (bool, error) {
			status, err := c.Revocation(ctx, node, old)
			if err != nil || !status.RevocationRequested {
				return false, err
			}
			groupComplete, hostPending := false, false
			for _, obligation := range status.Obligations {
				groupComplete = groupComplete || obligation.Resource == (topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}) && obligation.Evidence != nil
				hostPending = hostPending || obligation.Resource == resource && obligation.Evidence == nil
			}
			if status.RevocationComplete || !hostPending {
				return true, fmt.Errorf("unreachable host manufactured revocation completion: %+v", status)
			}
			return groupComplete, nil
		}); err != nil {
			t.Fatal("independent group evidence did not preserve pending host obligation", node, err)
		}
		op, err := c.Operation(ctx, node, newOp)
		if err != nil || op.State.Terminal() {
			t.Fatal("successor bypassed an unreachable resource fence", op, err)
		}
		active, err := c.Document(ctx, node, "active")
		if err != nil || active.Version != original.Version || active.Group("work") == nil {
			t.Fatal("pending host revocation advanced Active", active, err)
		}
		view, err := c.Authority(ctx, node, "work")
		if err != nil {
			t.Fatal(err)
		}
		for _, action := range view.Actions {
			if action.Authorization.OperatorID == newOp {
				t.Fatal("successor issued effects before inherited host revocation", action)
			}
		}
	}
	value, err := c.Read(ctx, "node3", "work", "key")
	requireReadValue(t, value, err, "node3", "before")
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperation(ctx, "node2", newOp); err != nil {
		for _, name := range []string{"node1", "node2", "node3"} {
			view, viewErr := c.Authority(ctx, name, "work")
			for _, action := range view.Actions {
				if action.Authorization.OperatorID == newOp && action.Authorization.Kind == topology.StepDeleteGroupState {
					t.Logf("delete request %s: %+v", name, action)
				}
			}
			for _, result := range view.Receipts {
				if result.Authorization.OperatorID == newOp && result.Authorization.Kind == topology.StepDeleteGroupState {
					t.Logf("delete receipt %s: %+v", name, result)
				}
			}
			t.Logf("authority observation %s: %v", name, viewErr)
		}
		t.Fatal("returning retained host could not finish exact fence and successor cleanup", err)
	}
	if err := c.WaitActive(ctx, newStage.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := c.Revocation(ctx, "node3", old)
	if err != nil || !status.RevocationRequested || !status.RevocationComplete {
		t.Fatal("completed operator did not retain durable host revocation", status, err)
	}
	returned, err := c.Local(ctx, "node1", topology.MetaGroupID)
	if err != nil || returned.Identity.MemberID != owner.Identity.MemberID || returned.Identity.InstanceID == owner.Identity.InstanceID {
		t.Fatal("retained return replaced member pin or reused process identity", returned, err)
	}
	// Deliver the unchanged old grant/action to the currently authenticated host
	// process. Historical target/storage identity remains exact, even when absent.
	delayed.To.Node = returned.Identity
	response, err = c.ReplayAction(ctx, "node1", returned.Identity, delayed)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("old host action crossed retained durable fence", err)
	}
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if err := response.Decode(&refusal); err != nil || refusal.Code != fault.AuthorityFenced {
		t.Fatal("absent data substituted for exact stale-authority refusal", refusal, err)
	}
	for _, node := range c.Nodes() {
		if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/work"}); !errdefs.IsNotFound(err) {
			t.Fatal("completed cleanup or old replay retained/recreated storage", node.Spec.Name, err)
		}
		local, err := c.Local(ctx, node.Spec.Name, "work")
		if err != nil || local.Found || local.StorageIdentity != nil {
			t.Fatal("deleted replica reopened after old host action", local, err)
		}
	}
}
