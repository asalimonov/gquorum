//go:build cluster

package cluster

import (
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

func groupStatus(ctx context.Context, c *Cluster, node, group string) (topology.GroupStatus, error) {
	status, err := c.Status(ctx, node)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	for _, observed := range status.Groups {
		if observed.GroupID == group {
			return observed, nil
		}
	}
	return topology.GroupStatus{}, fmt.Errorf("group %q absent from %s status", group, node)
}

func transferMeta(ctx context.Context, c *Cluster, ingress, target string, memberID uint64) error {
	response, err := c.Request(ctx, ingress, "POST", "/api/groups/_meta/leader/"+target, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return err
	}
	return Poll(ctx, func() (bool, error) {
		status, err := c.Status(ctx, target)
		return status.MetaLeader == memberID, err
	})
}

// Port of the source non-host meta-leader execution scenario. Each operation
// uses the caller's load transport; meta leadership alone grants no local FSM.
func TestRemoteApplyFromNonHostMetaLeader(t *testing.T) {
	lease := acquire(t, matrixSpec(3))
	c := lease.Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "remote", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node2")
	if err != nil {
		t.Fatal(err)
	}
	var metaHost string
	for _, node := range created.Document.Nodes {
		if created.Document.MemberID(node.Name) == status.MetaLeader {
			metaHost = node.Name
		}
	}
	if metaHost == "" {
		t.Fatal("meta leader has no approved owner", status)
	}
	if err := c.Put(ctx, metaHost, "remote", "before-growth", "before-growth", "retained"); err != nil {
		t.Fatal(err)
	}
	base := created.Document.Clone()
	removed := base.Clone()
	removed.Group("remote").Replicas = slices.DeleteFunc(removed.Group("remote").Replicas, func(replica topology.Replica) bool { return replica.NodeName == metaHost })
	staged, err := c.Stage(ctx, metaHost, removed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, metaHost, staged.Document.Version); err != nil {
		t.Fatal("remove meta leader's load replica", err)
	}
	if err := transferMeta(ctx, c, "node2", metaHost, base.MemberID(metaHost)); err != nil {
		t.Fatal(err)
	}
	remote, err := groupStatus(ctx, c, metaHost, "remote")
	if err != nil || !remote.Found || remote.HostNode == metaHost || len(remote.ConfState.Voters) != 2 {
		t.Fatal("non-host leader did not observe remote group", remote, err)
	}
	response, err := c.Request(ctx, metaHost, "GET", "/api/load/remote/keys/before-growth", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("non-host unexpectedly served a local FSM", err)
	}
	loadTarget := staged.Document.Group("remote").Replicas[0]
	response, err = c.Request(ctx, metaHost, "POST", "/api/groups/remote/leader/"+loadTarget.NodeName, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal("remote transfer", err)
	}
	if err := Poll(ctx, func() (bool, error) {
		observed, err := groupStatus(ctx, c, metaHost, "remote")
		return observed.LeaderID == loadTarget.ReplicaID, err
	}); err != nil {
		t.Fatal(err)
	}
	regrown := staged.Document.Clone()
	group := regrown.Group("remote")
	freshID, err := group.AllocateReplicaID()
	if err != nil {
		t.Fatal(err)
	}
	group.Replicas = append(group.Replicas, topology.Replica{NodeName: metaHost, ReplicaID: freshID, Role: topology.ReplicaVoter, CreatedAt: time.Now().UTC()})
	for _, old := range base.Group("remote").Replicas {
		if old.NodeName == metaHost && freshID <= old.ReplicaID {
			t.Fatal("regrowth reused retired replica identity", freshID, old)
		}
	}
	staged, err = c.Stage(ctx, metaHost, regrown)
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, metaHost, "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter} {
		if !slices.ContainsFunc(plan.Steps, func(step topology.Step) bool {
			return step.GroupID == "remote" && step.Kind == kind && step.NodeName == metaHost && step.ReplicaID == freshID
		}) {
			t.Fatal("regrowth missing exact learner-first step", kind, plan)
		}
	}
	if _, err := c.Apply(ctx, metaHost, staged.Document.Version); err != nil {
		t.Fatal("remote regrowth", err)
	}
	if err := c.WaitValue(ctx, metaHost, "remote", "before-growth", "retained"); err != nil {
		t.Fatal("fresh replica failed to catch up", err)
	}
	if err := c.Put(ctx, metaHost, "remote", "after-growth", "after-growth", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	removed = staged.Document.Clone()
	removed.Group("remote").Replicas = slices.DeleteFunc(removed.Group("remote").Replicas, func(replica topology.Replica) bool { return replica.NodeName == metaHost })
	staged, err = c.Stage(ctx, metaHost, removed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, metaHost, staged.Document.Version); err != nil {
		t.Fatal("remove regrown replica", err)
	}
	if err := transferMeta(ctx, c, "node2", metaHost, base.MemberID(metaHost)); err != nil {
		t.Fatal(err)
	}
	for _, replica := range staged.Document.Group("remote").Replicas {
		if err := c.WaitValue(ctx, replica.NodeName, "remote", "after-growth", "acknowledged"); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := c.Mutate(ctx, metaHost, "DELETE", "/api/groups/remote", meta.MutationOptions{Apply: true, Actor: "cluster-test"})
	if err != nil || deleted.Document.Group("remote") != nil {
		t.Fatal("remote deletion", deleted, err)
	}
	for _, node := range c.Nodes() {
		if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/remote"}); !errdefs.IsNotFound(err) {
			t.Fatal("deleted remote store remains", node.Spec.Name, err)
		}
	}
}
