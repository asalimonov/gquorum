//go:build clustertest

package cluster

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
)

func exactCleanup(page topology.ProgressPage, id storage.Identity) (topology.CleanupProgress, bool) {
	for _, row := range page.Cleanup {
		if row.Identity == id {
			return row, true
		}
	}
	return topology.CleanupProgress{}, false
}

func TestFailureIsolation_RemovedHostCleanup(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Retention = time.Nanosecond // Exercise collection after retirement in this case.
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "before", "before", "retained"); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"before": "retained"}
	if err := exactOwnedValues(ctx, c, created.Document, "work", values); err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node1", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := requestLeaderTransfer(ctx, c, "node1", "work", "node1"); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node3", "work")
	if err != nil || before.StorageIdentity == nil {
		t.Fatal("missing retained old identity", before, err)
	}
	old := *before.StorageIdentity
	if err := c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	desired := created.Document.Clone()
	group := desired.Group("work")
	group.Replicas = slices.DeleteFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == "node3" })
	group.VoterConfig.TargetVoters = 2
	staged, err := c.Stage(ctx, "node1", desired)
	if err != nil {
		t.Fatal(err)
	}
	removed := startApply(ctx, t, c, "node1", staged.Document.Version)
	if err := c.WaitOperation(ctx, "node1", removed); err != nil {
		t.Fatal("unreachable removed host blocked native convergence", err)
	}
	if err := c.WaitFullyConverged(ctx, staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "work", NodeName: "node3"})
	debt, found := exactCleanup(page, old)
	if err != nil || !found || debt.MemberID != before.Identity.MemberID || debt.OperationID != removed ||
		debt.RemovedAt == nil || debt.StoppedAt != nil || debt.RetiredAt != nil || debt.CollectedAt != nil {
		t.Fatal("native removal fabricated unreachable-host cleanup", debt, page, err)
	}
	if _, err := c.docker.ContainerStatPath(ctx, c.Node("node3").Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/work"}); err != nil {
		t.Fatal("unreachable host lost retained storage", err)
	}
	if err := c.Put(ctx, "node1", "work", "without-host", "without-host", "survivors"); err != nil {
		t.Fatal("surviving quorum stopped serving", err)
	}
	values["without-host"] = "survivors"
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Wait for exact durable proof, not for the mere absence of an old directory.
	if err := Poll(ctx, func() (bool, error) {
		page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "work", NodeName: "node3"})
		debt, found := exactCleanup(page, old)
		return found && debt.StoppedAt != nil && debt.RetiredAt != nil && debt.CollectedAt != nil, err
	}); err != nil {
		t.Fatal("returned host did not settle its retained obligation", err)
	}
	returned, err := c.Local(ctx, "node3", "work")
	if err != nil || returned.Found || returned.StorageIdentity != nil || returned.Identity.MemberID != before.Identity.MemberID || returned.Identity.InstanceID == before.Identity.InstanceID {
		t.Fatal("return published the removed identity or replaced its meta pin", returned, err)
	}
	response, err := c.Request(ctx, "node3", "GET", "/api/load/work/keys/before", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("returned old replica served after removal", err)
	}
	view, err := c.Authority(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	var stale load.ActionRequest
	for _, action := range view.Actions {
		if action.Authorization.OperationID == removed && action.Authorization.Kind == topology.StepCleanupReplicaState && action.LocalTarget.ReplicaID == old.ReplicaID {
			stale = action
		}
	}
	if stale.Grant.Key.Revision == 0 {
		t.Fatal("missing exact cleanup delivery for successor replay", view)
	}
	grown := staged.Document.Clone()
	group = grown.Group("work")
	fresh := group.NextReplicaID
	group.NextReplicaID++
	now := time.Now().UTC()
	group.Replicas = append(group.Replicas, topology.Replica{NodeName: "node3", ReplicaID: fresh, Role: topology.ReplicaVoter, CreatedAt: now, UpdatedAt: now})
	group.VoterConfig.TargetVoters = 3
	staged, err = c.Stage(ctx, "node1", grown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node1", staged.Document.Version); err != nil {
		t.Fatal("fresh successor could not join", err)
	}
	if err := exactOwnedValues(ctx, c, staged.Document, "work", values); err != nil {
		t.Fatal("successor did not recover surviving data", err)
	}
	successor, err := c.Local(ctx, "node3", "work")
	if err != nil || !successor.Found || successor.ReplicaID != fresh || fresh <= old.ReplicaID {
		t.Fatal("replacement reused the former replica", successor, err)
	}
	stale.To.Node = successor.Identity
	requireFencedReplay(ctx, t, c, successor, stale)
	if err := c.Put(ctx, "node3", "work", "after-replay", "after-replay", "safe"); err != nil {
		t.Fatal("stale cleanup damaged the successor", err)
	}
	values["after-replay"] = "safe"
	if err := exactOwnedValues(ctx, c, staged.Document, "work", values); err != nil {
		t.Fatal(err)
	}
}
