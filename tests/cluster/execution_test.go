//go:build clustertest

package cluster

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/asalimonov/gquorum"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
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
		if observed.GroupID == group && observed.Observation != nil {
			return observed.Observation.Clone(), nil
		}
	}
	return topology.GroupStatus{}, fmt.Errorf("group %q absent from %s status", group, node)
}

// Native leadership hints can precede current-term readiness. Retry only the
// classified transient refusals; callers still verify the exact leader below.
func requestLeaderTransfer(ctx context.Context, c *Cluster, ingress, group, target string) error {
	return Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, ingress, "POST", "/api/groups/"+group+"/leader/"+target, nil, 0)
		if err != nil {
			return false, err
		}
		if response.Status == 200 {
			return true, nil
		}
		var refusal struct {
			Code fault.Code `json:"code"`
		}
		if response.Decode(&refusal) == nil && response.Status == 503 && slices.Contains([]fault.Code{fault.AuthorityPending, fault.Unavailable, fault.NotLeader, fault.ReplicaNotReady}, refusal.Code) {
			return false, requireStatus(response, nil, 200)
		}
		return true, requireStatus(response, nil, 200)
	})
}

func transferMeta(ctx context.Context, c *Cluster, ingress, target string, memberID uint64) error {
	if err := requestLeaderTransfer(ctx, c, ingress, topology.MetaGroupID, target); err != nil {
		return err
	}
	return Poll(ctx, func() (bool, error) {
		status, err := c.Status(ctx, target)
		return status.MetaLeaderID == memberID, err
	})
}

// Port of the source non-host meta-leader execution scenario. Each operation
// uses the caller's load transport; meta leadership alone grants no local FSM.
func TestRemoteApplyFromNonHostMetaLeader(t *testing.T) {
	t.Parallel()
	lease := acquire(t, matrixSpec(3))
	c := lease.Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "remote", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node2")
	if err != nil {
		t.Fatal(err)
	}
	var metaHost string
	for _, node := range created.Document.Nodes {
		if created.Document.MemberID(node.Name) == status.MetaLeaderID {
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
	if _, err := c.Approve(ctx, metaHost, staged.Document.Version); err != nil {
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
	if err := requestLeaderTransfer(ctx, c, metaHost, "remote", loadTarget.NodeName); err != nil {
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
	if _, err := c.Approve(ctx, metaHost, staged.Document.Version); err != nil {
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
	if _, err := c.Approve(ctx, metaHost, staged.Document.Version); err != nil {
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
	deleted, err := c.Mutate(ctx, metaHost, "DELETE", "/api/groups/remote", gquorum.MutationOptions{Approve: true, Actor: "cluster-test"})
	if err != nil || deleted.Document.Group("remote") != nil {
		t.Fatal("remote deletion", deleted, err)
	}
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			_, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/remote"})
			if errdefs.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Fatal("deleted remote store remains", node.Spec.Name, err)
		}
	}
}

func TestCallerRefusedDeletionRetainsDataThreeNodes(t *testing.T) {
	t.Parallel()
	callerRefusedDeletion(t, 3)
}

// Callback refusal retains exact physical debt after logical deletion. Cleanup
// retries automatically under its original approval, including after restart.
func callerRefusedDeletion(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceClusterSpec(t, matrixSpec(size))
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "occupied", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"owned": "retained"}
	if err := c.Put(ctx, "node2", "occupied", "owned", "owned", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "occupied", expected); err != nil {
		t.Fatal(err)
	}
	for _, replica := range created.Document.Group("occupied").Replicas {
		response, err := c.Request(ctx, replica.NodeName, "PUT", "/internal/test/barriers/occupied/stop_replica?refuse=true", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	response, err := c.Request(ctx, "node2", "DELETE", "/api/groups/occupied", gquorum.MutationOptions{Actor: "cluster-test"}, created.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var deleted gquorum.MutationResult
	if err := response.Decode(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted.Document.Group("occupied") != nil || deleted.Document.Version != created.Document.Version+1 {
		t.Fatal(deleted)
	}
	if err := Poll(ctx, func() (bool, error) {
		latest, err := c.Document(ctx, "node2", "latest")
		return latest.Version == deleted.Document.Version, err
	}); err != nil {
		t.Fatal("staged deletion did not reach planning ingress", err)
	}
	response, err = c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/plan", deleted.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 4 || plan.Steps[0].Kind != topology.StepStopReplica || plan.Steps[3].Kind != topology.StepDeleteGroupState {
		t.Fatal(plan)
	}
	approve := func() topology.OperationID {
		t.Helper()
		response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/approve", deleted.Document.Version), nil, 0)
		if err := requireStatus(response, err, 202); err != nil {
			t.Fatal(err)
		}
		var result struct {
			ID topology.OperationID `json:"operation_id"`
		}
		if err := response.Decode(&result); err != nil || result.ID == "" {
			t.Fatal(result, err)
		}
		return result.ID
	}
	operation := approve()
	if err := c.WaitOperation(ctx, "node2", operation); err != nil {
		t.Fatal(err)
	}
	for _, replica := range created.Document.Group("occupied").Replicas {
		if err := Poll(ctx, func() (bool, error) {
			controls, err := c.Controls(ctx, replica.NodeName)
			for _, b := range controls.Barriers {
				if b.Group != "occupied" || b.Hits == 0 {
					continue
				}
				step := plan.Steps[replica.ReplicaID-1]
				if !b.Refuse || !b.Last.Matches(operation, 1, deleted.Document.Version, step) {
					return true, fmt.Errorf("refusal lost exact action: %+v", b)
				}
				return true, err
			}
			return false, err
		}); err != nil {
			t.Fatal(err)
		}
		local, err := c.Local(ctx, replica.NodeName, "occupied")
		if err != nil || !local.Found || local.StorageIdentity == nil || local.StorageIdentity.ReplicaID != replica.ReplicaID {
			t.Fatal("refusal changed retained owner", local, err)
		}
		if err := ownDurableFiles(ctx, c, replica.NodeName, "occupied"); err != nil {
			t.Fatal(err)
		}
		page, err := c.Progress(ctx, "node2", topology.ProgressQuery{GroupID: "occupied"})
		debt, found := exactCleanup(page, *local.StorageIdentity)
		if err != nil || !found || debt.StoppedAt != nil || debt.RetiredAt != nil || debt.CollectedAt != nil {
			t.Fatal("refusal manufactured physical completion", debt, err)
		}
	}
	if err := exactOwnedValues(ctx, c, created.Document, "occupied", expected); err != nil {
		t.Fatal("refusal destroyed surviving data", err)
	}
	status, err := c.Status(ctx, "node2")
	if err != nil {
		t.Fatal(err)
	}
	var oldLeader *Node
	for _, node := range c.Nodes() {
		if created.Document.MemberID(node.Spec.Name) == status.MetaLeaderID {
			oldLeader = node
		}
	}
	if oldLeader == nil {
		t.Fatal("leader has no exact owner")
	}
	if err := oldLeader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := oldLeader.Container.CopyFileFromContainer(ctx, "/data/load/occupied/state.bolt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.LimitReader(reader, (16<<20)+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(data) == 0 || len(data) > 16<<20 {
		t.Fatal("invalid retained state", err, closeErr)
	}
	retainedDigest := sha256.Sum256(data)
	volume := oldLeader.Volume
	if err := oldLeader.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// The restarted process has no ephemeral refusal barrier. Its retained exact
	// cleanup must resume automatically without another operation attempt.
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, oldLeader.Spec.Name, "occupied")
		return !local.Found && local.StorageIdentity == nil, err
	}); err != nil {
		t.Fatal(err)
	}
	if oldLeader.Volume != volume || retiredStateDigest(ctx, t, c, oldLeader, "occupied") != retainedDigest {
		t.Fatal("restart changed quarantined physical evidence")
	}
	for _, replica := range created.Document.Group("occupied").Replicas {
		if replica.NodeName != oldLeader.Spec.Name {
			if err := c.SetBarrier(ctx, replica.NodeName, "occupied", topology.StepStopReplica, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			_, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/occupied"})
			if errdefs.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Fatal("cleanup did not retire former state", err)
		}
		prior, err := c.Operation(ctx, node.Spec.Name, operation)
		if err != nil || prior.State != topology.OperationSucceeded || prior.Attempt != 1 {
			t.Fatal(prior, err)
		}
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/groups", map[string]any{"group": gquorum.GroupSpec{ID: "occupied"}, "mutation": gquorum.MutationOptions{Actor: "cluster-test"}}, deleted.Document.Version)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("deleted group ID became reusable", err)
	}
}

func retiredStateDigest(ctx context.Context, t *testing.T, c *Cluster, node *Node, group string) [32]byte {
	t.Helper()
	archive, err := c.docker.CopyFromContainer(ctx, node.Container.GetContainerID(), client.CopyFromContainerOptions{SourcePath: "/data/load"})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Content.Close()
	reader := tar.NewReader(archive.Content)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			t.Fatal("retired state file missing")
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(header.Name, group+".quarantine-") && strings.HasSuffix(header.Name, "/state.bolt") {
			data, err := io.ReadAll(io.LimitReader(reader, (16<<20)+1))
			if err != nil || len(data) == 0 || len(data) > 16<<20 {
				t.Fatal("invalid retired state", err)
			}
			return sha256.Sum256(data)
		}
	}
}
