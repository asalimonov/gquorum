//go:build clustertest

package cluster

import (
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
		if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/remote"}); !errdefs.IsNotFound(err) {
			t.Fatal("deleted remote store remains", node.Spec.Name, err)
		}
	}
}

func TestCallerRefusedDeletionRetainsDataThreeNodes(t *testing.T) {
	t.Parallel()
	callerRefusedDeletion(t, 3)
}

// Port of the source non-empty deletion refusal. Application occupancy belongs
// to the caller: refuse the first destructive callback, persist its failure,
// and retry the same desired version under a fresh authorized attempt.
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
	approve := func() topology.OperatorID {
		t.Helper()
		response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/approve", deleted.Document.Version), nil, 0)
		if err := requireStatus(response, err, 202); err != nil {
			t.Fatal(err)
		}
		var result struct {
			ID topology.OperatorID `json:"operation_id"`
		}
		if err := response.Decode(&result); err != nil || result.ID == "" {
			t.Fatal(result, err)
		}
		return result.ID
	}
	failedID := approve()
	var failed topology.OperatorStatus
	if err := Poll(ctx, func() (bool, error) {
		var err error
		failed, err = c.Operation(ctx, "node2", failedID)
		return failed.State.Terminal(), err
	}); err != nil {
		t.Fatal(err)
	}
	if failed.State != topology.OperatorFailed || failed.Attempt != 1 || failed.Version != deleted.Document.Version || failed.CurrentStep != 1 || !strings.Contains(failed.Error, "caller refuses deletion") {
		t.Fatal(failed)
	}
	assertFailure := func(restarted string) {
		t.Helper()
		for _, node := range c.Nodes() {
			if err := Poll(ctx, func() (bool, error) {
				status, err := c.Status(ctx, node.Spec.Name)
				return status.LatestVersion == deleted.Document.Version && status.TargetVersion == deleted.Document.Version && status.ActiveVersion == created.Document.Version && status.Operator != nil && *status.Operator == failed, err
			}); err != nil {
				t.Fatal(node.Spec.Name, err)
			}
		}
		serving := created.Document.Clone()
		serving.Group("occupied").Replicas = slices.DeleteFunc(serving.Group("occupied").Replicas, func(r topology.Replica) bool { return r.NodeName == restarted })
		if err := exactOwnedValues(ctx, c, serving, "occupied", expected); err != nil {
			t.Fatal("refusal destroyed surviving data", err)
		}
		for _, replica := range created.Document.Group("occupied").Replicas {
			local, err := c.Local(ctx, replica.NodeName, "occupied")
			if err != nil || local.StorageIdentity == nil || local.StorageIdentity.ReplicaID != replica.ReplicaID || replica.NodeName != restarted && (!local.Found || local.ReplicaID != replica.ReplicaID) || replica.NodeName == restarted && local.Found {
				t.Fatal("refusal changed retained owner or reopened a retired group", local, err)
			}
			if err := ownDurableFiles(ctx, c, replica.NodeName, "occupied"); err != nil {
				t.Fatal("refusal lost durable files", err)
			}
			if _, err := c.docker.ContainerStatPath(ctx, c.Node(replica.NodeName).Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/occupied"}); err != nil {
				t.Fatal("refusal removed physical store", err)
			}
		}
	}
	assertFailure("")
	hits := uint64(0)
	for _, replica := range created.Document.Group("occupied").Replicas {
		controls, err := c.Controls(ctx, replica.NodeName)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range controls.Barriers {
			if b.Group == "occupied" {
				hits += b.Hits
			}
			if b.Group == "occupied" && b.Hits > 0 && (!b.Refuse || !b.Last.Matches(failedID, 1, deleted.Document.Version, plan.Steps[0])) {
				t.Fatal("refusal lost exact authorized scope", b)
			}
		}
		for _, receipt := range controls.Receipts {
			if receipt.OperatorID == failedID {
				t.Fatal("refused attempt performed an effect", receipt)
			}
		}
	}
	if hits == 0 {
		t.Fatal("no caller observed the refused identity")
	}
	// The failed attempt survives replacement of the meta leader and its disk restart.
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
	var retainedDigest [32]byte
	wasOwner := slices.ContainsFunc(created.Document.Group("occupied").Replicas, func(r topology.Replica) bool { return r.NodeName == oldLeader.Spec.Name })
	digest := func() [32]byte {
		t.Helper()
		reader, err := oldLeader.Container.CopyFileFromContainer(ctx, "/data/load/occupied/state.bolt")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, (16<<20)+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > 16<<20 {
			t.Fatal("unreadable retained state", len(data), readErr, closeErr)
		}
		return sha256.Sum256(data)
	}
	if wasOwner {
		retainedDigest = digest()
	}
	var survivor string
	for _, node := range c.Nodes() {
		if node != oldLeader {
			survivor = node.Spec.Name
			break
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		observed, err := c.Status(ctx, survivor)
		return observed.MetaLeaderID != 0 && observed.MetaLeaderID != status.MetaLeaderID && observed.Operator != nil && *observed.Operator == failed, err
	}); err != nil {
		t.Fatal("failed execution did not survive leader replacement", err)
	}
	if err := oldLeader.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		observed, err := c.Status(ctx, oldLeader.Spec.Name)
		return observed.SessionLive && observed.MetaLeaderID != 0 && observed.Operator != nil && *observed.Operator == failed, err
	}); err != nil {
		t.Fatal("retained meta did not recover the failed execution", err)
	}
	assertFailure(oldLeader.Spec.Name)
	if wasOwner && digest() != retainedDigest {
		t.Fatal("restart changed the refused group's durable state")
	}

	// Process controls are intentionally ephemeral. Re-arm only the restarted
	// owner's control so removal below has the same exact local lifecycle.
	if wasOwner {
		response, err := c.Request(ctx, oldLeader.Spec.Name, "PUT", "/internal/test/barriers/occupied/stop_replica?refuse=true", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	// The caller permits deletion. The same desired version gets a fresh attempt.
	for _, replica := range created.Document.Group("occupied").Replicas {
		if err := c.SetBarrier(ctx, replica.NodeName, "occupied", topology.StepStopReplica, false); err != nil {
			t.Fatal(err)
		}
	}

	retryID := approve()
	if retryID == failedID {
		t.Fatal("retry reused failed attempt", retryID)
	}
	if err := c.WaitOperation(ctx, "node2", retryID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, deleted.Document.Version); err != nil {
		t.Fatal(err)
	}
	retry, err := c.Operation(ctx, "node2", retryID)
	if err != nil || retry.Attempt != 2 || retry.Version != failed.Version {
		t.Fatal(retry, err)
	}
	for _, node := range c.Nodes() {
		prior, err := c.Operation(ctx, node.Spec.Name, failedID)
		if err != nil || prior != failed {
			t.Fatal("retry rewrote historical failure", prior, err)
		}
		if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/occupied"}); !errdefs.IsNotFound(err) {
			t.Fatal("successful deletion retained usable store", node.Spec.Name, err)
		}
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/groups", map[string]any{"group": gquorum.GroupSpec{ID: "occupied"}, "mutation": gquorum.MutationOptions{Actor: "cluster-test"}}, deleted.Document.Version)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("deleted group ID became reusable", err)
	}
}
