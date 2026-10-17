//go:build clustertest

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func assertDrained(ctx context.Context, t *testing.T, c *Cluster, name, group string, version uint64) {
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
	// Drain converges after joined stop; physical retirement runs independently.
	if err := Poll(ctx, func() (bool, error) {
		_, err := c.docker.ContainerStatPath(ctx, c.Node(name).Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/" + group})
		if errdefs.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatal("drained node retained removed load store", err)
	}
}

func TestDrainViaNonLeaderPreservesMetaMembership(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node3", "work", "before-drain", "before-drain", "retained"); err != nil {
		t.Fatal(err)
	}
	drained, err := c.Mutate(ctx, "node3", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Approve: true, Actor: "cluster-test"})
	if err != nil || drained.OperationID == "" {
		t.Fatal("follower did not forward both stage and apply", drained, err)
	}
	assertDrained(ctx, t, c, "node3", "work", drained.Document.Version)
	status, err := c.Status(ctx, "node3")
	if err != nil || status.MetaLeaderID != created.Document.MemberID("node1") {
		t.Fatal("issuer became leader instead of forwarding", status, err)
	}
	path := fmt.Sprintf("/api/topology/%d/approve", drained.Document.Version)
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
	if err := requireStatus(response, err, 202); err != nil || !bytes.Contains(response.Data, []byte(drained.OperationID)) {
		t.Fatal("repeated apply created another attempt", response, err)
	}
	activated, err := c.Mutate(ctx, "node3", "POST", "/api/nodes/node3/activate", gquorum.MutationOptions{Approve: true, Actor: "cluster-test"})
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
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 95*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/node2/drain", gquorum.MutationOptions{Actor: "cluster-test"}, created.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var staged gquorum.MutationResult
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
		response, err := c.Request(ctx, "node1", "POST", fmt.Sprintf("/api/topology/%d/approve", staged.Document.Version), nil, 0)
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
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal("safe retained-voter recovery could not apply original staged version", err)
	}
	assertDrained(ctx, t, c, "node2", "work", staged.Document.Version)
}

func TestForceRetireDeadNodePreservesSafetyThreeNodes(t *testing.T) {
	t.Parallel()
	forceRetireDeadNode(t, 3)
}

func forceRetireDeadNode(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"before-retirement": "retained"}
	if err := c.Put(ctx, "node1", "work", "before-retirement", "before-retirement", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	oldMember := created.Document.MemberID("node3")
	oldReplica := replicaID(created.Document, "work", "node3")
	victim := c.Node("node3")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", "work")
		return err == nil && status.LeaderID != 0 && status.HealthKnown && len(status.LiveVoters) >= 2, err
	}); err != nil {
		t.Fatal("surviving load majority did not elect after abrupt loss", err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/node3/force-retire", gquorum.RetireOptions{Reason: "disk destroyed", Mutation: gquorum.MutationOptions{Actor: "operator", Approve: true}}, created.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal("forced retirement was not durably accepted by healthy majority", err)
	}
	var retired gquorum.MutationResult
	if err := response.Decode(&retired); err != nil {
		t.Fatal(err)
	}
	audit := retired.Document.Audit
	if audit == nil || !audit.Forced || audit.Operation != topology.AuditOperationForceRetireNode || audit.TargetNode != "node3" || audit.Reason != "disk destroyed" || audit.StagedBy != "operator" || audit.StagedAt.IsZero() {
		t.Fatal("retirement lost durable audit", audit)
	}
	if retired.Document.Node("node3").State != topology.NodeExcluded || retired.Document.MemberID("node3") != 0 || replicaID(retired.Document, "work", "node3") != 0 {
		t.Fatal("retired identities remain declared", retired.Document)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", topology.MetaGroupID)
		return err == nil && !slices.Contains(status.ConfState.Voters, oldMember), err
	}); err != nil {
		t.Fatal("forced retirement did not remove the dead metadata voter", err)
	}
	op, err := c.Operation(ctx, "node1", topology.OperationID(retired.OperationID))
	active, activeErr := c.Document(ctx, "node1", "active")
	if err != nil || activeErr != nil || op.State.Terminal() || active.Version != created.Document.Version {
		t.Fatal("unreachable required host was falsely acknowledged", op, active, err, activeErr)
	}
	// Forced intent does not fabricate resource receipts. Returning the retained
	// process supplies exact host fencing/cleanup before Active can advance.
	if err := victim.Start(ctx); err != nil {
		t.Fatal("retained host could not return for required retirement", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
	err = c.WaitOperation(waitCtx, "node1", topology.OperationID(retired.OperationID))
	cancelWait()
	if err != nil {
		operator, operatorErr := c.Operation(ctx, "node1", topology.OperationID(retired.OperationID))
		runtime, runtimeErr := c.runtime(ctx, "node3")
		t.Fatal("returning host could not complete exact forced retirement", err, operator, operatorErr, runtime, runtimeErr)
	}
	for _, n := range c.Nodes() {
		if n.Spec.Name == "node3" {
			continue
		}
		own, err := c.Local(ctx, n.Spec.Name, topology.MetaGroupID)
		if err != nil || len(own.Status.ConfState.Voters) != size-1 || slices.Contains(own.Status.ConfState.Voters, oldMember) {
			t.Fatal("actual meta membership kept retired voter", own, err)
		}
	}
	if err := exactOwnedValues(ctx, c, retired.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	// Probe repeatedly across real session/health ticks; a single early 503 would
	// allow a stale process to become ready after the assertion had finished.
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		response, err := c.Request(ctx, "node3", "GET", "/health/ready", nil, 0)
		if err == nil && response.Status == 200 {
			t.Fatal("forcibly retired stale process resumed serving")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": joiningNode(victim.Spec), "mutation": gquorum.MutationOptions{Actor: "operator"}}, current.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var replacement gquorum.MutationResult
	if err := response.Decode(&replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.OperationID != "" || replacement.Document.MemberID("node3") <= oldMember || replicaID(replacement.Document, "work", "node3") <= oldReplica {
		t.Fatal("staged replacement reused retired identities or self-approved", replacement)
	}
	active, err = c.Document(ctx, "node1", "active")
	if err != nil || active.Version != retired.Document.Version || active.MemberID("node3") != 0 {
		t.Fatal("replacement declaration changed approved membership", active, err)
	}
	t.Logf("METRIC nodes=%d retired_meta=%d replacement_meta=%d retired_load=%d replacement_load=%d audit_preserved=true", size, oldMember, replacement.Document.MemberID("node3"), oldReplica, replicaID(replacement.Document, "work", "node3"))
}
