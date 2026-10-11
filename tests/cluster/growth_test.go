//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func stageActivation(t *testing.T, ctx context.Context, c *Cluster, name string) (gquorum.MutationResult, topology.Plan) {
	t.Helper()
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/"+name+"/activate", gquorum.MutationOptions{Actor: "cluster-test"}, base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var staged gquorum.MutationResult
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	// Acknowledgement on node2 does not imply node1 has applied the document.
	// Plan against node1 only after observing this exact staged version there.
	if err := Poll(ctx, func() (bool, error) {
		doc, err := c.Document(ctx, "node1", "latest")
		if err != nil {
			return false, err
		}
		if doc.Version > staged.Document.Version {
			return true, fmt.Errorf("unexpected topology change before plan: %d > %d", doc.Version, staged.Document.Version)
		}
		return doc.Version == staged.Document.Version, nil
	}); err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node1", "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	return staged, plan
}
func startApply(t *testing.T, ctx context.Context, c *Cluster, node string, version uint64) topology.OperatorID {
	t.Helper()
	var id topology.OperatorID
	// A follower loss can interrupt leader observation after the intent was
	// staged. Apply is idempotent for this exact version; retry only transient
	// unavailable responses, and fail immediately on any other refusal.
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, node, "POST", fmt.Sprintf("/api/topology/%d/approve", version), nil, 0)
		if err != nil {
			return false, err
		}
		if response.Status == 503 {
			return false, requireStatus(response, nil, 202)
		}
		if err := requireStatus(response, nil, 202); err != nil {
			return true, err
		}
		var result struct {
			ID topology.OperatorID `json:"operator_id"`
		}
		if err := response.Decode(&result); err != nil {
			return true, err
		}
		if result.ID == "" {
			return true, fmt.Errorf("apply lacks exact execution identity: %s", response.Data)
		}
		id = result.ID
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func replicaID(doc topology.Document, group, name string) uint64 {
	for _, replica := range doc.Group(group).Replicas {
		if replica.NodeName == name {
			return replica.ReplicaID
		}
	}
	return 0
}

func TestRuntimeGrowthExposesLearnerBeforePromotionAndSurvivesRestart(t *testing.T) {
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "before-grow", "before-grow", "retained"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.SetBarrier(ctx, node.Spec.Name, "work", topology.StepPromoteVoter, true); err != nil {
			t.Fatal(err)
		}
	}
	staged, plan := stageActivation(t, ctx, c, "node3")
	fresh := replicaID(staged.Document, "work", "node3")
	if fresh <= replicaID(created.Document, "work", "node3") {
		t.Fatal("activation reused a removed identity", fresh)
	}
	var promotion topology.Step
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter} {
		index := slices.IndexFunc(plan.Steps, func(step topology.Step) bool {
			return step.GroupID == "work" && step.Kind == kind && step.NodeName == "node3" && step.ReplicaID == fresh
		})
		if index < 0 {
			t.Fatal("missing exact learner-first step", kind, plan)
		}
		if kind == topology.StepPromoteVoter {
			promotion = plan.Steps[index]
		}
	}
	id := startApply(t, ctx, c, "node2", staged.Document.Version)
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			controls, err := c.Controls(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			for _, b := range controls.Barriers {
				if b.Hits > 0 && b.Last.Matches(id, 1, staged.Document.Version, promotion) {
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("promotion never reached the exact effect barrier", err)
	}
	local, err := c.Local(ctx, "node3", "work")
	if err != nil || !local.Found || local.ReplicaID != fresh || !slices.Contains(local.Status.ConfState.Learners, fresh) || slices.Contains(local.Status.ConfState.Voters, fresh) || local.Values["before-grow"] != "retained" {
		t.Fatal("returning process was not a caught-up learner before promotion", local, err)
	}
	beforeInstance := local.Identity.InstanceID
	response, err := c.Request(ctx, "node3", "GET", "/api/load/work/keys/before-grow", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("unapproved learner served application reads", err)
	}
	for i := range 8 {
		if err := c.Put(ctx, "node1", "work", fmt.Sprintf("during-grow-%d", i), fmt.Sprintf("during-grow-%d", i), "acknowledged"); err != nil {
			t.Fatal("voter traffic stalled during learner growth", err)
		}
	}
	for _, node := range c.Nodes() {
		if err := c.SetBarrier(ctx, node.Spec.Name, "work", topology.StepPromoteVoter, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitOperator(ctx, "node2", id); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	local, err = c.Local(ctx, "node3", "work")
	if err != nil || !local.Found || local.ReplicaID != fresh || local.Identity.InstanceID == beforeInstance || local.Identity.MemberID != created.Document.MemberID("node3") || !slices.Contains(local.Status.ConfState.Voters, fresh) || slices.Contains(local.Status.ConfState.Learners, fresh) {
		t.Fatal("promoted exact replica did not survive process restart", local, err)
	}
	for i := range 8 {
		if err := c.WaitValue(ctx, "node3", "work", fmt.Sprintf("during-grow-%d", i), "acknowledged"); err != nil {
			t.Fatal("recovering voter lost acknowledged growth traffic", err)
		}
	}
}

func TestCompactedLearnerCatchesUpFromFullSnapshot(t *testing.T) {
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	if _, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"}); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	// Retain the source's nonempty >1000-command workload, with exact values.
	for i := range 1100 {
		key := fmt.Sprintf("compact-%04d", i)
		if err := c.Put(ctx, fmt.Sprintf("node%d", i%2+1), "work", key, key, key); err != nil {
			t.Fatal("compaction workload", i, err)
		}
	}
	status, err := groupStatus(ctx, c, "node2", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Snapshot(ctx, status.HostNode, "work"); err != nil {
		t.Fatal(err)
	}
	leader, err := c.Local(ctx, status.HostNode, "work")
	if err != nil || leader.Storage.SnapshotIndex <= before.Storage.SnapshotIndex || leader.Storage.SnapshotBytes == 0 || len(leader.Values) != 1100 {
		t.Fatal("workload did not produce a durable compacted snapshot", leader.Storage, len(leader.Values), err)
	}
	grown, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/node3/activate", gquorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	fresh := replicaID(grown.Document, "work", "node3")
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "work")
		if err != nil || !local.Found || len(local.Values) != 1100 {
			return false, err
		}
		if local.ReplicaID != fresh || local.ReceivedSnapshots <= before.ReceivedSnapshots || local.Storage.SnapshotIndex < leader.Storage.SnapshotIndex || !slices.Contains(local.Status.ConfState.Voters, fresh) {
			return false, fmt.Errorf("joining voter lacks the compacted snapshot: %+v", local.Storage)
		}
		for i := range 1100 {
			key := fmt.Sprintf("compact-%04d", i)
			if local.Values[key] != key {
				return true, fmt.Errorf("acknowledged command %s missing from recovering replica", key)
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("compacted learner did not recover exact full state", err)
	}
	if err := c.Put(ctx, "node3", "work", "after-snapshot", "after-snapshot", "acknowledged"); err != nil {
		t.Fatal("recovered voter cannot accept traffic", err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "work", "after-snapshot", "acknowledged"); err != nil {
			t.Fatal(err)
		}
	}
}
