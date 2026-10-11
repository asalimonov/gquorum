//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
)

func joiningNode(spec NodeSpec) topology.Node {
	return topology.Node{Name: spec.Name, Type: spec.Type, Labels: map[string]string{
		"example.meta_address": spec.Name + ":9000",
		"example.load_address": spec.Name + ":9001",
		"example.http_address": spec.Name + ":8080",
	}}
}

func TestMetaAdmissionUsesLearnerBeforeServingAndRetainsIdentityOnRestart(t *testing.T) {
	testMetaAdmission(t, false)
}

func TestAdmissionOperatorResumesAfterMetaLeaderTransferWithoutNewAttempt(t *testing.T) {
	testMetaAdmission(t, true)
}

func testMetaAdmission(t *testing.T, transferDuringLearner bool) {
	t.Helper()
	spec := matrixSpec(3)
	spec.Limits.MaxNodes, spec.Limits.MaxReplicas = 4, 4
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", metaservice.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "before-join", "before-join", "retained"); err != nil {
		t.Fatal(err)
	}
	// Raising the desired bound does not exceed Limits or invent another cap.
	placement := created.Document.Clone()
	placement.Placement.MaxReplicas = 4
	staged, err := c.Stage(ctx, "node2", placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	node, err := c.PrepareJoinNode(ctx, NodeSpec{Name: "node4", Type: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	for _, seed := range c.Spec.Nodes {
		if err := c.SetBarrier(ctx, seed.Name, "work", topology.StepPromoteVoter, true); err != nil {
			t.Fatal(err)
		}
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{
		"node": joiningNode(node.Spec), "mutation": metaservice.MutationOptions{Actor: "cluster-test", Apply: true},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var admitted metaservice.MutationResult
	if err := response.Decode(&admitted); err != nil || admitted.OperatorID == "" {
		t.Fatal("admission did not start an exact execution", admitted, err)
	}
	member := admitted.Document.MemberID("node4")
	loadID := replicaID(admitted.Document, "work", "node4")
	if member == 0 || loadID == 0 {
		t.Fatal("admission did not allocate exact meta/load identities", admitted)
	}
	started := make(chan error, 1)
	go func() { started <- node.Start(ctx) }()
	defer func() { cancel(); <-started }()
	learnerSeen := false
	transferred := false
	var replacementMember uint64
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := c.Status(ctx, "node1")
		if err == nil {
			for _, group := range status.Groups {
				if group.GroupID == topology.MetaGroupID && slices.Contains(group.ConfState.Learners, member) {
					learnerSeen = true
					if transferDuringLearner && !transferred {
						target := "node1"
						if admitted.Document.MemberID(target) == status.MetaLeader {
							target = "node2"
						}
						replacementMember = admitted.Document.MemberID(target)
						if err := transferMeta(ctx, c, "node3", target, replacementMember); err != nil {
							t.Fatal("transfer during admitted meta learner execution", err)
						}
						transferred = true
					}
				}
			}
			if status.Operator != nil && status.Operator.ID == admitted.OperatorID && status.Operator.GroupID == "work" && status.Operator.CurrentStep > 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("meta admission did not reach load growth", err)
		case <-ticker.C:
		}
	}
	if transferDuringLearner && !transferred {
		t.Fatal("execution never transferred while the new meta member was a learner")
	}
	if !learnerSeen {
		t.Fatal("new meta member was never observed as a learner")
	}
	select {
	case err := <-started:
		started <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("joining process did not start")
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node4", "work")
		return err == nil && local.Found && local.ReplicaID == loadID && slices.Contains(local.Status.ConfState.Learners, loadID) && local.Values["before-join"] == "retained", err
	}); err != nil {
		t.Fatal("joining node never became a caught-up load learner", err)
	}
	local, err := c.Local(ctx, "node4", "work")
	if err != nil || local.Identity.MemberID != member {
		t.Fatal("joining process owns another meta identity", local.Identity, err)
	}
	instance := local.Identity.InstanceID
	if transferDuringLearner {
		operator, err := c.Operator(ctx, "node2", admitted.OperatorID)
		if err != nil || operator.ID != admitted.OperatorID || operator.Attempt != 1 || operator.Version != admitted.Document.Version || operator.State.Terminal() {
			t.Fatal("leader replacement changed or finished the paused execution", operator, err)
		}
		status, err := c.Status(ctx, "node2")
		if err != nil || status.MetaLeader != replacementMember {
			t.Fatal("replacement meta leader did not retain the ongoing execution", status.MetaLeader, err)
		}
		// The stored execution's current exact step is exposed by the authorized
		// receiver, even if planning against already changed membership differs.
		if err := Poll(ctx, func() (bool, error) {
			current, err := c.Operator(ctx, "node2", admitted.OperatorID)
			if err != nil {
				return false, err
			}
			if current.ID != admitted.OperatorID || current.Attempt != 1 || current.Version != admitted.Document.Version || current.State.Terminal() {
				return true, fmt.Errorf("replacement changed the paused execution: %+v", current)
			}
			for _, seed := range c.Spec.Nodes {
				controls, err := c.Controls(ctx, seed.Name)
				if err != nil {
					return false, err
				}
				for _, barrier := range controls.Barriers {
					a := barrier.Last
					if barrier.Hits > 0 && a.OperatorID == admitted.OperatorID && a.Attempt == 1 && a.Version == admitted.Document.Version && a.StepSeq == current.CurrentStep && a.Kind == topology.StepPromoteVoter && a.GroupID == "work" && a.NodeName == "node4" && a.ReplicaID == loadID {
						return true, nil
					}
				}
			}
			return false, nil
		}); err != nil {
			t.Fatal("replacement controller did not resume the exact authorized promotion", err)
		}
	}
	response, err = c.Request(ctx, "node4", "GET", "/health/ready", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("joining learner became application-ready before approval completed", err)
	}
	response, err = c.Request(ctx, "node4", "GET", "/api/load/work/keys/before-join", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("joining learner served application state", err)
	}
	for _, seed := range c.Spec.Nodes {
		if err := c.SetBarrier(ctx, seed.Name, "work", topology.StepPromoteVoter, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitOperator(ctx, "node2", admitted.OperatorID); err != nil {
		t.Fatal(err)
	}
	if transferDuringLearner {
		operator, err := c.Operator(ctx, "node2", admitted.OperatorID)
		if err != nil || operator.Attempt != 1 || operator.ID != admitted.OperatorID || operator.State != topology.OperatorSucceeded {
			t.Fatal("resumed admission created another attempt", operator, err)
		}
	}
	if err := c.WaitActive(ctx, admitted.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node4", "work", "after-join", "after-join", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	for _, current := range c.Nodes() {
		if err := c.WaitValue(ctx, current.Spec.Name, "work", "after-join", "acknowledged"); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	local, err = c.Local(ctx, "node4", "work")
	if err != nil || !local.Found || local.Identity.InstanceID == instance || local.Identity.MemberID != member || local.ReplicaID != loadID || !slices.Contains(local.Status.ConfState.Voters, loadID) || local.Values["before-join"] != "retained" || local.Values["after-join"] != "acknowledged" {
		t.Fatal("same-store joining restart lost its exact state/identity", local, err)
	}
	for _, current := range c.Nodes() {
		latest, err := c.Document(ctx, current.Spec.Name, "latest")
		if err != nil || latest.Version != admitted.Document.Version || latest.MemberID("node4") != member {
			t.Fatal("retained restart churned the admission document", current.Spec.Name, latest, err)
		}
		status, err := groupStatus(ctx, c, current.Spec.Name, topology.MetaGroupID)
		if err != nil || len(status.ConfState.Voters) != 4 || slices.Contains(status.ConfState.Learners, member) {
			t.Fatal(fmt.Sprintf("%s meta member did not retain promotion", current.Spec.Name), status, err)
		}
	}
}
