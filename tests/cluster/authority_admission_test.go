//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

// A fourth process is required: three established voters plus one admitted
// metadata learner that is superseded before its topology can become Active.
func TestAuthoritySupersessionRetiresIntermediateMetaLearnerFourNodes(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Limits.MaxNodes, spec.Limits.MaxReplicas = 4, 4
	spec.RetryRemoval = true // retain the removed process for exact inspection
	ctx, c, original := readProcessClusterSpec(t, "node1", spec)
	placement := original.Clone()
	placement.Group("work").Placement.MaxReplicas = 4
	stagedPlacement, err := c.Stage(ctx, "node2", placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", stagedPlacement.Document.Version); err != nil {
		t.Fatal(err)
	}
	original = stagedPlacement.Document
	if err := transferMeta(ctx, c, "node2", "node2", original.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	joining, err := c.PrepareJoinNode(ctx, NodeSpec{Name: "node4", Type: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepAddLearner, true); err != nil {
		t.Fatal(err)
	}
	held := true
	t.Cleanup(func() {
		if !held {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.SetBarrier(cleanup, "node1", "work", topology.StepAddLearner, false); err != nil {
			t.Error("admission action barrier cleanup", err)
		}
	})
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{
		"node": joiningNode(joining.Spec), "mutation": gquorum.MutationOptions{Actor: "cluster-test"},
	}, original.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var admitted gquorum.MutationResult
	if err := response.Decode(&admitted); err != nil {
		t.Fatal(err)
	}
	old := startApply(ctx, t, c, "node2", admitted.Document.Version)
	if err := joining.Start(ctx); err != nil {
		t.Fatal(err)
	}
	member, replica := admitted.Document.MemberID("node4"), replicaID(admitted.Document, "work", "node4")
	var delayed load.ActionRequest
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Authority(ctx, "node1", "work")
		for _, request := range view.Actions {
			if request.Authorization.OperatorID == old && request.Authorization.Kind == topology.StepAddLearner && request.Authorization.ReplicaID == replica {
				delayed = request
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("admission did not reach its load learner barrier", err)
	}
	status, err := groupStatus(ctx, c, "node2", topology.MetaGroupID)
	if err != nil || !slices.Contains(status.ConfState.Learners, member) {
		t.Fatal("intermediate approved metadata learner is not native", status, err)
	}
	active, err := c.Document(ctx, "node2", "active")
	if err != nil || active.Version != original.Version || active.MemberID("node4") != 0 {
		t.Fatal("held admission unexpectedly reached Active", active, err)
	}
	desired := original.Clone()
	desired.Version = admitted.Document.Version
	for i := range desired.Groups {
		desired.Groups[i].NextReplicaID = admitted.Document.Group(desired.Groups[i].ID).NextReplicaID
	}
	staged, err := c.Stage(ctx, "node2", desired)
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal("supersession could not map approved intermediate metadata", err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil || !slices.ContainsFunc(plan.Steps, func(step topology.Step) bool {
		return step.GroupID == topology.MetaGroupID && step.Kind == topology.StepRemoveReplica && step.NodeName == "node4" && step.ReplicaID == member
	}) {
		t.Fatal("successor omitted the intermediate native member", plan, err)
	}
	next := startApply(ctx, t, c, "node2", staged.Document.Version)
	waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
	err = c.WaitOperation(waitCtx, "node2", next)
	cancelWait()
	if err != nil {
		operator, operatorErr := c.Operation(ctx, "node2", next)
		runtime, runtimeErr := c.runtime(ctx, "node4")
		t.Fatal("intermediate metadata/load retirement did not converge", err, operator, operatorErr, runtime, runtimeErr)
	}
	for _, name := range []string{"node1", "node2", "node3"} {
		if err := Poll(ctx, func() (bool, error) {
			doc, err := c.Document(ctx, name, "active")
			return doc.Version == staged.Document.Version && doc.MemberID("node4") == 0, err
		}); err != nil {
			t.Fatal(err)
		}
		value, err := c.Read(ctx, name, "work", "key")
		requireReadValue(t, value, err, name, "before")
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepAddLearner, false); err != nil {
		t.Fatal(err)
	}
	held = false
	owner, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	requireFencedReplay(ctx, t, c, owner, delayed)
	if _, err := c.docker.ContainerStatPath(ctx, joining.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/work"}); !errdefs.IsNotFound(err) {
		t.Fatal("prepared intermediate load state survived retirement", err)
	}
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.runtime(ctx, "node4")
		return view.Status.State == gquorum.NodeFenced && !view.Status.Meta.Found && !view.Status.Ready && !view.Status.SessionLive, err
	}); err != nil {
		t.Fatal("intermediate metadata owner was not fenced", err)
	}
}
