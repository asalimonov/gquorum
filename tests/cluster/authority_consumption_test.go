//go:build clustertest

package cluster

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestAuthorityEarlierLearnerActionCannotUndoSameOperatorPromotionThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, original := readProcessCluster(t, "node1")
	if err := transferMeta(ctx, c, "node2", "node2", original.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	shrunk := original.Clone()
	group := shrunk.Group("work")
	group.Replicas = slices.DeleteFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == "node3" })
	group.Placement.MinReplicas, group.Placement.MaxReplicas = 2, 2
	group.VoterPolicy.TargetVoters = 2
	staged, err := c.Stage(ctx, "node2", shrunk)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	owner, err := c.Local(ctx, "node1", "work")
	if err != nil || !owner.Found || owner.Status.LeaderID != owner.ReplicaID {
		t.Fatal(owner, err)
	}
	// Reseating uses a fresh monotonic load identity; it cannot reopen the
	// already retired node3 replica merely to exercise a later role change.
	grown := staged.Document.Clone()
	group = grown.Group("work")
	newReplica := group.NextReplicaID
	group.NextReplicaID++
	now := time.Now().UTC()
	group.Replicas = append(group.Replicas, topology.Replica{NodeName: "node3", ReplicaID: newReplica, Role: topology.ReplicaVoter, CreatedAt: now, UpdatedAt: now})
	group.Placement.MinReplicas, group.Placement.MaxReplicas = 3, 3
	group.VoterPolicy.TargetVoters = 3
	response, err := c.Request(ctx, "node1", "PUT", "/internal/test/barriers/work/add_learner?after=true", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepPromoteVoter, true); err != nil {
		t.Fatal(err)
	}
	held := map[topology.StepKind]bool{topology.StepAddLearner: true, topology.StepPromoteVoter: true}
	release := func(kind topology.StepKind) {
		t.Helper()
		if !held[kind] {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.SetBarrier(cleanup, "node1", "work", kind, false); err != nil {
			t.Error("membership barrier cleanup", kind, err)
			return
		}
		held[kind] = false
	}
	t.Cleanup(func() { release(topology.StepAddLearner); release(topology.StepPromoteVoter) })
	growStage, err := c.Stage(ctx, "node2", grown)
	if err != nil {
		t.Fatal(err)
	}
	op := startApply(t, ctx, c, "node2", growStage.Document.Version)
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Controls(ctx, "node1")
		for _, barrier := range view.Barriers {
			if barrier.Kind == topology.StepAddLearner && barrier.Hits > 0 && barrier.Last.OperatorID == op && barrier.Last.ReplicaID == newReplica {
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("exact learner application did not reach its held reply", err)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "work")
		return err == nil && local.Found && local.ReplicaID == newReplica && slices.Contains(local.Status.ConfState.Learners, newReplica), err
	}); err != nil {
		t.Fatal("learner-first effect did not install the fresh identity", err)
	}
	release(topology.StepAddLearner)
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Controls(ctx, "node1")
		for _, barrier := range view.Barriers {
			if barrier.Kind == topology.StepPromoteVoter && barrier.Hits > 0 && barrier.Last.OperatorID == op && barrier.Last.ReplicaID == newReplica {
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("same operator did not advance to its exact guarded promotion", err)
	}
	view, err := c.Authority(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	var earlier, later load.ActionRequest
	for _, request := range view.Actions {
		if request.Authorization.OperatorID != op {
			continue
		}
		switch request.Authorization.Kind {
		case topology.StepAddLearner:
			earlier = request
		case topology.StepPromoteVoter:
			later = request
		}
	}
	if earlier.Grant.Key.Revision == 0 || later.Grant.Key.Revision <= earlier.Grant.Key.Revision || earlier.Authorization.Version != later.Authorization.Version || earlier.Authorization.Attempt != later.Authorization.Attempt || later.Authorization.StepSeq <= earlier.Authorization.StepSeq || earlier.Authorization.ReplicaID != newReplica || later.Authorization.ReplicaID != newReplica {
		t.Fatal("operator changed its exact attempt/action issuance boundaries", earlier, later)
	}
	requireFencedReplay(t, ctx, c, owner, earlier)
	// Matching a key is insufficient: a structurally valid altered target set
	// must fail full equality against the retained immutable grant.
	altered := earlier
	altered.Grant = earlier.Grant.Clone()
	altered.Grant.Targets[0].ReplicaID += 10
	if err := altered.Grant.Validate(); err != nil {
		t.Fatal("altered-grant test must reach equality validation", err)
	}
	before, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.ReplayAction(ctx, "node1", owner.Identity, altered)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("same revision accepted altered target identity", err)
	}
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if err := response.Decode(&refusal); err != nil || refusal.Code != fault.IdentityMismatch {
		t.Fatal("altered immutable grant bypassed full equality", refusal, err)
	}
	after, err := c.Local(ctx, "node1", "work")
	if err != nil || !reflect.DeepEqual(before.Status.ConfState, after.Status.ConfState) || !reflect.DeepEqual(before.StorageIdentity, after.StorageIdentity) || !reflect.DeepEqual(before.Values, after.Values) {
		t.Fatal("altered grant changed native/storage/application state", before, after, err)
	}
	release(topology.StepPromoteVoter)
	if err := c.WaitOperator(ctx, "node2", op); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, growStage.Document.Version); err != nil {
		t.Fatal(err)
	}
	finished, err := c.Operator(ctx, "node2", op)
	if err != nil || finished.State != topology.OperatorSucceeded || finished.Attempt != earlier.Authorization.Attempt || finished.Version != earlier.Authorization.Version {
		t.Fatal("promotion changed approved execution identity", finished, err)
	}
	// The earlier add-learner action must not demote the freshly promoted voter.
	requireFencedReplay(t, ctx, c, owner, earlier)
	waitAuthorityMembership(t, ctx, c, growStage.Document, false)
	value, err := c.Read(ctx, "node3", "work", "key")
	requireReadValue(t, value, err, "node3", "before")
}
