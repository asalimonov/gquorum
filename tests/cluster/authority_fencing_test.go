//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// Each case owns three processes. The metadata controller and application
// leader differ, so every held action/fence crosses the authenticated transport.
func TestAuthorityRevocationBoundaryAndLostFenceReplyThreeNodes(t *testing.T) {
	for _, after := range []bool{false, true} {
		name := "delayed_action_before_fence"
		if after {
			name = "lost_reply_after_fence"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			spec := matrixSpec(3)
			spec.MetaFaultProxy = true
			ctx, c, original := readProcessClusterSpec(t, "node1", spec)
			if err := transferMeta(ctx, c, "node2", "node2", original.MemberID("node2")); err != nil {
				t.Fatal(err)
			}
			owner, err := c.Local(ctx, "node1", "work")
			if err != nil || !owner.Found || owner.Status.LeaderID != owner.ReplicaID {
				t.Fatal("missing exact group leader", owner, err)
			}
			if err := c.SetBarrier(ctx, "node1", "work", topology.StepDemoteVoter, true); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := c.SetBarrier(cleanup, "node1", "work", topology.StepDemoteVoter, false); err != nil {
					t.Error("action barrier cleanup", err)
				}
			})
			demoted := original.Clone()
			voterConfig := topology.DefaultVoterConfig()
			voterConfig.TargetVoters = 2
			demoted.Group("work").VoterConfig = &voterConfig
			demoted.Group("work").Placement.MinReplicas = 2
			for i := range demoted.Group("work").Replicas {
				r := &demoted.Group("work").Replicas[i]
				if r.NodeName == "node3" {
					r.Role, r.PinnedLearner = topology.ReplicaLearner, true
				}
			}
			staged, err := c.Stage(ctx, "node2", demoted)
			if err != nil {
				t.Fatal(err)
			}
			old := startApply(ctx, t, c, "node2", staged.Document.Version)
			var delayed load.ActionRequest
			if err := Poll(ctx, func() (bool, error) {
				view, err := c.Authority(ctx, "node1", "work")
				for _, action := range view.Actions {
					if action.Authorization.OperatorID == old && action.Authorization.Kind == topology.StepDemoteVoter {
						delayed = action
						return true, err
					}
				}
				return false, err
			}); err != nil {
				t.Fatal("old action did not reach its delivery barrier", err)
			}
			if delayed.From.NodeName != "node2" || delayed.To.Node != owner.Identity || delayed.To.Replica.ReplicaID != owner.ReplicaID || delayed.Authorization.ReplicaID != replicaID(original, "work", "node3") {
				t.Fatal("captured action lost exact courier/receiver/target", delayed)
			}
			if err := Poll(ctx, func() (bool, error) {
				target, err := c.Document(ctx, "node1", "target")
				return err == nil && target.Version == staged.Document.Version, err
			}); err != nil {
				t.Fatal(err)
			}
			// Keep application Raft and proof/control RPCs live while withholding
			// this receiver's metadata replication, including snapshot catch-up.
			healReceiver := holdMetaRaft(ctx, t, c, "node1", owner.Identity)
			boundary := FenceFault{Identity: owner.Identity, ReplicaID: owner.ReplicaID, Purpose: topology.AuthorityFence, MinRevision: delayed.Grant.Key.Revision + 1, After: after}
			if err := c.SetFenceFault(ctx, "node1", "work", topology.AuthorityGroup, "PUT", boundary); err != nil {
				t.Fatal(err)
			}
			fenceHeld := true
			t.Cleanup(func() {
				if !fenceHeld {
					return
				}
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := c.SetFenceFault(cleanup, "node1", "work", topology.AuthorityGroup, "DELETE", boundary); err != nil {
					t.Error("fence barrier cleanup", err)
				}
			})
			// Native membership is still three voters when this immutable plan is
			// constructed. Restoring the original desired topology needs no steps.
			successor := original.Clone()
			successor.Version = staged.Document.Version
			newStage, err := c.Stage(ctx, "node2", successor)
			if err != nil {
				t.Fatal(err)
			}
			response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/plan", newStage.Document.Version), nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				t.Fatal(err)
			}
			var plan topology.Plan
			if err := response.Decode(&plan); err != nil {
				t.Fatal(err)
			}
			if !plan.IsEmpty() {
				t.Fatal("successor setup must capture the pre-delivery native state", plan)
			}
			newOp := startApply(ctx, t, c, "node2", newStage.Document.Version)
			var fence load.FenceRequest
			if err := Poll(ctx, func() (bool, error) {
				view, err := c.Authority(ctx, "node1", "work")
				for _, b := range view.Barriers {
					if b.Hits > 0 {
						fence = b.Last
						return true, err
					}
				}
				return false, err
			}); err != nil {
				t.Fatal("successor did not attempt its inherited resource fence", err)
			}
			status, err := c.Revocation(ctx, "node3", old)
			if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) == 0 {
				t.Fatal("withheld fence proof must remain pending", status, err)
			}
			if err := Poll(ctx, func() (bool, error) {
				faults, err := c.MetaFault(ctx, "node1", "GET", owner.Identity, false)
				return err == nil && faults.DropRaft && faults.FramesDropped > 0, err
			}); err != nil {
				t.Fatal("metadata receiver never became stale", err)
			}
			stale, err := c.Document(ctx, "node1", "target")
			if err != nil || stale.Version != staged.Document.Version {
				t.Fatal("metadata fault did not retain the older approved target", stale, err)
			}
			var healController func()
			if after {
				view, err := c.Authority(ctx, "node1", "work")
				if err != nil || !slices.ContainsFunc(view.Fences, func(result load.FenceResult) bool {
					return result.Evidence.Key == fence.Grant.Key && result.Evidence.Validate(fence.Grant) == nil
				}) {
					t.Fatal("reply fault preceded durable resource fencing", view, err)
				}
				requireFencedReplay(ctx, t, c, owner, delayed)
				// Preserve the former controller process/courier identity while
				// forcing ordinary Raft election. Direct transfer is deliberately
				// inadmissible while the topology revocation is pending.
				healReceiver()
				controller, err := c.Local(ctx, "node2", topology.MetaGroupID)
				if err != nil {
					t.Fatal(err)
				}
				healController = holdMetaRaft(ctx, t, c, "node2", controller.Identity)
				if err := Poll(ctx, func() (bool, error) {
					one, err := c.Status(ctx, "node1")
					if err != nil {
						return false, err
					}
					three, err := c.Status(ctx, "node3")
					return err == nil && one.MetaLeaderID != 0 && one.MetaLeaderID != controller.Identity.MemberID && one.MetaLeaderID == three.MetaLeaderID, err
				}); err != nil {
					t.Fatal("pending fence did not survive metadata controller replacement", err)
				}
				status, err := c.Revocation(ctx, "node3", old)
				if err != nil || status.RevocationComplete {
					t.Fatal("controller replacement manufactured fence acknowledgement", status, err)
				}
			} else {
				response, err := c.ReplayAction(ctx, "node1", owner.Identity, delayed)
				if err := requireStatus(response, err, 200); err != nil {
					t.Fatal("approved replacement fenced an action before its resource boundary", err)
				}
				var result load.ActionResult
				if err := response.Decode(&result); err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(delayed.Grant) != nil {
					t.Fatal("permitted old action returned no exact receipt", result, err)
				}
				waitAuthorityMembership(ctx, t, c, original, true)
			}
			if err := c.SetFenceFault(ctx, "node1", "work", topology.AuthorityGroup, "DELETE", boundary); err != nil {
				t.Fatal(err)
			}
			fenceHeld = false
			if err := Poll(ctx, func() (bool, error) {
				status, err := c.Revocation(ctx, "node3", old)
				return err == nil && status.RevocationRequested && status.RevocationComplete, err
			}); err != nil {
				t.Fatal("durable fence reply did not complete revocation", err)
			}
			healReceiver()
			if healController != nil {
				healController()
			}
			requireFencedReplay(ctx, t, c, owner, delayed)
			if after {
				if err := c.WaitOperation(ctx, "node2", newOp); err != nil {
					t.Fatal("lost acknowledgement could not resume the same empty plan", err)
				}
				if err := c.WaitActive(ctx, newStage.Document.Version); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := Poll(ctx, func() (bool, error) {
					op, err := c.Operation(ctx, "node2", newOp)
					if err != nil || !op.State.Terminal() {
						return false, err
					}
					if op.State != topology.OperatorFailed || !strings.Contains(op.Error, string(fault.ReplanRequired)+":") {
						return true, fmt.Errorf("stale empty plan did not report its replan requirement: %+v", op)
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				active, err := c.Document(ctx, "node2", "active")
				if err != nil || active.Version != original.Version {
					t.Fatal("unreconciled permitted effect advanced Active", active, err)
				}
				latest, err := c.Document(ctx, "node2", "latest")
				if err != nil {
					t.Fatal(err)
				}
				replanned, err := c.Stage(ctx, "node2", latest)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Approve(ctx, "node2", replanned.Document.Version); err != nil {
					t.Fatal("fresh approved plan could not reconcile the permitted old effect", err)
				}
				requireFencedReplay(ctx, t, c, owner, delayed)
			}
			waitAuthorityMembership(ctx, t, c, original, false)
			// The replicated resource floor must outlive both leadership changes.
			forceReadLeader(ctx, t, c, original, "node3")
			newOwner, err := c.Local(ctx, "node3", "work")
			if err != nil {
				t.Fatal(err)
			}
			delayed.To = load.Peer{Node: newOwner.Identity, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: newOwner.ReplicaID}}
			requireFencedReplay(ctx, t, c, newOwner, delayed)
			waitAuthorityMembership(ctx, t, c, original, false)
		})
	}
}

func requireFencedReplay(ctx context.Context, t *testing.T, c *Cluster, owner LocalObservation, request load.ActionRequest) {
	t.Helper()
	node := owner.Identity.NodeName
	before, err := c.Local(ctx, node, "work")
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.ReplayAction(ctx, node, owner.Identity, request)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("old action crossed a durable resource fence", err)
	}
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if err := response.Decode(&refusal); err != nil || refusal.Code != fault.AuthorityFenced {
		t.Fatal("replay did not identify its stale authority", refusal, err)
	}
	after, err := c.Local(ctx, node, "work")
	if err != nil || !reflect.DeepEqual(before.Status.ConfState, after.Status.ConfState) || before.ReplicaID != after.ReplicaID || before.Identity != after.Identity || !reflect.DeepEqual(before.StorageIdentity, after.StorageIdentity) || !reflect.DeepEqual(before.Values, after.Values) {
		t.Fatal("rejected replay mutated membership/application/storage identity", before, after, err)
	}
}

func holdMetaRaft(ctx context.Context, t *testing.T, c *Cluster, node string, identity topology.Identity) func() {
	t.Helper()
	if _, err := c.MetaFault(ctx, node, "PUT", identity, true); err != nil {
		t.Fatal(err)
	}
	held := true
	heal := func() {
		if !held {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.MetaFault(cleanup, node, "PUT", identity, false); err != nil {
			t.Error("metadata fault cleanup", err)
			return
		}
		held = false
	}
	t.Cleanup(heal)
	return heal
}

func waitAuthorityMembership(ctx context.Context, t *testing.T, c *Cluster, doc topology.Document, learner bool) {
	t.Helper()
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			local, err := c.Local(ctx, node.Spec.Name, "work")
			if err != nil {
				return false, err
			}
			id := replicaID(doc, "work", "node3")
			voters := 3
			if learner {
				voters = 2
			}
			if !local.Found || len(local.Status.ConfState.Voters) != voters || slices.Contains(local.Status.ConfState.Learners, id) != learner || slices.Contains(local.Status.ConfState.Voters, id) == learner || local.ReplicaID != replicaID(doc, "work", node.Spec.Name) || local.StorageIdentity == nil || local.StorageIdentity.ReplicaID != local.ReplicaID || local.Values["key"] != "before" {
				return false, fmt.Errorf("membership/data/identity did not converge: %+v", local)
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
}
