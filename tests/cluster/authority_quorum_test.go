//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestAuthorityGroupRevocationRemainsPendingWithoutLoadQuorumThreeNodes(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.MetaFaultProxy = true
	ctx, c, original := readProcessClusterSpec(t, "node1", spec)
	if err := transferMeta(ctx, c, "node2", "node2", original.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	owner, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepDemoteVoter, true); err != nil {
		t.Fatal(err)
	}
	actionHeld := true
	t.Cleanup(func() {
		if !actionHeld {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.SetBarrier(cleanup, "node1", "work", topology.StepDemoteVoter, false); err != nil {
			t.Error("demotion barrier cleanup", err)
		}
	})
	demoted := original.Clone()
	group := demoted.Group("work")
	group.VoterConfig.TargetVoters = 2
	group.Placement.MinReplicas = 2
	for i := range group.Replicas {
		if group.Replicas[i].NodeName == "node3" {
			group.Replicas[i].Role, group.Replicas[i].PinnedLearner = topology.ReplicaLearner, true
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
			if action.Authorization.OperationID == old && action.Authorization.Kind == topology.StepDemoteVoter {
				delayed = action
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	boundary := FenceFault{Identity: owner.Identity, ReplicaID: owner.ReplicaID, Purpose: topology.AuthorityFence, MinRevision: delayed.Grant.Key.Revision + 1}
	if err := c.SetFenceFault(ctx, "node1", "work", topology.AuthorityGroup, "PUT", boundary); err != nil {
		t.Fatal(err)
	}
	held := true
	t.Cleanup(func() {
		if held {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.SetFenceFault(cleanup, "node1", "work", topology.AuthorityGroup, "DELETE", boundary); err != nil {
				t.Error("group fence cleanup", err)
			}
		}
	})
	successor := original.Clone()
	successor.Version = staged.Document.Version
	newStage, err := c.Stage(ctx, "node2", successor)
	if err != nil {
		t.Fatal(err)
	}
	newOp := startApply(ctx, t, c, "node2", newStage.Document.Version)
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Authority(ctx, "node1", "work")
		for _, barrier := range view.Barriers {
			if barrier.Hits > 0 {
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("approval did not attempt its real inherited group fence", err)
	}
	// All load elections/read acknowledgements are prevented, while all three
	// metadata participants and their authority RPCs remain reachable.
	blocked := map[string]LocalObservation{}
	for _, node := range []string{"node2", "node3"} {
		blocked[node] = armReadFault(ctx, t, c, node, ReadFaults{DropRaft: true})
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	_, err = c.Read(short, "node1", "work", "key")
	cancel()
	if err == nil {
		t.Fatal("load fault did not remove quorum confirmation")
	}
	for node := range blocked {
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, node, "work")
			return err == nil && local.Reads.DropRaft && local.Reads.RaftDropped > blocked[node].Reads.RaftDropped, err
		}); err != nil {
			t.Fatal("load partition never withheld actual Raft traffic", node, err)
		}
	}
	if err := c.SetFenceFault(ctx, "node1", "work", topology.AuthorityGroup, "DELETE", boundary); err != nil {
		t.Fatal(err)
	}
	held = false
	for _, node := range []string{"node2", "node3"} {
		status, err := c.Revocation(ctx, node, old)
		if err != nil || !status.RevocationRequested || status.RevocationComplete {
			t.Fatal("metadata quorum substituted for an unconfirmed load fence", status, err)
		}
		pending := false
		for _, obligation := range status.Obligations {
			pending = pending || obligation.Resource == (topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}) && obligation.Evidence == nil
		}
		if !pending {
			t.Fatal("load group lost its pending resource obligation", status)
		}
		op, err := c.Operation(ctx, node, newOp)
		totalSteps := 0
		for _, block := range op.Blocks {
			totalSteps += block.TotalSteps
		}
		if err != nil || op.State.Terminal() || totalSteps != 0 {
			t.Fatal("empty successor bypassed its no-quorum revocation", op, err)
		}
		active, err := c.Document(ctx, node, "active")
		if err != nil || active.Version != original.Version {
			t.Fatal("unconfirmed fence advanced Active", active, err)
		}
	}
	// Restore a majority while keeping the original executor isolated until
	// another leader is observed. The uncertainty assertion must also hold
	// across leader replacement, not just when node1 wins the next election.
	blockedLeader := armReadFault(ctx, t, c, "node1", ReadFaults{DropRaft: true})
	for node, local := range blocked {
		clearReadFault(ctx, t, c, node, local)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node2", "work")
		return err == nil && local.Status.LeaderID != 0 && local.Status.LeaderID != owner.ReplicaID, err
	}); err != nil {
		t.Fatal("restored majority did not replace the original executor", err)
	}
	clearReadFault(ctx, t, c, "node1", blockedLeader)
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Revocation(ctx, "node3", old)
		return err == nil && status.RevocationRequested && status.RevocationComplete, err
	}); err != nil {
		t.Fatal("restored quorum did not confirm the resource fence", err)
	}
	// A transient quorum failure may replace the approval-time
	// membership_unproven diagnostic. Assert the safety state after fresh
	// quorum reads, rather than requiring that historical reason to persist.
	for _, node := range c.Nodes() {
		value, err := c.Read(ctx, node.Spec.Name, "work", "key")
		requireReadValue(t, value, err, node.Spec.Name, "before")
		status, err := c.Operation(ctx, node.Spec.Name, newOp)
		block, found := operationBlock(status, "work")
		if err != nil || !found || block.State.Terminal() || block.TotalSteps != 0 || block.Blocked == nil {
			t.Fatal("a fence incorrectly proved the old demotion outcome", status, err)
		}
		converged, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || converged.Version != original.Version {
			t.Fatal("fencing advanced convergence without membership receipts", converged, err)
		}
	}
	if err := c.SetBarrier(ctx, "node1", "work", topology.StepDemoteVoter, false); err != nil {
		t.Fatal(err)
	}
	actionHeld = false
	// Obtain explicit new-grant receipts before declaring the original voting
	// configuration converged. Fencing alone cannot prove non-execution.
	demoted.Version = newStage.Document.Version
	confirmed, err := c.Stage(ctx, "node2", demoted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", confirmed.Document.Version); err != nil {
		t.Fatal("confirm demotion after fencing", err)
	}
	restored := original.Clone()
	restored.Version = confirmed.Document.Version
	confirmed, err = c.Stage(ctx, "node2", restored)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", confirmed.Document.Version); err != nil {
		t.Fatal("restore confirmed membership", err)
	}
	status, err := groupStatus(ctx, c, "node2", "work")
	if err != nil || status.LeaderID == 0 {
		t.Fatal(status, err)
	}
	leader := ""
	for _, r := range original.Group("work").Replicas {
		if r.ReplicaID == status.LeaderID {
			leader = r.NodeName
		}
	}
	if leader == "" {
		t.Fatal(fmt.Errorf("unknown exact restored leader %d", status.LeaderID))
	}
	current, err := c.Local(ctx, leader, "work")
	if err != nil {
		t.Fatal(err)
	}
	delayed.To = load.Peer{Node: current.Identity, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: current.ReplicaID}}
	requireFencedReplay(ctx, t, c, current, delayed)
	waitAuthorityMembership(ctx, t, c, original, false)
	value, err := c.Read(ctx, leader, "work", "key")
	requireReadValue(t, value, err, leader, "before")
}
