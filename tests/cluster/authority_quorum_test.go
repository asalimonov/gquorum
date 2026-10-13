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
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.SetBarrier(cleanup, "node1", "work", topology.StepDemoteVoter, false); err != nil {
			t.Error("demotion barrier cleanup", err)
		}
	})
	demoted := original.Clone()
	group := demoted.Group("work")
	group.VoterPolicy.TargetVoters = 2
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
	old := startApply(t, ctx, c, "node2", staged.Document.Version)
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
	newOp := startApply(t, ctx, c, "node2", newStage.Document.Version)
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
		blocked[node] = armReadFault(t, ctx, c, node, ReadFaults{DropRaft: true})
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
		op, err := c.Operator(ctx, node, newOp)
		if err != nil || op.State.Terminal() || op.TotalSteps != 0 {
			t.Fatal("empty successor bypassed its no-quorum revocation", op, err)
		}
		active, err := c.Document(ctx, node, "active")
		if err != nil || active.Version != original.Version {
			t.Fatal("unconfirmed fence advanced Active", active, err)
		}
	}
	for node, local := range blocked {
		clearReadFault(t, ctx, c, node, local)
	}
	if err := c.WaitOperator(ctx, "node2", newOp); err != nil {
		t.Fatal("restored load quorum did not complete the real resource fence", err)
	}
	if err := c.WaitActive(ctx, newStage.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Revocation(ctx, "node3", old)
		return err == nil && status.RevocationRequested && status.RevocationComplete, err
	}); err != nil {
		t.Fatal(err)
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
	requireFencedReplay(t, ctx, c, current, delayed)
	waitAuthorityMembership(t, ctx, c, original, false)
	value, err := c.Read(ctx, leader, "work", "key")
	requireReadValue(t, value, err, leader, "before")
}
