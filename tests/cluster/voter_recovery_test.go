//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

// Four participants are necessary for three voters and one retained standby.
// The five-node automatic case covers qualification; this case interrupts the
// actual membership effect before acknowledgement in both approved orderings.
func TestVoterSwapResumesAfterCommittedEffectAndMetaLeaderCrashFourNodes(t *testing.T) {
	t.Parallel()
	for _, surge := range []int{0, 1} {
		t.Run(fmt.Sprintf("surge_%d", surge), func(t *testing.T) {
			t.Parallel()
			c := acquire(t, matrixSpec(4)).Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
			defer cancel()
			p := voterConfig(3)
			p.MaxSurgeVoters = surge
			if surge == 0 {
				// Same-AZ standby: strict placement forces the guarded
				// 3 -> 2 -> 3 path across the lost acknowledgement.
				placementEdit(ctx, t, c, func(doc *topology.Document) {
					for i, name := range []string{"node1", "node2", "node3", "node4"} {
						labels := doc.Node(name).GetLabels()
						labels["az"] = []string{"a", "b", "c", "c"}[i]
						if err := doc.Node(name).SetLabels(labels); err != nil {
							t.Fatal(err)
						}
					}
				})
				p.DistinctVoterLabel = "az"
			}
			created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "swap", VoterConfig: p, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 4}})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put(ctx, "node4", "swap", "retained", "retained", "through-learner"); err != nil {
				t.Fatal(err)
			}
			if err := requestLeaderTransfer(ctx, c, "node2", "swap", "node1"); err != nil {
				t.Fatal(err)
			}
			if err := Poll(ctx, func() (bool, error) {
				s, err := groupStatus(ctx, c, "node2", "swap")
				return s.LeaderID == replicaID(created.Document, "swap", "node1"), err
			}); err != nil {
				t.Fatal(err)
			}
			// The victim is the meta leader but never the load leader. Killing it
			// after demotion leaves both voters of the no-surge intermediate live.
			if err := transferMeta(ctx, c, "node2", "node3", created.Document.MemberID("node3")); err != nil {
				t.Fatal(err)
			}
			desired := created.Document.Clone()
			for i := range desired.Group("swap").Replicas {
				r := &desired.Group("swap").Replicas[i]
				if r.NodeName == "node3" {
					r.Role = topology.ReplicaLearner
				}
				if r.NodeName == "node4" {
					r.Role = topology.ReplicaVoter
				}
			}
			staged, err := c.Stage(ctx, "node3", desired)
			if err != nil {
				t.Fatal(err)
			}
			response, err := c.Request(ctx, "node3", "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				t.Fatal(err)
			}
			var plan topology.Plan
			if err := response.Decode(&plan); err != nil {
				t.Fatal(err)
			}
			roles := []topology.Step{}
			for _, step := range plan.Steps {
				if step.Kind == topology.StepPromoteVoter || step.Kind == topology.StepDemoteVoter {
					roles = append(roles, step)
				}
			}
			firstKind := topology.StepDemoteVoter
			intermediateVoters := 2
			if surge == 1 {
				firstKind, intermediateVoters = topology.StepPromoteVoter, 4
			}
			if len(roles) != 2 || roles[0].Kind != firstKind || roles[1].Kind == firstKind {
				t.Fatal("wrong approved ordering", plan)
			}
			for _, node := range c.Nodes() {
				response, err := c.Request(ctx, node.Spec.Name, "PUT", "/internal/test/barriers/swap/"+string(firstKind)+"?after=true", nil, 0)
				if err := requireStatus(response, err, 200); err != nil {
					t.Fatal(err)
				}
				if err := c.SetBarrier(ctx, node.Spec.Name, "swap", roles[1].Kind, true); err != nil {
					t.Fatal(err)
				}
			}
			id := startApply(ctx, t, c, "node3", staged.Document.Version)
			if err := Poll(ctx, func() (bool, error) {
				controls, err := c.Controls(ctx, "node1")
				if err != nil {
					return false, err
				}
				for _, b := range controls.Barriers {
					if b.Hits > 0 && b.Last.Matches(id, 1, staged.Document.Version, roles[0]) {
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				t.Fatal("committed role effect did not reach acknowledgement barrier", err)
			}
			assertIntermediate := func() error {
				local, err := c.Local(ctx, "node1", "swap")
				if err != nil {
					return err
				}
				if len(local.Status.ConfState.Voters) != intermediateVoters || len(local.Status.ConfState.Learners) != 4-intermediateVoters {
					return fmt.Errorf("approved intermediate membership changed: %+v", local.Status.ConfState)
				}
				return nil
			}
			if err := assertIntermediate(); err != nil {
				t.Fatal(err)
			}
			if err := c.Node("node3").Kill(ctx); err != nil {
				t.Fatal(err)
			}
			if err := Poll(ctx, func() (bool, error) {
				status, err := c.Status(ctx, "node2")
				if err != nil {
					return false, err
				}
				if status.MetaLeaderID == 0 || status.MetaLeaderID == created.Document.MemberID("node3") {
					return false, nil
				}
				op, err := c.Operator(ctx, "node2", id)
				if err != nil {
					return false, err
				}
				if op.ID != id || op.Attempt != 1 || op.Version != staged.Document.Version || op.State.Terminal() {
					return true, fmt.Errorf("lost exact interrupted operator: %+v", op)
				}
				return true, assertIntermediate()
			}); err != nil {
				t.Fatal("replacement meta leader did not retain approved execution", err)
			}
			// The first effect's receipt is durable at its native resource, but
			// role observations alone cannot complete the step. Let a same-grant
			// retry return that exact receipt to the replacement controller.
			for _, name := range []string{"node1", "node2", "node4"} {
				if err := c.SetBarrier(ctx, name, "swap", firstKind, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := Poll(ctx, func() (bool, error) {
				op, err := c.Operator(ctx, "node2", id)
				if err != nil {
					return false, err
				}
				if op.ID != id || op.Attempt != 1 || op.Version != staged.Document.Version || op.State.Terminal() {
					return true, fmt.Errorf("receipt recovery changed interrupted operator: %+v", op)
				}
				for _, name := range []string{"node1", "node2", "node4"} {
					controls, err := c.Controls(ctx, name)
					if err != nil {
						return false, err
					}
					for _, b := range controls.Barriers {
						if b.Hits > 0 && b.Last.Matches(id, 1, staged.Document.Version, roles[1]) {
							return true, assertIntermediate()
						}
					}
				}
				return false, nil
			}); err != nil {
				t.Fatal("replacement meta leader did not recover exact native receipt", err)
			}
			for _, node := range c.Nodes() {
				if node.Spec.Name == "node3" {
					continue
				}
				if err := c.SetBarrier(ctx, node.Spec.Name, "swap", roles[1].Kind, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.WaitOperator(ctx, "node2", id); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitActive(ctx, staged.Document.Version); err != nil {
				t.Fatal(err)
			}
			if err := c.Node("node3").Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			for _, node := range c.Nodes() {
				if err := Poll(ctx, func() (bool, error) {
					local, err := c.Local(ctx, node.Spec.Name, "swap")
					if err != nil {
						return false, err
					}
					conf := local.Status.ConfState
					return local.Found && local.ReplicaID == replicaID(created.Document, "swap", node.Spec.Name) && len(conf.Voters) == 3 && len(conf.Learners) == 1 && slices.Contains(conf.Voters, replicaID(created.Document, "swap", "node4")) && slices.Contains(conf.Learners, replicaID(created.Document, "swap", "node3")) && local.Values["retained"] == "through-learner", nil
				}); err != nil {
					t.Fatal("retained swap identity/data did not converge", node.Spec.Name, err)
				}
			}
			op, err := c.Operator(ctx, "node2", id)
			if err != nil || op.Attempt != 1 || op.State != topology.OperatorSucceeded {
				t.Fatal("recovery created another attempt", op, err)
			}
		})
	}
}

// Four nodes are necessary for the replacement's temporary four-voter state.
// The victim regains leadership after the approved explicit transfer; the
// demotion must repeat only its exact guard and wait while that target is down.
func TestVoterDemotionRepeatsExactTransferAfterLeaderRegainFourNodes(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(4)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "swap", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node2", created.Document.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	transfer := func(target string) {
		t.Helper()
		if err := requestLeaderTransfer(ctx, c, "node2", "swap", target); err != nil {
			t.Fatal(err)
		}
		if err := Poll(ctx, func() (bool, error) {
			status, err := groupStatus(ctx, c, "node2", "swap")
			return status.LeaderID == replicaID(created.Document, "swap", target), err
		}); err != nil {
			t.Fatal(err)
		}
	}
	transfer("node3")
	desired := created.Document.Clone()
	for i := range desired.Group("swap").Replicas {
		r := &desired.Group("swap").Replicas[i]
		if r.NodeName == "node3" {
			r.Role = topology.ReplicaLearner
		}
		if r.NodeName == "node4" {
			r.Role = topology.ReplicaVoter
		}
	}
	staged, err := c.Stage(ctx, "node2", desired)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.SetBarrier(ctx, node.Spec.Name, "swap", topology.StepDemoteVoter, true); err != nil {
			t.Fatal(err)
		}
	}
	id := startApply(ctx, t, c, "node2", staged.Document.Version)
	var demotion topology.AuthorizedStep
	if err := Poll(ctx, func() (bool, error) {
		controls, err := c.Controls(ctx, "node1")
		if err != nil {
			return false, err
		}
		for _, b := range controls.Barriers {
			if b.Hits > 0 && b.Last.OperatorID == id && b.Last.Kind == topology.StepDemoteVoter {
				demotion = b.Last
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("explicit transfer did not finish before demotion", err)
	}
	if demotion.TransferNodeName != "node1" || demotion.TransferReplicaID != replicaID(created.Document, "swap", "node1") {
		t.Fatal("wrong exact transfer guard", demotion)
	}
	// Pending topology owns transfer authority. Make node3 regain leadership
	// through a genuine election: isolate the old load leader and withhold only
	// competing vote requests at node3. Votes/responses for node3 still flow.
	armGroupReadFault(ctx, t, c, "node3", "swap", ReadFaults{DropVoteRequestsFrom: []uint64{replicaID(created.Document, "swap", "node2"), replicaID(created.Document, "swap", "node4")}})
	isolate := armGroupReadFault(ctx, t, c, "node1", "swap", ReadFaults{DropRaft: true})
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "swap")
		return local.Status.LeaderID == demotion.ReplicaID, err
	}); err != nil {
		t.Fatal("demotion target did not regain leadership through quorum election", err)
	}
	for _, name := range []string{"node1", "node2", "node3", "node4"} {
		if err := c.SetBarrier(ctx, name, "swap", topology.StepDemoteVoter, false); err != nil {
			t.Fatal(err)
		}
	}
	waitGuard, cancelGuard := context.WithTimeout(ctx, 30*time.Second)
	defer cancelGuard()
	if err := Poll(waitGuard, func() (bool, error) {
		op, err := c.Operator(ctx, "node2", id)
		if err != nil {
			return false, err
		}
		if op.State.Terminal() {
			return true, fmt.Errorf("guarded demotion terminated with unavailable transfer target: %+v", op)
		}
		if !op.State.Waiting() || !strings.Contains(op.WaitingReason, "demotion transfer target is unavailable") {
			return false, nil
		}
		local, err := c.Local(ctx, "node3", "swap")
		return op.CurrentStep == demotion.StepSeq && op.Attempt == demotion.Attempt && local.Status.LeaderID == demotion.ReplicaID && len(local.Status.ConfState.Voters) == 4 && len(local.Status.ConfState.Learners) == 0, err
	}); err != nil {
		operator, operatorErr := c.Operator(ctx, "node2", id)
		local, localErr := c.Local(ctx, "node3", "swap")
		t.Fatal("demotion did not preserve its approved guard", err, operator, operatorErr, local, localErr)
	}
	if _, err := c.SetReadFaults(ctx, "node1", "swap", isolate.ReplicaID, "DELETE", ReadFaults{Identity: isolate.Identity}); err != nil {
		t.Fatal(err)
	}
	healCtx, cancelHeal := context.WithTimeout(ctx, 30*time.Second)
	err = c.WaitOperator(healCtx, "node2", id)
	cancelHeal()
	if err != nil {
		operator, operatorErr := c.Operator(ctx, "node2", id)
		target, targetErr := c.Local(ctx, "node1", "swap")
		demoted, demotedErr := c.Local(ctx, "node3", "swap")
		t.Fatal("guarded demotion did not resume after healing", err, operator, operatorErr, target, targetErr, demoted, demotedErr)
	}
	if err := c.WaitActive(ctx, staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, node.Spec.Name, "swap")
			return local.Status.LeaderID == demotion.TransferReplicaID && len(local.Status.ConfState.Voters) == 3 && slices.Contains(local.Status.ConfState.Learners, demotion.ReplicaID), err
		}); err != nil {
			t.Fatal("guarded transfer and demotion did not converge", node.Spec.Name, err)
		}
	}
	op, err := c.Operator(ctx, "node2", id)
	if err != nil || op.Attempt != demotion.Attempt || op.Version != demotion.Version {
		t.Fatal("demotion changed approved attempt after transfer", op, err)
	}
}
