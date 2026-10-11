package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaFollowerApprovesEmptyPlanAndActivationIsDurable(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test-cluster")
	}
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := follower.Latest(ctx)
	doc.Nodes[0].Labels = map[string]string{"zone": "b"}
	doc.Origin = topology.OriginOperator
	staged, err := follower.Stage(ctx, doc, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := follower.PlanFor(ctx, staged)
	if err != nil || !plan.IsEmpty() {
		t.Fatal(plan, err)
	}
	foreign := staged.Clone()
	foreign.ClusterID = "foreign-cluster"
	if _, err := follower.PlanFor(ctx, foreign); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("preview planned a foreign cluster", err)
	}
	if follower.state().Target().Version != 1 {
		t.Fatal("dry planning approved intent")
	}
	id, err := follower.Apply(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	status, err := follower.WaitOperator(ctx, id)
	if err != nil || status.State != topology.OperatorSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return follower.state().Active().Version == 2 })
	same, err := follower.Apply(ctx, 2)
	if err != nil || same != id {
		t.Fatal("approval retry allocated another attempt", same, err)
	}
	if _, err = follower.Apply(ctx, 1); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("approved stale historical version", err)
	}
	for _, g := range groups {
		waitPublic(t, func() bool { return g.state().Active().Version == 2 })
	}
}

func TestMetaDrainTransfersLeadershipAndKeepsEveryAdmittedVoter(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test-cluster")
	}
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	departing := leader.currentIdentity()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	doc, _ := leader.Latest(ctx)
	doc.Node(departing.NodeName).State = topology.NodeDraining
	doc.Origin = topology.OriginOperator
	staged, err := leader.Stage(ctx, doc, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := leader.PlanFor(ctx, staged)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Kind != topology.StepTransferLeader || plan.Steps[0].ReplicaID == departing.MemberID {
		t.Fatal("drain did not plan a retained leader", plan)
	}
	id, err := leader.Apply(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	status, err := leader.WaitOperator(ctx, id)
	if err != nil || status.State != topology.OperatorSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == 2 })
	if leader.IsLeader() {
		t.Fatal("drained node retained leadership")
	}
	active := leader.state().Active()
	if len(active.MetaGroup().Replicas) != 3 || active.MemberID(departing.NodeName) != departing.MemberID {
		t.Fatal("drain removed meta participation")
	}
	if topology.EffectiveNodeState(active, departing.NodeName) != topology.NodeDrained {
		t.Fatal("empty load placement did not finish drain")
	}
}

func TestMetaUnknownLoadControllerRefusesApprovalWithoutChangingTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test-cluster")
	}
	g := readyPublic(t, publicFoundingCluster(t, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := g.Latest(ctx)
	doc.Origin = topology.OriginOperator
	doc.Groups = append(doc.Groups, topology.Group{ID: "load-1", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	staged, err := g.Stage(ctx, doc, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.Apply(ctx, staged.Version); !errors.Is(err, fault.ErrNoController) {
		t.Fatal("approval has no load controller", err)
	}
	if g.state().Target().Version != 1 || g.state().Latest().Version != 2 {
		t.Fatal("failed approval rolled back stage or changed Target")
	}
}
