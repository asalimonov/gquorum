package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestPublicNodeReadmissionRetiresFormerHostAndRecoversFreshReplica(t *testing.T) {
	for _, lostMeta := range []bool{true, false} {
		name := "retained_meta"
		if lostMeta {
			name = "lost_meta"
		}
		t.Run(name, func(t *testing.T) { testPublicNodeReadmission(t, lostMeta) })
	}
}

func testPublicNodeReadmission(t *testing.T, lostMeta bool) {
	retired := make(chan load.RetirementRequest, 16)
	c := newAPICluster(t, 3, true, func(_ int, o *quorum.NodeOptions) {
		o.Loads.Retire = func(_ context.Context, r load.RetirementRequest) (load.RetirementResult, error) {
			retired <- r
			return load.RetirementResult{Identity: r.Identity, Reason: r.Reason, Authorization: r.Authorization, AdmissionID: r.AdmissionID, PreviousMemberID: r.PreviousMemberID, GrantedMemberID: r.GrantedMemberID, Verified: true}, nil
		}
	})
	c.startAll(t)
	ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()
	leader := c.leader(t)
	if leader == c.nodes[2] {
		if err := leader.Cluster().TransferLeader(ctx, topology.MetaGroupID, "n1"); err != nil {
			t.Fatal(err)
		}
		apiEventually(t, func() bool { return !c.nodes[2].Cluster().IsLeader() })
		leader = c.leader(t)
	}
	result, err := leader.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "readmitted"}, quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if s, e := leader.Cluster().WaitOperator(ctx, result.OperatorID); e != nil || s.State != topology.OperatorSucceeded {
		t.Fatal(s, e)
	}
	apiEventually(t, func() bool {
		d, e := leader.Cluster().Active(ctx)
		return e == nil && d.Version == result.Document.Version
	})
	var group *load.Group
	apiEventually(t, func() bool {
		for _, n := range c.nodes {
			if g, ok := n.LoadGroup("readmitted"); ok && g.IsLeader() {
				group = g
				return true
			}
		}
		return false
	})
	if _, err := group.Propose(ctx, load.Proposal{RequestID: "retained", Data: []byte{11}}); err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool { return c.value("n3", "readmitted") == 11 })
	previous := c.nodes[2].Identity()
	oldGroup, _ := c.nodes[2].LoadGroup("readmitted")
	if err := c.nodes[2].Stop(ctx); err != nil {
		t.Fatal(err)
	}
	opts := c.options[2]
	if lostMeta {
		if err := os.Remove(opts.Meta.DBPath); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := leader.Cluster().ReadmitNode(ctx, "n3", quorum.MutationOptions{Approve: true}); err != nil {
			t.Fatal(err)
		}
	}
	l, err := net.Listen("tcp", opts.Meta.Network.AdvertiseAddress)
	if err != nil {
		t.Fatal(err)
	}
	opts.Meta.Network.Listener = l
	if lostMeta {
		opts.Identity.MemberID = 0
	} else {
		opts.Identity.MemberID = previous.MemberID
	}
	opts.Identity.InstanceID += "-readmitted"
	var seed string
	for i, n := range c.nodes {
		if n == leader {
			seed = c.options[i].Meta.Network.AdvertiseAddress
		}
	}
	opts.Bootstrap = quorum.BootstrapOptions{Join: true, JoinSeeds: []string{seed}}
	n, err := quorum.NewNode(opts)
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	c.nodes[2] = n
	c.network.install(n)
	if err = n.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	if err = n.WaitReady(ctx); err != nil {
		for _, node := range c.nodes {
			t.Logf("readmission node: %+v", node.Status())
		}
		t.Fatal("readmission failed", err)
	}
	if n.Identity().MemberID == previous.MemberID {
		t.Fatal("readmission reused member identity")
	}
	apiEventually(t, func() bool {
		g, ok := n.LoadGroup("readmitted")
		return ok && g.ReplicaID() != oldGroup.ReplicaID() && c.value("n3", "readmitted") == 11
	})
	select {
	case r := <-retired:
		if r.Identity != oldGroup.Identity() || r.Reason != load.RetirementReadmission || r.Authorization != nil || r.AdmissionID == "" || r.PreviousMemberID != previous.MemberID || r.GrantedMemberID != n.Identity().MemberID {
			t.Fatal("incorrect retirement grant", r)
		}
	default:
		t.Fatal("readmission omitted application verification")
	}
}
