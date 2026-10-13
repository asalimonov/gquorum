//go:build functional

package main

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type removedStartupNode struct {
	gquorum.Node
	status gquorum.NodeStatus
}

func (*removedStartupNode) WaitReady(context.Context) error { return fault.ErrStopped }
func (n *removedStartupNode) Status() gquorum.NodeStatus    { return n.status }

func TestSupersededAdmissionReadinessKeepsControlUntilRemovalPolicy(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "n4", MemberID: 4, InstanceID: "process"}
	node := &removedStartupNode{status: gquorum.NodeStatus{Identity: id, State: gquorum.NodeFenced,
		SelfRemoval: &topology.SelfRemovalStatus{Fenced: true, MemberID: 4, Version: 5, AppliedIndex: 90},
	}}
	a := &application{node: node, workCtx: t.Context(), closing: make(chan struct{})}
	a.markEstablished()
	select {
	case <-a.closing:
		t.Fatal("caller closed control before required host retirement")
	default:
	}
	if a.fatalErr != nil {
		t.Fatal("approved pre-readiness removal was treated as startup failure", a.fatalErr)
	}
}
