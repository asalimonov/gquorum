//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestExactMetaDispatchReturnsOneDurableReceipt(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc := g.state().Latest()
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	step := topology.Step{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, Kind: topology.StepTransferLeader}
	at := time.Now().UTC()
	begin, err := g.propose(ctx, metaengine.BeginOperatorProposal{Version: staged.Version, Plan: topology.Plan{Version: staged.Version, Steps: []topology.Step{step}}, At: at})
	if err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: staged.Version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, Kind: step.Kind}
	transition := topology.OperatorTransition{OperatorID: begin.OperatorID, Version: staged.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, NodeName: step.NodeName, State: topology.OperatorRunning, At: at.Add(time.Millisecond)}
	if _, err := g.propose(ctx, metaengine.TransitionOperatorProposal{Transition: transition}); err != nil {
		t.Fatal(err)
	}
	grant, err := g.BeginEffect(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	c := groupControllers{g: g}
	receipt, err := c.DispatchEffect(ctx, grant, grant.Resources[0], grant.Targets[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := receipt.Validate(grant); err != nil {
		t.Fatal(err)
	}
	if receipt.AppliedIndex == 0 || receipt.AppliedTerm == 0 {
		t.Fatal("missing committed receipt", receipt)
	}
	if err := g.RecordEffect(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DispatchEffect(ctx, grant, grant.Resources[0], topology.AuthorityTarget{}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("zero target accepted", err)
	}
	foreign := grant.Targets[0]
	foreign.MemberID++
	if _, err := c.DispatchEffect(ctx, grant, grant.Resources[0], foreign); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("foreign target accepted", err)
	}
	// The exact native path has no forwarding fallback, including before a
	// participant starts. This is also checked after action-context acquisition.
	if _, err := (&Group{}).applyNativeMetaEffect(ctx, grant); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
}
