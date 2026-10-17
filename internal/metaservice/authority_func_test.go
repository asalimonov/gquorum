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

func TestAuthorityServiceReadsRetainedGrantAndRequestedRevocation(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc := g.state().Latest()
	doc.MetaGroup().Labels = map[string]string{"test": "transfer"}
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	step := topology.Step{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, Kind: topology.StepTransferLeader}
	approval := (&mutationFixture{g: g, native: map[string]topology.NativeState{topology.MetaGroupID: fixtureNative(*doc.MetaGroup())}}).fixtureApproval(t, staged)
	approval.Plan.Steps = []topology.Step{step}
	approval.Plan.Blocks[0].Steps = []int{1}
	begin, err := g.propose(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperationID: begin.OperationID, Version: staged.Version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, Kind: step.Kind}
	transition := topology.OperationTransition{OperationID: begin.OperationID, Version: staged.Version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, State: topology.OperationRunning, At: time.Now().UTC()}
	if _, err := g.propose(ctx, metaengine.TransitionOperationProposal{Transition: transition, ProposalID: newProposalID()}); err != nil {
		t.Fatal(err)
	}
	grant, err := g.BeginEffect(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.ValidateGrant(ctx, grant); err != nil {
		t.Fatal(err)
	}
	// A Raft leader without its current process registration may retain the
	// grant, but must wait instead of dispatching an unregistered courier.
	session, err := g.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Deactivate(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := g.BeginEffect(ctx, action); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("unregistered courier did not wait", err)
	}
	if _, err := g.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	if retried, err := g.BeginEffect(ctx, action); err != nil || !retried.Equal(grant) {
		t.Fatal("activation did not resume the same grant", retried, err)
	}
	if got, err := g.ReadGrant(ctx, grant.Key); err != nil || !got.Found || !got.Grant.Equal(grant) {
		t.Fatal(got.Grant, got.Found, err)
	}
	altered := grant.Clone()
	altered.Targets[0].MemberID++
	if err := g.ValidateGrant(ctx, altered); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("changed full grant reused cached key", err)
	}
	future := grant.Clone()
	future.Key.Revision += 1000
	if err := g.ValidateGrant(ctx, future); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("unknown grant inferred from current approval", err)
	}
	status, err := g.ReadAuthority(ctx, begin.OperationID)
	if err != nil || status.RevocationRequested {
		t.Fatal(status, err)
	}
	wait, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	if _, err := g.WaitRevoked(wait, begin.OperationID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unrequested revocation reported complete", err)
	}
	successor := g.state().Latest()
	successor.MetaGroup().Labels["test"] = "supersede"
	staged, err = g.Stage(ctx, successor, successor.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Approve(ctx, staged.Version); err != nil {
		t.Fatal(err)
	}
	status, err = g.ReadAuthority(ctx, begin.OperationID)
	if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) != 1 {
		t.Fatal(status, err)
	}
	if err := g.ValidateGrant(ctx, grant); err != nil {
		t.Fatal("revocation invalidated retained proof instead of resource execution", err)
	}
	if peer, err := g.ResolveAuthorityPeer(ctx, grant, grant.Targets[0]); err != nil || peer.Node != g.Identity() || peer.Replica.ReplicaID != grant.Targets[0].ReplicaID {
		t.Fatal("retained grant did not resolve its exact process", peer, err)
	}
	foreign := grant.Targets[0]
	foreign.MemberID++
	if _, err := g.ResolveAuthorityPeer(ctx, grant, foreign); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("authority resolution redirected to another member", err)
	}
	if floors, err := g.ReadAuthorityFloors(ctx, "n1"); err != nil || len(floors) != 0 {
		t.Fatal("group-only transfer invented a host obligation", floors, err)
	}
}
