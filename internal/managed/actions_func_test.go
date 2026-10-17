//go:build functional

package managed

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (_ *fixture) approve(kind topology.StepKind, targets ...storage.Identity) topology.AuthorizedStep {
	a := topology.AuthorizedStep{OperationID: topology.MakeOperationID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, Kind: kind, GroupID: "work"}
	if kind != topology.StepCreateFoundingGroup && kind != topology.StepDeleteGroupState {
		a.NodeName, a.ReplicaID = targets[0].NodeName, targets[0].ReplicaID
	}
	switch kind {
	case topology.StepCreateFoundingGroup:
		a.ExpectedVotersDigest = topology.VoterSetDigest(nil)
		ids := make([]uint64, 0, len(targets))
		for _, target := range targets {
			ids = append(ids, target.ReplicaID)
		}
		a.ResultVotersDigest = topology.VoterSetDigest(ids)
	case topology.StepAddLearner:
		a.ExpectedVotersDigest, a.ResultVotersDigest = topology.VoterSetDigest([]uint64{1}), topology.VoterSetDigest([]uint64{1})
	case topology.StepPromoteVoter:
		a.ExpectedVotersDigest, a.ResultVotersDigest = topology.VoterSetDigest([]uint64{1}), topology.VoterSetDigest([]uint64{1, a.ReplicaID})
	}
	return a
}

func TestMembershipTargetsTheLearnerThroughADifferentLeaderReceiver(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, founding := fixtureAuthority(t, f)
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), founding)
	must(t, err)
	g, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	await(deadline(t), t, g.Core().IsLeader)
	target := storage.Identity{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}
	a := f.approve(topology.StepAddLearner, target)
	req := f.request(a, f.localID(1))
	req.LocalTarget = storage.Identity{}
	grant := founding.Clone()
	grant.Key.Revision++
	grant.Action, grant.Resources = &a, grant.Resources[:1]
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	req.Grant = grant
	await(deadline(t), t, func() bool {
		_, err = g.Core().InstallAuthority(deadline(t), grant)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	var result load.ActionResult
	await(deadline(t), t, func() bool {
		result, err = h.ApplyAction(deadline(t), req)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if len(result.Receipts) != 1 || !slices.Contains(g.Core().Status().ConfState.Learners, 2) {
		t.Fatal(result, g.Core().Status())
	}
	a = f.approve(topology.StepPromoteVoter, target)
	req.Authorization = a
	grant.Key.Revision++
	grant.Action = &a
	req.Grant = grant
	await(deadline(t), t, func() bool {
		_, err = g.Core().InstallAuthority(deadline(t), grant)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	await(deadline(t), t, func() bool {
		_, err := h.ApplyAction(deadline(t), req)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		if !errors.Is(err, fault.ErrUnsafeMembership) {
			t.Fatal("promotion bypassed fixed catch-up rules", err)
		}
		return true
	})
	if slices.Contains(g.Core().Status().ConfState.Voters, 2) {
		t.Fatal("unready learner became a voter")
	}
}
func (f *fixture) localID(replica uint64) storage.Identity {
	return storage.Identity{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID, GroupID: "work", ReplicaID: replica}
}
func (f *fixture) request(a topology.AuthorizedStep, id storage.Identity) load.ActionRequest {
	return load.ActionRequest{From: f.node, To: load.Peer{Node: f.node, Replica: load.ReplicaRef{GroupID: id.GroupID, ReplicaID: id.ReplicaID}}, Authorization: a, LocalTarget: id}
}

func TestGrantedCreationAndRefusedIdentities(t *testing.T) {
	functional(t)
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	transport := &grantedLoopback{}
	f.cfg.Transport = transport
	var factories atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	transport.host = h
	id := f.localID(1)
	a := f.approve(topology.StepCreateFoundingGroup, id)
	req := f.request(a, id)
	req.Grant = grant
	for _, mutate := range []func(*load.ActionRequest){
		func(r *load.ActionRequest) { r.Grant = topology.AuthorityGrant{} },
		func(r *load.ActionRequest) { r.LocalTarget.ReplicaID++ },
		func(r *load.ActionRequest) { r.To.Node.InstanceID = "stale" },
		func(r *load.ActionRequest) { r.Authorization.Attempt++ },
		func(r *load.ActionRequest) { r.Authorization.Version++ },
		func(r *load.ActionRequest) { r.Authorization.StepSeq++ },
		func(r *load.ActionRequest) { r.Authorization.Kind = topology.StepDeleteGroupState },
	} {
		bad := req
		mutate(&bad)
		if reply, err := h.ApplyAction(deadline(t), bad); err == nil || len(reply.Receipts) != 0 {
			t.Fatal(reply, err)
		}
	}
	if factories.Load() != 0 {
		t.Fatal("refused action reached factory")
	}
	// Dispatch cancellation ends its wait, not the accepted startup. Retry the
	// exact approval through the existing owner when parallel disk work exceeds
	// the fixture's one-second effect deadline. The factory-once assertion below
	// still requires retries to share the accepted operation.
	ctx := deadline(t)
	await(ctx, t, func() bool {
		_, err := h.DispatchGrant(ctx, grant)
		if errors.Is(err, fault.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, fault.ErrReplicaNotReady) || errors.Is(err, fault.ErrAuthorityPending) || errors.Is(err, fault.ErrNotLeader) {
			return false
		}
		must(t, err)
		return true
	})
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if len(result.Receipts) != 1 || result.From != req.To || result.Authorization != a || result.LocalTarget != id || factories.Load() != 1 {
		t.Fatal(result, factories.Load())
	}
}
