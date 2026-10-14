package metaservice

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) metaHostEffect(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	a := grant.Action
	var target topology.AuthorityTarget
	for _, candidate := range grant.Targets {
		if candidate.NodeName == a.NodeName && candidate.ReplicaID == a.ReplicaID {
			target = candidate
			break
		}
	}
	peer, err := g.ResolveAuthorityPeer(ctx, grant, target)
	if err != nil {
		return nil, err
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID,
		NodeName: target.NodeName}
	request := load.FenceRequest{From: g.Identity(), To: peer.Node, Resource: resource, Grant: grant}
	proof, err := g.sendFence(ctx, request)
	if err != nil {
		return nil, err
	}
	if proof.From != request.To || proof.Evidence.Receiver != request.To || proof.Evidence.Resource != resource ||
		proof.Evidence.ReplicaID != 0 || proof.Evidence.Validate(grant) != nil {
		return nil, fault.ErrOperatorAuthorization
	}
	if err := g.RecordFence(ctx, proof.Evidence); err != nil {
		return nil, err
	}
	return g.sendMetaAction(ctx, load.ActionRequest{
		From:          g.Identity(),
		To:            peer,
		Grant:         grant,
		Authorization: *a,
		LocalTarget: storage.Identity{
			ClusterID: grant.Key.ClusterID,
			GroupID:   topology.MetaGroupID,
			NodeName:  target.NodeName,
			ReplicaID: target.ReplicaID,
		},
	})
}

func (g *Group) forwardMetaEffect(ctx context.Context,
	grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	engine := g.engine.Load()
	if engine == nil {
		return nil, fault.ErrReplicaNotReady
	}
	leader := engine.Status().LeaderID
	for _, target := range grant.Targets {
		if target.ReplicaID != leader {
			continue
		}
		peer, err := g.ResolveAuthorityPeer(ctx, grant, target)
		if err != nil {
			return nil, err
		}
		return g.sendMetaAction(ctx, load.ActionRequest{From: g.Identity(), To: peer, Grant: grant,
			Authorization: *grant.Action})
	}
	return nil, fault.ErrUnavailable
}

func (g *Group) sendMetaAction(ctx context.Context, request load.ActionRequest) ([]topology.EffectReceipt, error) {
	var result load.ActionResult
	var err error
	if request.To.Node == g.Identity() {
		result, err = g.ApplyMetaAction(ctx, request)
	} else {
		var data []byte
		data, err = json.Marshal(request)
		if err != nil {
			return nil, err
		}
		address, err := g.resolveNode(ctx, request.To.Node.NodeName)
		if err != nil {
			return nil, err
		}
		reply, callErr := g.transport.CallAddress(ctx, address, "MetaControl", data)
		if callErr != nil {
			return nil, callErr
		}
		if err := metatransport.ReplyError(reply); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(reply.Data, &result)
	}
	if err != nil {
		return nil, err
	}
	return metaActionReceipts(request, result)
}

func metaActionReceipts(request load.ActionRequest, result load.ActionResult) ([]topology.EffectReceipt, error) {
	if result.From != request.To || result.Authorization != request.Authorization ||
		result.LocalTarget != request.LocalTarget || len(result.Receipts) != 1 {
		return nil, fault.ErrOperatorAuthorization
	}
	receipt := result.Receipts[0]
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}
	if request.LocalTarget != (storage.Identity{}) {
		resource.Kind, resource.NodeName = topology.AuthorityHost, request.To.Node.NodeName
		if receipt.Target != (topology.AuthorityTarget{NodeName: request.To.Node.NodeName,
			MemberID:  request.To.Node.MemberID,
			ReplicaID: request.LocalTarget.ReplicaID}) {
			return nil, fault.ErrOperatorAuthorization
		}
	}
	if receipt.Resource != resource || receipt.Validate(request.Grant) != nil {
		return nil, fault.ErrOperatorAuthorization
	}
	return []topology.EffectReceipt{receipt.Clone()}, nil
}

// ApplyMetaAction validates and dispatches a granted meta effect for an exact
// target.
func (g *Group) ApplyMetaAction(parent context.Context, request load.ActionRequest) (load.ActionResult, error) {
	ctxHandle, err := g.beginRPC(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.ActionResult{}, err
	}
	defer release()
	request.Grant = request.Grant.Clone()
	id := g.Identity()
	if request.To.Node != id ||
		request.To.Replica != (load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}) ||
		request.Grant.Key.GroupID != topology.MetaGroupID || request.Grant.Key.ClusterID != id.ClusterID {
		return load.ActionResult{}, fault.ErrIdentityMismatch
	}
	if err := g.ValidateAuthorityCourier(ctx, request.Grant, request.From); err != nil {
		return load.ActionResult{}, err
	}
	if request.Grant.Action == nil || *request.Grant.Action != request.Authorization {
		return load.ActionResult{}, fault.ErrOperatorAuthorization
	}
	if request.LocalTarget == (storage.Identity{}) {
		return g.applyMetaConsensusAction(ctx, request)
	}
	if request.Authorization.Kind != topology.StepPrepareReplica &&
		request.Authorization.Kind != topology.StepStopReplica {
		return load.ActionResult{}, fault.ErrOperatorAuthorization
	}
	want := storage.Identity{ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, NodeName: id.NodeName,
		ReplicaID: id.MemberID}
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}
	if request.LocalTarget != want || !slices.Contains(request.Grant.Targets, target) {
		return load.ActionResult{}, fault.ErrIdentityMismatch
	}
	if g.opts.ApplyLocalMetaAction == nil {
		return load.ActionResult{}, fault.ErrNoController
	}
	return g.opts.ApplyLocalMetaAction(ctx, request)
}

func (g *Group) applyMetaConsensusAction(parent context.Context,
	request load.ActionRequest) (load.ActionResult, error) {
	if !g.IsLeader() {
		return load.ActionResult{}, fault.ErrNotLeader
	}
	if request.Authorization.Kind == topology.StepPrepareReplica ||
		request.Authorization.Kind == topology.StepStopReplica {
		return load.ActionResult{}, fault.ErrOperatorAuthorization
	}
	ctxHandle, err := g.acquireEffectContext(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.ActionResult{}, err
	}
	defer done()
	receipts, err := g.metaEffect(ctx, request.Grant)
	return load.ActionResult{From: request.To, Authorization: request.Authorization, Receipts: receipts}, err
}
