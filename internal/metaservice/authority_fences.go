package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) fenceReceiver(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource) (load.Peer, error) {
	var replica uint64
	if resource.Kind == topology.AuthorityGroup {
		status, err := (groupControllers{g}).Status(ctx, resource.GroupID)
		if err != nil {
			return load.Peer{}, err
		}
		if status.GroupID != resource.GroupID || status.LeaderID == 0 {
			return load.Peer{}, fault.ErrReplicaNotReady
		}
		replica = status.LeaderID
	}
	err := error(fault.ErrReplicaNotReady)
	for _, target := range grant.Targets {
		if resource.Kind == topology.AuthorityHost && target.NodeName != resource.NodeName ||
			resource.Kind == topology.AuthorityGroup && target.ReplicaID != replica {
			continue
		}
		var peer load.Peer
		peer, err = g.ResolveAuthorityPeer(ctx, grant, target)
		if err == nil {
			return peer, nil
		}
	}
	return load.Peer{}, err
}

func (g *Group) sendFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if request.To == g.Identity() {
		return g.InstallFence(ctx, request)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return load.FenceResult{}, err
	}
	address, err := g.resolveNode(ctx, request.To.NodeName)
	if err != nil {
		return load.FenceResult{}, err
	}
	reply, err := g.transport.CallAddress(ctx, address, "InstallFence", data)
	if err != nil {
		return load.FenceResult{}, err
	}
	if err := metatransport.ReplyError(reply); err != nil {
		return load.FenceResult{}, err
	}
	var result load.FenceResult
	if err := json.Unmarshal(reply.Data, &result); err != nil {
		return result, fault.New(fault.Unreadable, "invalid fence acknowledgement", err)
	}
	return result, nil
}
