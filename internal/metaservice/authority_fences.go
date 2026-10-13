package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// ReconcileFences is independent of the current execution cursor. Failed,
// superseded and empty operators can all leave durable revocation obligations.
func (g *Group) ReconcileFences(parent context.Context) error {
	ctx, release, err := g.beginEffect(parent)
	if err != nil {
		return err
	}
	defer release()
	if !g.IsLeader() {
		return fault.ErrNotLeader
	}
	pending := latestPendingFences(g.state().PendingFences())
	jobs := make(chan topology.FenceObligation, len(pending))
	for _, obligation := range pending {
		jobs <- obligation
	}
	close(jobs)
	var workers sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for range min(8, len(pending)) {
		workers.Go(func() {
			for obligation := range jobs {
				// An unreachable resource must not prevent independent resources
				// from completing. Receiver-owned work survives this bounded wait.
				attempt, cancel := context.WithTimeout(ctx, g.opts.Timing.DialTimeout)
				err := g.reconcileFence(attempt, obligation)
				cancel()
				if err != nil {
					mu.Lock()
					failures = append(failures, err)
					mu.Unlock()
				}
			}
		})
	}
	workers.Wait()
	return errors.Join(append(failures, ctx.Err())...)
}

func latestPendingFences(obligations []topology.FenceObligation) []topology.FenceObligation {
	latest := make(map[topology.AuthorityResource]topology.FenceObligation)
	for _, obligation := range obligations {
		if obligation.Evidence != nil {
			continue
		}
		if old, found := latest[obligation.Resource]; !found || old.Key.Revision < obligation.Key.Revision {
			latest[obligation.Resource] = obligation
		}
	}
	result := make([]topology.FenceObligation, 0, len(latest))
	for _, obligation := range latest {
		result = append(result, obligation)
	}
	slices.SortFunc(result, func(a, b topology.FenceObligation) int { return a.Resource.Compare(b.Resource) })
	return result
}

func (g *Group) reconcileFence(ctx context.Context, obligation topology.FenceObligation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.IsLeader() {
		return fault.ErrNotLeader
	}
	grant, found := g.state().AuthorityGrant(obligation.Key)
	if !found || grant.Purpose != topology.AuthorityFence || !slices.Contains(grant.Resources, obligation.Resource) {
		return fault.ErrUnreadable
	}
	peer, err := g.fenceReceiver(ctx, grant, obligation.Resource)
	if err != nil {
		return err
	}
	replica := peer.Replica.ReplicaID
	if obligation.Resource.Kind == topology.AuthorityHost {
		replica = 0
	}
	request := load.FenceRequest{From: g.Identity(), To: peer.Node, ReplicaID: replica, Resource: obligation.Resource, Grant: grant}
	result, err := g.sendFence(ctx, request)
	if err != nil {
		return err
	}
	if result.From != request.To || result.Evidence.Receiver != request.To || result.Evidence.Resource != request.Resource || result.Evidence.ReplicaID != request.ReplicaID || result.Evidence.Validate(grant) != nil {
		return fault.ErrOperatorAuthorization
	}
	// Recording a valid older boundary is safe if leadership or approval changed
	// during the call. The replicated catalog checks it against all newer proofs.
	return g.RecordFence(ctx, result.Evidence)
}

func (g *Group) fenceReceiver(ctx context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource) (load.Peer, error) {
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
		if resource.Kind == topology.AuthorityHost && target.NodeName != resource.NodeName || resource.Kind == topology.AuthorityGroup && target.ReplicaID != replica {
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
