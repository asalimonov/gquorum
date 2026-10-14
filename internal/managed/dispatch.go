package managed

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (h *Host) peerFor(ctx context.Context, id storage.Identity) (load.Peer, error) {
	ref := load.ReplicaRef{GroupID: id.GroupID, ReplicaID: id.ReplicaID}
	peer, err := h.cfg.ResolvePeer(ctx, ref)
	if err != nil {
		return peer, err
	}
	if peer.Replica != ref || peer.Node.NodeName != id.NodeName || peer.Node.ClusterID != id.ClusterID ||
		!completePeer(peer) {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	return peer, nil
}

// Effects adapts the concrete host to the private operator boundary. Lifecycle
// methods remain separate from approved distributed StopReplica dispatch.
func (h *Host) Effects() operator.GroupEffects { return effects{h} }

type effects struct{ host *Host }

func (e effects) ReadNative(ctx context.Context, id string,
	candidates ...storage.Identity) (topology.NativeState, error) {
	return e.host.ReadNative(ctx, id, candidates...)
}

func (e effects) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	return e.host.ObserveGroup(ctx, id)
}

func (e effects) invoke(ctx context.Context, a topology.AuthorityGrant,
	kind topology.StepKind) ([]topology.EffectReceipt, error) {
	if a.Action == nil || a.Action.Kind != kind {
		return nil, fault.ErrOperatorAuthorization
	}
	return e.host.DispatchGrant(ctx, a)
}

func (e effects) CreateGroup(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepCreateFoundingGroup)
}

func (e effects) PrepareReplica(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepPrepareReplica)
}

func (e effects) TransferLeader(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepTransferLeader)
}

func (e effects) StopReplica(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepStopReplica)
}

func (e effects) CleanupReplicaState(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepCleanupReplicaState)
}

func (e effects) DeleteGroupState(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.invoke(ctx, a, topology.StepDeleteGroupState)
}

func (e effects) ApplyMembership(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if a.Action == nil {
		return nil, fault.ErrOperatorAuthorization
	}
	switch a.Action.Kind {
	case topology.StepAddLearner,
		topology.StepPromoteVoter,
		topology.StepDemoteVoter,
		topology.StepRemoveReplica:
		return e.host.DispatchGrant(ctx, a)
	default:
		return nil, fault.ErrOperatorAuthorization
	}
}
