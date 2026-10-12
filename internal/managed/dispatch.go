package managed

import (
	"context"
	"errors"
	"sync"

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
	if !completePeer(peer) || peer.Replica != ref || peer.Node.NodeName != id.NodeName || peer.Node.ClusterID != id.ClusterID {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	return peer, nil
}

func (h *Host) Dispatch(parent context.Context, a topology.AuthorizedStep) error {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctx, done, err := h.beginWork(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := h.cfg.Authorize(ctx, a); err != nil {
		return err
	}
	targets, err := h.cfg.ActionTargets(ctx, a)
	if err != nil {
		return err
	}
	if len(targets) == 0 && a.Kind != topology.StepDeleteGroupState {
		return fault.ErrOperatorAuthorization
	}
	if localAction(a.Kind) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var failures []error
		for _, id := range targets {
			wg.Go(func() {
				peer, err := h.peerFor(ctx, id)
				if err == nil {
					err = h.sendAction(ctx, load.ActionRequest{From: h.cfg.Identity(), To: peer, Authorization: a, LocalTarget: id})
				}
				if err != nil {
					mu.Lock()
					failures = append(failures, err)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		if err := errors.Join(failures...); err != nil {
			return err
		}
	} else {
		status, err := h.ObserveGroup(ctx, a.GroupID)
		if err != nil {
			return err
		}
		if status.LeaderID == 0 {
			return fault.ErrUnavailable
		}
		peer, err := h.cfg.ResolvePeer(ctx, load.ReplicaRef{GroupID: a.GroupID, ReplicaID: status.LeaderID})
		if err != nil {
			return err
		}
		if !completePeer(peer) || peer.Replica.GroupID != a.GroupID || peer.Replica.ReplicaID != status.LeaderID {
			return fault.ErrIdentityMismatch
		}
		if err := h.sendAction(ctx, load.ActionRequest{From: h.cfg.Identity(), To: peer, Authorization: a}); err != nil {
			return err
		}
	}
	return h.cfg.Authorize(ctx, a)
}

func (h *Host) sendAction(ctx context.Context, req load.ActionRequest) error {
	if err := h.cfg.Authorize(ctx, req.Authorization); err != nil {
		return err
	}
	local := req.To.Node == h.cfg.Identity()
	result, err := h.cfg.Transport.ApplyAction(ctx, req)
	if err != nil {
		// A remote participant can still lack the approved plan or the applied
		// process session used for routing. Keep its refusal separate from an
		// affirmative attestation; retry only while our full approval stays valid.
		if !local && (errors.Is(err, fault.ErrOperatorAuthorization) || errors.Is(err, fault.ErrIdentityMismatch)) && h.cfg.Authorize(ctx, req.Authorization) == nil {
			return fault.New(fault.Unavailable, "remote participant refused the current approval or identity", err)
		}
		return err
	}
	if !result.Verified || result.From != req.To || result.Authorization != req.Authorization || result.LocalTarget != req.LocalTarget {
		return fault.ErrOperatorAuthorization
	}
	return h.cfg.Authorize(ctx, req.Authorization)
}

// Effects adapts the concrete host to the private operator boundary. Lifecycle
// methods remain separate from approved distributed StopReplica dispatch.
func (h *Host) Effects() operator.GroupEffects { return effects{h} }

type effects struct{ host *Host }

func (e effects) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	return e.host.ObserveGroup(ctx, id)
}
func (e effects) invoke(ctx context.Context, a topology.AuthorizedStep, kind topology.StepKind) error {
	if a.Kind != kind {
		return fault.ErrOperatorAuthorization
	}
	return e.host.Dispatch(ctx, a)
}
func (e effects) CreateGroup(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepCreateFoundingGroup)
}
func (e effects) PrepareReplica(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepPrepareReplica)
}
func (e effects) TransferLeader(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepTransferLeader)
}
func (e effects) StopReplica(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepStopReplica)
}
func (e effects) CleanupReplicaState(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepCleanupReplicaState)
}
func (e effects) DeleteGroupState(ctx context.Context, a topology.AuthorizedStep) error {
	return e.invoke(ctx, a, topology.StepDeleteGroupState)
}
func (e effects) ApplyMembership(ctx context.Context, a topology.AuthorizedStep) error {
	switch a.Kind {
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
		return e.host.Dispatch(ctx, a)
	default:
		return fault.ErrOperatorAuthorization
	}
}
