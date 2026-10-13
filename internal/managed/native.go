package managed

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReadNative asks an exact ready replica for a fresh quorum-confirmed membership
// read. It deliberately bypasses observation caches and partial status results.
// Any ready voter, follower or learner can obtain the leader's confirmation.
func (h *Host) ReadNative(parent context.Context, group string, candidates ...storage.Identity) (topology.NativeState, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctx, done, err := h.beginWork(ctx)
	if err != nil {
		return topology.NativeState{}, err
	}
	defer done()
	ids, err := h.cfg.Catalog(ctx, group)
	if err != nil {
		return topology.NativeState{}, err
	}
	ids = append(append([]storage.Identity(nil), ids...), candidates...)
	seen := make(map[storage.Identity]bool)
	unique := make([]storage.Identity, 0, len(ids))
	for _, id := range ids {
		if id.GroupID != group || id.ClusterID != h.cfg.Identity().ClusterID {
			return topology.NativeState{}, fault.ErrIdentityMismatch
		}
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	var last error = fault.ErrUnavailable
	for i, id := range unique {
		if err := ctx.Err(); err != nil {
			return topology.NativeState{}, err
		}
		deadline, _ := ctx.Deadline()
		attempt, stop := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(unique)-i))
		state, err := h.readNative(attempt, id)
		stop()
		if err == nil {
			return state, nil
		}
		if errors.Is(err, fault.ErrUnreadable) {
			return topology.NativeState{}, err
		}
		last = err
	}
	return topology.NativeState{}, fault.New(fault.Unavailable, "no ready replica confirmed native membership", last)
}

func (h *Host) readNative(ctx context.Context, id storage.Identity) (topology.NativeState, error) {
	peer, err := h.peerFor(ctx, id)
	if err != nil {
		return topology.NativeState{}, err
	}
	query := load.ReplicaQuery{From: h.cfg.Identity(), To: peer, Native: true}
	var reply load.ReplicaStatus
	if peer.Node == query.From {
		reply, err = h.ReadReplica(ctx, query)
	} else {
		reply, err = h.cfg.Transport.ReadReplica(ctx, query)
	}
	if err != nil {
		return topology.NativeState{}, err
	}
	if err := ctx.Err(); err != nil {
		return topology.NativeState{}, err
	}
	if reply.Peer != peer {
		return topology.NativeState{}, fault.New(fault.Unreadable, "native read returned a different peer", nil)
	}
	if !reply.Found || reply.Native == nil {
		return topology.NativeState{}, fault.ErrReplicaNotReady
	}
	state := *reply.Native
	if state.GroupID != id.GroupID || state.ReplicaID != id.ReplicaID || state.Applied == 0 {
		return topology.NativeState{}, fault.New(fault.Unreadable, "native read returned a different replica or no applied index", nil)
	}
	return state.Clone(), nil
}
