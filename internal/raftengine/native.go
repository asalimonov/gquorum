package raftengine

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/tracker"
)

// ReadNative captures applied native membership on the Raft owner. A separate
// ReadBarrier followed by cached Status could instead read an earlier cache.
// This read is independent of the application's optional Query implementation.
func (g *Group) ReadNative(parent context.Context) (topology.NativeState, error) {
	ctx, cancel := context.WithTimeout(parent, g.opts.Reads.Timeout)
	defer cancel()
	r, err := g.waitRead(&readRequest{ctx: ctx, native: true, reply: make(chan response, 1)})
	var state topology.NativeState
	if err == nil {
		err = json.Unmarshal(r.Data, &state)
	}
	return state, err
}

// nativeState is called only by the serialized Raft owner.
func (g *Group) nativeState() topology.NativeState {
	status := g.rn.Status()
	pt := tracker.ProgressTracker{Config: status.Config}
	return topology.NativeState{GroupID: g.opts.GroupID, ReplicaID: g.opts.ReplicaID,
		Applied: g.applied, ConfState: pt.ConfState()}
}
