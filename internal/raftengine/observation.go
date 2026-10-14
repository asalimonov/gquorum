package raftengine

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/replicastate"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
)

// Watch follows only this engine run; it cannot redirect to a successor.
func (g *Group) Watch(ctx context.Context) (<-chan replicastate.Observation, error) {
	return g.observations.Subscribe(ctx)
}

type observationUpdate uint8

const (
	regularObservation observationUpdate = iota
	finalObservation
)

// publishObservationLocked only uses state protected by mu. RawNode is never
// accessed here: stop and callback failure can publish outside its owner loop.
func (g *Group) publishObservationLocked(update observationUpdate) {
	if g.observation.State == replicastate.Stopped || g.observation.State == replicastate.Failed {
		return
	}
	if g.observations == nil {
		return
	} // internal value-only fixtures
	o := g.currentObservationLocked(update)
	previous := g.observation
	o.Revision = previous.Revision
	if o != previous {
		o.Revision++
		g.observation = o
	}
	if update == finalObservation {
		g.observations.Finish(o)
	} else if o != previous {
		g.observations.Publish(o)
	}
	g.refreshLeadershipLocked(o)
}

// finishObservations is called after engine workers join (or before Start).
func (g *Group) finishObservations() {
	g.mu.Lock()
	g.publishObservationLocked(finalObservation)
	g.mu.Unlock()
	_ = g.observations.Join(context.Background())
}

func (g *Group) currentObservationLocked(update observationUpdate) replicastate.Observation {
	o := replicastate.Observation{
		Identity: storage.Identity{
			NodeName:  g.opts.NodeName,
			ClusterID: g.opts.ClusterID,
			GroupID:   g.opts.GroupID,
			ReplicaID: g.opts.ReplicaID,
		},
		InstanceID: g.opts.InstanceID, RunID: g.nonce, State: replicastate.Starting,
		LeaderID: g.cached.Lead, Term: g.cached.Term, AppliedIndex: g.cached.Applied,
	}
	_, voter := g.cached.Config.Voters[0][g.opts.ReplicaID]
	_, outgoing := g.cached.Config.Voters[1][g.opts.ReplicaID]
	o.IsVoter = voter || outgoing
	if g.started && !g.stopping && !g.stopped && !g.restoring.Load() && g.ctx.Err() == nil {
		select {
		case <-g.ready:
			o.Ready = true
			o.State = replicastate.Ready
		default:
		}
	}
	o.IsLeader = o.Ready && o.IsVoter && g.cached.RaftState == raft.StateLeader
	if g.stopping || g.stopped || g.started && g.ctx.Err() != nil {
		o.State = replicastate.Stopping
		o.LeaderID = 0
	}
	g.setObservationError(&o)
	if update == finalObservation {
		o.State = replicastate.Stopped
		if o.Error != "" {
			o.State = replicastate.Failed
		}
	}
	return o
}

func (g *Group) setObservationError(o *replicastate.Observation) {
	if g.err != nil && !errors.Is(g.err, fault.ErrStopped) {
		o.Error, o.ErrorCode = g.err.Error(), fault.Unavailable
		var classified *fault.Error
		if errors.As(g.err, &classified) {
			o.ErrorCode = classified.Code
		}
	}
}
