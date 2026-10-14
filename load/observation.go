package load

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/replicastate"
)

// ReplicaState describes the lifetime of one exact local engine run.
type ReplicaState = replicastate.State

// ReplicaStarting and the following states describe the observed replica
// lifecycle.
const (
	ReplicaStarting = replicastate.Starting
	ReplicaReady    = replicastate.Ready
	ReplicaStopping = replicastate.Stopping
	ReplicaStopped  = replicastate.Stopped
	ReplicaFailed   = replicastate.Failed
)

// Observation is an owned local snapshot. RunID changes on every engine open;
// Revision increases within that run. Leadership and readiness are not
// barriers.
type Observation = replicastate.Observation

// Watch atomically seeds current state and then coalesces updates into one
// buffered slot. Revision gaps indicate omitted intermediate states. A terminal
// run retains its final snapshot and closes the channel; it never follows a
// replacement. Canceling ctx closes only this subscription. Slow consumers
// never block Raft, and exceeding EngineOptions.MaxWatchers returns
// ErrOverloaded.
func (g *Group) Watch(ctx context.Context) (<-chan Observation, error) {
	if core := g.core(); core != nil {
		return core.Watch(ctx)
	}
	return nil, fault.ErrInvalid
}
