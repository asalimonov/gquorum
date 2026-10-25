package main

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Serving uses locally applied convergence, not a metadata quorum barrier. The
// approved member binds the process; localServing additionally checks the exact
// published replica and native role. ConvergedVersion is provenance, not a pin:
// its source document can be compacted while this declaration stays usable.
func servingGroup(ctx context.Context, cluster gquorum.Cluster, row topology.GroupProgress,
	identity topology.Identity) (*topology.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if row.Converged == nil || row.ConvergedVersion == 0 {
		return nil, fault.ErrReplicaNotReady
	}
	if !slices.ContainsFunc(row.Converged.Replicas, func(replica topology.Replica) bool {
		return replica.NodeName == identity.NodeName
	}) {
		return nil, fault.ErrReplicaNotReady
	}
	approved, err := cluster.Target(ctx)
	if err != nil {
		return nil, err
	}
	if identity.MemberID == 0 || approved.MemberID(identity.NodeName) != identity.MemberID {
		return nil, fault.ErrIdentityMismatch
	}
	return row.Converged, nil
}

func (a *application) servingGroup(ctx context.Context, id string) (*topology.Group, error) {
	a.mu.RLock()
	row := a.serving[id].Clone()
	a.mu.RUnlock()
	if row.Converged == nil || row.ConvergedVersion == 0 {
		// A just-completed creation may precede the next periodic observation.
		// Bound that initial read; established serving never probes its peers.
		a.refreshServing(ctx)
		a.mu.RLock()
		row = a.serving[id].Clone()
		a.mu.RUnlock()
	}
	return servingGroup(ctx, a.currentCluster(), row, a.currentNode().Identity())
}

// Status may probe remote replicas. Keep those observations off application
// requests: only its locally applied convergence rows feed this readiness cache.
// The existing work lifetime joins the single bounded refresher during shutdown.
func (a *application) observeServingContext(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		a.refreshServing(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (a *application) refreshServing(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 250*time.Millisecond)
	defer cancel()
	status, err := a.currentCluster().Status(ctx)
	if err != nil {
		return
	}
	groups := make(map[string]topology.GroupProgress, len(status.Groups))
	for _, row := range status.Groups {
		groups[row.GroupID] = row.Clone()
	}
	a.mu.Lock()
	// Concurrent request/bootstrap observations cannot roll a checkpoint back.
	for id, row := range groups {
		if previous := a.serving[id]; previous.ConvergedVersion > row.ConvergedVersion {
			groups[id] = previous
		}
	}
	a.serving = groups
	a.mu.Unlock()
}
