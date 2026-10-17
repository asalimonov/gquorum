package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// ReadProgress confirms quorum and local apply for one metadata page. Cached
// native observations retain their own provenance; no host is queried here.
func (g *Group) ReadProgress(ctx context.Context, query topology.ProgressQuery) (topology.ProgressPage, error) {
	if err := query.Validate(); err != nil {
		return topology.ProgressPage{}, err
	}
	var page topology.ProgressPage
	if err := g.read(ctx, metaengine.QueryRequest{Kind: "progress", Progress: &query}, &page); err != nil {
		return topology.ProgressPage{}, err
	}
	g.decorateProgress(page.Groups)
	return page, nil
}

func (g *Group) decorateProgress(groups []topology.GroupProgress) {
	// Copy only returned candidates. Never retain the cache lock while entering
	// the FSM, and never refresh timestamps when attaching an old observation.
	cached := make(map[string]topology.GroupStatus, len(groups))
	g.mu.RLock()
	for _, row := range groups {
		if observed, found := g.lastLoadStatus[row.GroupID]; found {
			cached[row.GroupID] = observed.Clone()
		}
	}
	g.mu.RUnlock()
	for i := range groups {
		row := &groups[i]
		row.Observation = nil
		observed, found := cached[row.GroupID]
		if !found {
			continue
		}
		process, current := g.state().ProcessIdentity(observed.Reporter.NodeName)
		if current && matchingProgressObservation(*row, observed, process) {
			observed.LeaderNodeName = progressLeaderName(*row, observed.LeaderID)
			row.Observation = &observed
		}
	}
}

func matchingProgressObservation(row topology.GroupProgress, observed topology.GroupStatus,
	process topology.Identity) bool {
	if !observed.Found || observed.GroupID != row.GroupID || observed.SourceVersion != row.ConvergedVersion ||
		observed.SourceVersion == 0 || process.MemberID == 0 || process.InstanceID == "" ||
		observed.Reporter != process || observed.ReporterReplicaID == 0 ||
		observed.HostNode != "" && observed.HostNode != process.NodeName || observed.ReceivedAt.IsZero() {
		return false
	}
	if row.GroupID == topology.MetaGroupID && observed.ReporterReplicaID != process.MemberID {
		return false
	}
	for _, declaration := range []*topology.Group{row.Converged, row.Approved} {
		if progressHasReplica(declaration, process.NodeName, observed.ReporterReplicaID) {
			return true
		}
	}
	return false
}

func progressHasReplica(group *topology.Group, node string, replica uint64) bool {
	if group != nil {
		for _, member := range group.Replicas {
			if member.ReplicaID == replica && member.NodeName == node {
				return true
			}
		}
	}
	return false
}

func progressLeaderName(row topology.GroupProgress, id uint64) string {
	if id == 0 {
		return ""
	}
	for _, group := range []*topology.Group{row.Converged, row.Approved} {
		if group == nil {
			continue
		}
		for _, replica := range group.Replicas {
			if replica.ReplicaID == id {
				return replica.NodeName
			}
		}
	}
	return ""
}

func (g *Group) cacheProgressObservation(status topology.GroupStatus) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastLoadStatus == nil {
		g.lastLoadStatus = make(map[string]topology.GroupStatus)
	}
	g.lastLoadStatus[status.GroupID] = status.Clone()
}
