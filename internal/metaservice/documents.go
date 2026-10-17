package metaservice

import (
	"context"
	"errors"
	"maps"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Latest returns owned locally applied staged intent. WaitReady is the startup
// gate; reads do not promise a linearizable view of an unreachable leader.
func (g *Group) Latest(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Latest(), nil
}

// Target returns an owned locally applied approved topology document.
func (g *Group) Target(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Target(), nil
}

// Get returns an owned retained topology version or a history-unavailable
// error.
func (g *Group) Get(ctx context.Context, version uint64) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	doc, ok := g.state().Get(version)
	if !ok {
		return topology.Document{}, fault.ErrHistoryUnavailable
	}
	return doc, nil
}

// History returns a bounded descending page and the oldest retained version.
func (g *Group) History(ctx context.Context, page topology.Page) (topology.HistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return topology.HistoryResult{}, err
	}
	if page.Offset < 0 || page.Limit < 0 {
		return topology.HistoryResult{}, fault.ErrInvalid
	}
	machine := g.state()
	all := machine.History(0, 0)
	result := topology.HistoryResult{}
	if len(all) > 0 {
		result.OldestVersion = all[len(all)-1].Version
	}
	if page.Offset < len(all) {
		end := len(all)
		if page.Limit > 0 && page.Limit < end-page.Offset {
			end = page.Offset + page.Limit
		}
		result.Documents = all[page.Offset:end]
	}
	return result, nil
}

// Stage proposes an owned document under replicated expected-version
// compare-and-swap.
func (g *Group) Stage(ctx context.Context, doc topology.Document, expected uint64) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	proposal := metaengine.ProposeDocument{Doc: doc.Clone(), ExpectedVersion: expected}
	if g.state().Latest().Version > expected {
		result, err := g.state().ReplayMutation(proposal)
		return result.Doc, err
	}
	result, err := g.propose(ctx, proposal)
	return result.Doc, err
}

// LocalStatus returns owned, locally observed meta Raft state. It does not
// probe
// peers or invoke caller load controllers. Found is false before engine startup
// and after shutdown; an observation is not a linearizable read or quorum
// proof.
func (g *Group) LocalStatus() topology.GroupStatus {
	if engine := g.engine.Load(); engine != nil {
		status := engine.Status()
		status.LeaderNodeName, _ = g.state().MemberOwner(status.LeaderID)
		return status
	}
	return topology.GroupStatus{GroupID: topology.MetaGroupID, HostNode: g.Identity().NodeName, ErrorCode: string(
		fault.Unavailable)}
}

// Status combines owned local document/runtime state with caller load-group
// status queries. The caller controller must honor ctx. Health is sampled by
// the observer; a mismatched approved version is reported as unsampled unknown
// with a zero observation timestamp until a current sample becomes available.
func (g *Group) Status(ctx context.Context) (topology.ClusterStatus, error) {
	return g.readClusterStatus(ctx, true)
}

// CachedStatus remains available after fencing without starting peer probes.
func (g *Group) CachedStatus(ctx context.Context) (topology.ClusterStatus, error) {
	return g.readClusterStatus(ctx, false)
}

func (g *Group) readClusterStatus(ctx context.Context, live bool) (topology.ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ClusterStatus{}, err
	}
	machine := g.state()
	progress, err := machine.ProgressStatus()
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	local := g.clusterStatusSnapshot(live)
	result := progress
	result.Lifecycle, result.ErrorCode, result.SelfRemoval = local.lifecycle, g.terminalCode(), local.removal
	result.SessionLive = g.OwnSessionLive()
	if g.scheduler != nil {
		result.Scheduling = g.scheduler.Status()
	}
	if observation := g.HealthObservation(); observation.Version == result.TargetVersion {
		result.Observation = observation
	}
	g.decorateClusterStatus(ctx, machine, &result, local)
	return result, nil
}

func (g *Group) decorateClusterStatus(ctx context.Context, machine *metaengine.Machine,
	result *topology.ClusterStatus, local localClusterStatus) {
	for i := range result.Nodes {
		row := &result.Nodes[i]
		row.Address = local.addresses[row.Node.Name]
		if result.Observation.Version == result.TargetVersion {
			if health, ok := result.Observation.Nodes[row.Node.Name]; ok {
				row.Health = health
			}
		}
	}
	if engine := g.engine.Load(); engine != nil {
		status := engine.Status()
		status.LeaderNodeName, _ = machine.MemberOwner(status.LeaderID)
		g.cacheProgressObservation(status)
		result.MetaLeaderID = status.LeaderID
		result.MetaLeaderNodeName = status.LeaderNodeName
	}
	if local.includeLive {
		for _, row := range result.Groups {
			if row.Converged == nil {
				continue
			}
			g.cacheProgressObservation(g.liveLoadStatus(ctx, row.GroupID))
		}
	}
	g.decorateProgress(result.Groups)
}

type localClusterStatus struct {
	lifecycle   string
	addresses   map[string]string
	includeLive bool
	removal     *topology.SelfRemovalStatus
}

func (g *Group) clusterStatusSnapshot(live bool) localClusterStatus {
	g.mu.RLock()
	lifecycle := g.lifecycle
	addresses := maps.Clone(g.addresses)
	includeLive := live && !g.quiescing && g.selfRemoval == nil
	var removal *topology.SelfRemovalStatus
	if g.selfRemoval != nil {
		owned := *g.selfRemoval
		removal = &owned
	}
	g.mu.RUnlock()
	return localClusterStatus{lifecycle: lifecycle, addresses: addresses,
		includeLive: includeLive, removal: removal}
}

func (g *Group) liveLoadStatus(ctx context.Context, id string) topology.GroupStatus {
	observed := topology.GroupStatus{GroupID: id}
	var err error
	if g.opts.LoadGroups == nil {
		err = fault.ErrNoController
	} else {
		observed, err = g.opts.LoadGroups.Status(ctx, id)
	}
	if err == nil && observed.GroupID != id {
		err = fault.ErrIdentityMismatch
	}
	if err != nil {
		code := fault.Unavailable
		var typed *fault.Error
		if errors.As(err, &typed) {
			code = typed.Code
		}
		observed = topology.GroupStatus{GroupID: id, ErrorCode: string(code)}
	}
	return observed
}
