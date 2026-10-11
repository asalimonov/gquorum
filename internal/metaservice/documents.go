package metaservice

import (
	"context"
	"errors"
	"maps"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Document reads return owned locally applied state. WaitReady is the startup
// gate; reads do not promise a linearizable view of an unreachable leader.
func (g *Group) Latest(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Latest(), nil
}
func (g *Group) Target(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Target(), nil
}
func (g *Group) Active(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Active(), nil
}
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
func (g *Group) Stage(ctx context.Context, doc topology.Document, expected uint64) (topology.Document, error) {
	result, err := g.propose(ctx, metaengine.ProposeDocument{Doc: doc.Clone(), ExpectedVersion: expected})
	if errors.Is(err, fault.ErrVersionConflict) {
		return topology.Document{}, &fault.Error{Code: fault.VersionConflict, Message: result.Reason, ExpectedVersion: expected, CurrentVersion: result.CurrentVersion}
	}
	return result.Doc, err
}
func (g *Group) Authorize(ctx context.Context, a topology.AuthorizedStep) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	return g.state().Authorize(ctx, a)
}
func (g *Group) Operator(ctx context.Context, id topology.OperatorID) (topology.OperatorStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.OperatorStatus{}, err
	}
	status, ok := g.state().Operator(id)
	if !ok {
		return topology.OperatorStatus{}, fault.New(fault.HistoryUnavailable, "execution is not retained", nil)
	}
	return status, nil
}

// LocalStatus returns owned, locally observed meta Raft state. It does not probe
// peers or invoke caller load controllers. Found is false before engine startup
// and after shutdown; an observation is not a linearizable read or quorum proof.
func (g *Group) LocalStatus() topology.GroupStatus {
	if engine := g.engine.Load(); engine != nil {
		return withLeaderNodeName(engine.Status(), g.state().Active())
	}
	return topology.GroupStatus{GroupID: topology.MetaGroupID, HostNode: g.Identity().NodeName, ErrorCode: string(fault.Unavailable)}
}

// Status combines owned local document/runtime state with caller load-group
// status queries. The caller controller must honor ctx. Health is sampled by
// the observer; a mismatched Active version is reported as unsampled unknown
// with a zero observation timestamp until a current sample becomes available.
func (g *Group) Status(ctx context.Context) (topology.ClusterStatus, error) {
	return g.status(ctx, true)
}

// CachedStatus remains available after fencing without starting peer probes.
func (g *Group) CachedStatus(ctx context.Context) (topology.ClusterStatus, error) {
	return g.status(ctx, false)
}

func (g *Group) status(ctx context.Context, live bool) (topology.ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ClusterStatus{}, err
	}
	machine := g.state()
	latest, target, active := machine.Views()
	g.mu.RLock()
	lifecycle := g.lifecycle
	addresses := maps.Clone(g.addresses)
	live = live && !g.quiescing && g.selfRemoval == nil
	cached := maps.Clone(g.lastLoadStatus)
	var removal *topology.SelfRemovalStatus
	if g.selfRemoval != nil {
		owned := *g.selfRemoval
		removal = &owned
	}
	g.mu.RUnlock()
	result := topology.ClusterStatus{Lifecycle: lifecycle, ErrorCode: g.terminalCode(), LatestVersion: latest.Version, TargetVersion: target.Version, ActiveVersion: active.Version, SelfRemoval: removal}
	result.SessionLive = g.OwnSessionLive()
	if g.scheduler != nil {
		result.Scheduling = g.scheduler.Status()
	}
	result.Observation = g.HealthObservation()
	if result.Observation.Version != active.Version {
		result.Observation = topology.HealthObservation{Version: active.Version, Groups: topology.EvaluateAllGroupHealth(active, nil)}
	}
	for _, node := range latest.Nodes {
		effective := topology.EffectiveNodeState(active, node.Name)
		if effective == "" {
			effective = node.State
		}
		health := topology.HealthUnknown
		if result.Observation.Version == active.Version {
			if observed, exists := result.Observation.Nodes[node.Name]; exists {
				health = observed
			}
		}
		result.Nodes = append(result.Nodes, topology.NodeStatus{Node: node, EffectiveState: effective, Health: health, Address: addresses[node.Name]})
	}
	if engine := g.engine.Load(); engine != nil {
		status := withLeaderNodeName(engine.Status(), active)
		result.Groups = append(result.Groups, status)
		result.MetaLeaderID = status.LeaderID
		result.MetaLeaderNodeName = status.LeaderNodeName
	} else if active.MetaGroup() != nil {
		result.Groups = append(result.Groups, topology.GroupStatus{GroupID: topology.MetaGroupID, ErrorCode: string(fault.Unavailable)})
	}
	for _, group := range active.LoadGroups() {
		if !live {
			observed, found := cached[group.ID]
			if !found {
				observed = topology.GroupStatus{GroupID: group.ID, ErrorCode: string(fault.Unavailable)}
			}
			result.Groups = append(result.Groups, withLeaderNodeName(observed.Clone(), active))
			continue
		}
		observed := topology.GroupStatus{GroupID: group.ID}
		var err error
		if g.opts.LoadGroups == nil {
			err = fault.ErrNoController
		} else {
			observed, err = g.opts.LoadGroups.Status(ctx, group.ID)
		}
		if err == nil && observed.GroupID != group.ID {
			err = fault.ErrIdentityMismatch
		}
		if err != nil {
			code := fault.Unavailable
			var typed *fault.Error
			if errors.As(err, &typed) {
				code = typed.Code
			}
			observed = topology.GroupStatus{GroupID: group.ID, ErrorCode: string(code)}
		}
		if err := ctx.Err(); err != nil {
			return topology.ClusterStatus{}, err
		}
		result.Groups = append(result.Groups, withLeaderNodeName(observed.Clone(), active))
		g.mu.Lock()
		if g.lastLoadStatus == nil {
			g.lastLoadStatus = make(map[string]topology.GroupStatus)
		}
		g.lastLoadStatus[group.ID] = observed.Clone()
		g.mu.Unlock()
	}
	if operator, ok := machine.CurrentOperator(); ok {
		status := operator.Status
		result.Operator = &status
	}
	return result, nil
}

// Resolve names from the same Active snapshot used by the status response.
// Never trust a caller's name or map a retired ID through a staged replacement.
func withLeaderNodeName(status topology.GroupStatus, active topology.Document) topology.GroupStatus {
	status.LeaderNodeName = ""
	if status.LeaderID != 0 {
		if group := active.Group(status.GroupID); group != nil {
			for _, replica := range group.Replicas {
				if replica.ReplicaID == status.LeaderID {
					status.LeaderNodeName = replica.NodeName
					break
				}
			}
		}
	}
	return status
}
