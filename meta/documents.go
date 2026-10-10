package meta

import (
	"context"
	"errors"
	"maps"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

type View string

const (
	Latest View = "latest"
	Target View = "target"
	Active View = "active"
)

type Page struct{ Offset, Limit int }
type HistoryResult struct {
	Documents     []topology.Document
	OldestVersion uint64
}
type Update struct {
	View     View
	Document topology.Document
}

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
func (g *Group) History(ctx context.Context, page Page) (HistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return HistoryResult{}, err
	}
	if page.Offset < 0 || page.Limit < 0 {
		return HistoryResult{}, fault.ErrInvalid
	}
	machine := g.state()
	all := machine.History(0, 0)
	result := HistoryResult{}
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
func (g *Group) Execution(ctx context.Context, id topology.ExecutionID) (topology.ExecutionStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ExecutionStatus{}, err
	}
	status, ok := g.state().Execution(id)
	if !ok {
		return topology.ExecutionStatus{}, fault.New(fault.HistoryUnavailable, "execution is not retained", nil)
	}
	return status, nil
}

// Status combines owned local document/runtime state with caller load-group
// status queries. The caller controller must honor ctx. Health is sampled by
// the observer; a mismatched Active version is reported as unsampled unknown
// with a zero observation timestamp until a current sample becomes available.
func (g *Group) Status(ctx context.Context) (topology.ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ClusterStatus{}, err
	}
	machine := g.state()
	latest, target, active := machine.Views()
	g.mu.RLock()
	lifecycle := g.lifecycle
	addresses := maps.Clone(g.addresses)
	var removal *topology.SelfRemovalStatus
	if g.selfRemoval != nil {
		owned := *g.selfRemoval
		removal = &owned
	}
	g.mu.RUnlock()
	result := topology.ClusterStatus{Lifecycle: lifecycle, ErrorCode: g.terminalCode(), LatestVersion: latest.Version, TargetVersion: target.Version, ActiveVersion: active.Version, SelfRemoval: removal}
	result.SessionLive = g.OwnSessionLive()
	if g.reconciler != nil {
		result.Reconciliation = g.reconciler.Status()
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
		status := engine.Status()
		result.Groups = append(result.Groups, status)
		result.MetaLeader = status.LeaderID
	} else if active.MetaGroup() != nil {
		result.Groups = append(result.Groups, topology.GroupStatus{GroupID: topology.MetaGroupID, ErrorCode: string(fault.Unavailable)})
	}
	for _, group := range active.LoadGroups() {
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
		result.Groups = append(result.Groups, observed.Clone())
	}
	if execution, ok := machine.CurrentExecution(); ok {
		status := execution.Status
		result.Execution = &status
	}
	return result, nil
}
