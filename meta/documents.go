package meta

import (
	"context"
	"errors"
	"time"

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
func (g *Group) Status(ctx context.Context) (topology.ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ClusterStatus{}, err
	}
	machine := g.state()
	latest, target, active := machine.Views()
	g.mu.RLock()
	lifecycle := g.lifecycle
	g.mu.RUnlock()
	result := topology.ClusterStatus{Lifecycle: lifecycle, ErrorCode: g.terminalCode(), LatestVersion: latest.Version, TargetVersion: target.Version, ActiveVersion: active.Version}
	g.sessionMu.Lock()
	result.SessionLive = g.ownSession != "" && time.Until(g.ownDeadline) > 0
	g.sessionMu.Unlock()
	for _, node := range latest.Nodes {
		result.Nodes = append(result.Nodes, topology.NodeStatus{Node: node, EffectiveState: topology.EffectiveNodeState(latest, node.Name), Health: topology.HealthUnknown})
	}
	if engine := g.engine.Load(); engine != nil {
		status := engine.Status()
		result.Groups = append(result.Groups, status)
		result.MetaLeader = status.LeaderID
	}
	if execution, ok := machine.CurrentExecution(); ok {
		status := execution.Status
		result.Execution = &status
	}
	return result, nil
}
