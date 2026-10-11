package meta

import (
	"context"
	"maps"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

// ReconciliationStatus reads local diagnostics without causing an observation
// or a topology mutation. Followers report durable blockers and standby state.
func (g *Group) ReconciliationStatus() topology.ReconciliationStatus {
	if g == nil || g.reconciler == nil {
		return topology.ReconciliationStatus{}
	}
	return g.reconciler.Status()
}

// SetReconciliationEnabled changes this node's local automatic-repair switch.
// It never changes a topology document or cancels already approved execution.
func (g *Group) SetReconciliationEnabled(ctx context.Context, enabled bool) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	return g.reconciler.SetEnabled(ctx, enabled)
}

func (g *Group) recordReconciliation(status topology.ReconciliationStatus) {
	if g.opts.Events == nil {
		return
	}
	id := g.currentIdentity()
	g.opts.Events.Publish(events.Event{At: status.Since, Type: events.ReconciliationChanged, NodeName: id.NodeName, InstanceID: id.InstanceID, MemberID: id.MemberID, ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, Code: status.Reason, State: string(status.State)})
}

type reconciliationHost struct{ g *Group }

var _ scheduling.Host = reconciliationHost{}

func (h reconciliationHost) Views(ctx context.Context) (topology.Document, topology.Document, topology.Document, error) {
	if err := h.g.ensureRunning(ctx); err != nil {
		return topology.Document{}, topology.Document{}, topology.Document{}, err
	}
	latest, target, active := h.g.state().Views()
	return latest, target, active, nil
}
func (h reconciliationHost) CurrentExecution(ctx context.Context) (topology.ExecutionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return topology.ExecutionRecord{}, false, err
	}
	ex, exists := h.g.state().CurrentExecution()
	return ex, exists, nil
}
func (h reconciliationHost) IsLeader() bool { return h.g.IsLeader() }
func (h reconciliationHost) Health(ctx context.Context) (map[string]topology.Health, error) {
	if h.g.opts.Health != nil {
		observed, err := h.g.opts.Health.Snapshot(ctx)
		return maps.Clone(observed), err
	}
	core := h.g.engine.Load()
	if core == nil {
		return nil, fault.ErrUnavailable
	}
	status := core.Status()
	live, err := h.g.metaHealth(ctx, status)
	if err != nil {
		return nil, err
	}
	active := h.g.state().Active()
	if active.Version == 0 {
		return nil, nil
	}
	health := make(map[string]topology.Health, len(active.Nodes))
	for _, node := range active.Nodes {
		health[node.Name] = topology.HealthDead
		if live[active.MemberID(node.Name)] {
			health[node.Name] = topology.HealthAlive
		}
	}
	return health, nil
}
func (h reconciliationHost) GlobalStatus(ctx context.Context, active topology.Document) (map[string]topology.GroupStatus, error) {
	return h.g.observeGroups(ctx, active, active)
}
func (h reconciliationHost) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	return h.g.PlanFor(ctx, doc)
}
func (h reconciliationHost) Approve(ctx context.Context, version uint64) (topology.ExecutionID, error) {
	return h.g.Apply(ctx, version)
}
func (h reconciliationHost) CommitRepair(ctx context.Context, doc topology.Document, expected uint64, plan topology.Plan) (topology.ExecutionID, error) {
	if !h.g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	result, err := h.g.propose(ctx, metaengine.CommitRepairExecutionProposal{Doc: doc, ExpectedVersion: expected, Plan: plan, At: time.Now().UTC()})
	return result.ExecutionID, err
}
func (h reconciliationHost) WakeConvergence() { h.g.convergence.Signal() }
