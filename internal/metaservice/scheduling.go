package metaservice

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

// SchedulingStatus reads local diagnostics without causing an observation
// or a topology mutation. Followers report durable blockers and standby state.
func (g *Group) SchedulingStatus() topology.SchedulingStatus {
	if g == nil || g.scheduler == nil {
		return topology.SchedulingStatus{}
	}
	return g.scheduler.Status()
}

// SetSchedulingEnabled changes this node's local automatic-repair switch.
// It never changes a topology document or cancels already approved execution.
func (g *Group) SetSchedulingEnabled(ctx context.Context, enabled bool) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	return g.scheduler.SetEnabled(ctx, enabled)
}

func (g *Group) recordScheduling(status topology.SchedulingStatus) {
	if g.opts.Events == nil {
		return
	}
	id := g.currentIdentity()
	g.opts.Events.Publish(events.Event{At: status.Since, Type: events.SchedulingChanged, NodeName: id.NodeName, InstanceID: id.InstanceID, MemberID: id.MemberID, ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, Code: status.Reason, State: string(status.State)})
}

type schedulingHost struct{ g *Group }

var _ scheduling.Host = schedulingHost{}

func (h schedulingHost) Views(ctx context.Context) (topology.Document, topology.Document, topology.Document, error) {
	if err := h.g.ensureRunning(ctx); err != nil {
		return topology.Document{}, topology.Document{}, topology.Document{}, err
	}
	latest, target, active := h.g.state().Views()
	return latest, target, active, nil
}
func (h schedulingHost) CurrentOperator(ctx context.Context) (topology.Operator, bool, error) {
	if err := ctx.Err(); err != nil {
		return topology.Operator{}, false, err
	}
	ex, exists := h.g.state().CurrentOperator()
	return ex, exists, nil
}
func (h schedulingHost) IsLeader() bool { return h.g.IsLeader() }
func (h schedulingHost) Health(ctx context.Context) (map[string]topology.Health, error) {
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
func (h schedulingHost) GlobalStatus(ctx context.Context, active topology.Document) (map[string]topology.GroupStatus, error) {
	return h.g.observeGroups(ctx, active, active)
}
func (h schedulingHost) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	return h.g.PlanFor(ctx, doc)
}
func (h schedulingHost) Approve(ctx context.Context, version uint64) (topology.OperatorID, error) {
	return h.g.Apply(ctx, version)
}
func (h schedulingHost) CommitRepair(ctx context.Context, doc topology.Document, expected uint64, plan topology.Plan) (topology.OperatorID, error) {
	if !h.g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	result, err := h.g.propose(ctx, metaengine.CommitRepairOperatorProposal{Doc: doc, ExpectedVersion: expected, Plan: plan, At: time.Now().UTC()})
	return result.OperatorID, err
}
func (h schedulingHost) WakeOperator() { h.g.controller.Signal() }
