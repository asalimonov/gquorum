package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/events"
	internalscheduling "github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/metrics"
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
	return g.scheduler.SetEnabled(ctx, internalscheduling.Enablement{Enabled: enabled})
}

func (g *Group) recordScheduling(status topology.SchedulingStatus) {
	if g.opts.Events == nil {
		return
	}
	id := g.currentIdentity()
	g.opts.Events.Publish(events.Event{
		At:         status.Since,
		Type:       events.SchedulingChanged,
		NodeName:   id.NodeName,
		InstanceID: id.InstanceID,
		MemberID:   id.MemberID,
		ClusterID:  id.ClusterID,
		GroupID:    topology.MetaGroupID,
		Code:       status.Reason,
		State:      string(status.State),
	})
}

type schedulingHost struct{ g *Group }

var _ internalscheduling.Host = schedulingHost{}

func (h schedulingHost) IsLeader() bool { return h.g.IsLeader() }

func (h schedulingHost) WakeOperations() { h.g.controller.Signal() }

func (h schedulingHost) Observations() metrics.ClusterSnapshot { return h.g.Metrics() }
func (h schedulingHost) VoterSwitches() []topology.VoterSwitchState {
	return h.g.state().VoterSwitches()
}

func (h schedulingHost) RefreshObservations(ctx context.Context) error {
	return h.g.collector.Revalidate(ctx)
}

func (h schedulingHost) ResetObservations() {
	if h.g.collector != nil {
		h.g.collector.ResetDecisions()
	}
}
