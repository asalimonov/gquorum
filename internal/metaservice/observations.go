package metaservice

import (
	"context"
	"maps"
	"reflect"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// HealthObservation returns owned observer-local data without sampling or
// proposing. Its timestamp/version identify the observation's age and scope.
func (g *Group) HealthObservation() topology.HealthObservation {
	if g == nil || g.observer == nil {
		return topology.HealthObservation{}
	}
	return g.observer.Snapshot()
}

type observationSource struct{ g *Group }

func (s observationSource) Target(ctx context.Context) (topology.Document, error) {
	return s.g.Target(ctx)
}

func (s observationSource) Health(ctx context.Context, target topology.Document) (map[string]topology.Health, error) {
	if target.Version == 0 {
		return nil, nil
	}
	if s.g.opts.Health != nil {
		health, err := s.g.opts.Health.Snapshot(ctx)
		return maps.Clone(health), err
	}
	core := s.g.engine.Load()
	if core == nil {
		return nil, fault.ErrUnavailable
	}
	live, err := s.g.metaHealth(ctx, core.Status())
	if err != nil {
		return nil, err
	}
	health := make(map[string]topology.Health, len(target.Nodes))
	for _, n := range target.Nodes {
		health[n.Name] = topology.HealthUnknown
		id := target.MemberID(n.Name)
		if id != 0 {
			health[n.Name] = topology.HealthDead
			if live[id] {
				health[n.Name] = topology.HealthAlive
			}
		}
	}
	return health, nil
}
func (g *Group) observerEvent(typ events.Type) events.Event {
	id := g.currentIdentity()
	return events.Event{
		Type:       typ,
		NodeName:   id.NodeName,
		InstanceID: id.InstanceID,
		ClusterID:  id.ClusterID,
		MemberID:   id.MemberID,
	}
}

func (g *Group) recordGroupHealth(old topology.GroupHealth, next topology.GroupHealthEvaluation) {
	if g.opts.Events == nil {
		return
	}
	e := g.observerEvent(events.GroupHealthChanged)
	observation := g.HealthObservation()
	e.Version, e.At = observation.Version, observation.At
	e.GroupID = next.GroupID
	e.State = string(next.ObservedHealth.State)
	e.HealthyVoters = next.ObservedHealth.HealthyVoters
	e.Voters = next.Voters
	g.opts.Events.Publish(e)
	switch {
	case next.ObservedHealth.State == topology.GroupQuorumLost && old != topology.GroupQuorumLost:
		e.Type = events.QuorumLost
	case old == topology.GroupQuorumLost && (next.ObservedHealth.State == topology.GroupHealthy ||
		next.ObservedHealth.State == topology.GroupDegraded):
		e.Type = events.QuorumAchieved
	default:
		return
	}
	g.opts.Events.Publish(e)
}

func (g *Group) recordNodeHealth(name string, _ topology.Health, next topology.Health) {
	if g.opts.Events == nil {
		return
	}
	typ := events.NodeAlive
	if next == topology.HealthDead {
		typ = events.NodeDead
	} else if next != topology.HealthAlive {
		return
	}
	e := g.observerEvent(typ)
	observation := g.HealthObservation()
	e.Version, e.At = observation.Version, observation.At
	e.SubjectNode = name
	e.State = string(next)
	g.opts.Events.Publish(e)
}

// recordScopedApplied publishes drain completion from durable per-node
// progress and operation changes from the exact retained operation record.
func (g *Group) recordScopedApplied(machine *metaengine.Machine) {
	g.recorderMu.Lock()
	defer g.recorderMu.Unlock()
	g.recordOperationChanges(machine)
	status, err := machine.ProgressStatus()
	if err != nil {
		return
	}
	drained := map[string]bool{}
	for _, n := range status.Nodes {
		if n.EffectiveState != topology.NodeDrained {
			continue
		}
		drained[n.Node.Name] = true
		if !g.lastDrained[n.Node.Name] && g.opts.Events != nil {
			e := g.observerEvent(events.DrainCompleted)
			e.SubjectNode = n.Node.Name
			e.Version = status.TargetVersion
			e.GroupID = topology.MetaGroupID
			g.opts.Events.Publish(e)
		}
	}
	g.lastDrained = drained
}

// The recorder follows every retained block, including work from older approvals;
// a newer disjoint operation cannot hide an earlier block's completion.
// The caller holds recorderMu and the engine invokes it after durable Apply.
func (g *Group) recordOperationChanges(machine *metaengine.Machine) {
	operations, err := machine.OperationStatuses()
	if err != nil {
		return
	}
	current := make(map[topology.OperationID]map[string]topology.BlockStatus, len(operations))
	for _, op := range operations {
		blocks := make(map[string]topology.BlockStatus, len(op.Blocks))
		current[op.ID] = blocks
		g.recordChangedBlocks(op, blocks)
	}
	g.lastOperations = current // Pruned operations release their local dedup state.
}

func (g *Group) recordChangedBlocks(op topology.OperationStatus, blocks map[string]topology.BlockStatus) {
	for _, block := range op.Blocks {
		blocks[block.GroupID] = block
		previous, found := g.lastOperations[op.ID][block.GroupID]
		if !found || !reflect.DeepEqual(previous, block) {
			g.recordOperationBlock(op, block)
		}
	}
}

func (g *Group) recordOperationBlock(operation topology.OperationStatus, block topology.BlockStatus) {
	if g.opts.Events == nil {
		return
	}
	e := g.observerEvent(events.OperatorChanged)
	e.OperationID, e.Version, e.Attempt = string(operation.ID), operation.Version, operation.Attempt
	e.GroupID, e.StepSeq, e.State, e.Code = block.GroupID, block.CurrentStep, string(block.State), block.ErrorCode
	if reason := block.Blocked; reason != nil {
		e.SubjectNode, e.ReplicaID = reason.NodeName, reason.ReplicaID
		if e.Code == "" {
			e.Code = reason.Code
		}
	}
	g.opts.Events.Publish(e)
}
