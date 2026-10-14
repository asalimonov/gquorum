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

func (s observationSource) Active(ctx context.Context) (topology.Document, error) {
	return s.g.Active(ctx)
}

func (s observationSource) Health(ctx context.Context, active topology.Document) (map[string]topology.Health, error) {
	if active.Version == 0 {
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
	health := make(map[string]topology.Health, len(active.Nodes))
	for _, n := range active.Nodes {
		health[n.Name] = topology.HealthUnknown
		id := active.MemberID(n.Name)
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

func (g *Group) recordApplied(machine *metaengine.Machine, active topology.Document) {
	g.recorderMu.Lock()
	defer g.recorderMu.Unlock()
	g.recordOperatorApplied(machine)
	drained := map[string]bool{}
	for _, n := range active.Nodes {
		if topology.EffectiveNodeState(active, n.Name) != topology.NodeDrained {
			continue
		}
		drained[n.Name] = true
		if !g.lastDrained[n.Name] && g.opts.Events != nil {
			e := g.observerEvent(events.DrainCompleted)
			e.SubjectNode = n.Name
			e.Version = active.Version
			e.GroupID = topology.MetaGroupID
			g.opts.Events.Publish(e)
		}
	}
	g.lastDrained = drained
}

// recordOperatorApplied shares recordApplied's recorder lock.
func (g *Group) recordOperatorApplied(machine *metaengine.Machine) {
	if ex, exists := machine.CurrentOperator(); exists {
		next := ex.Status
		if g.lastOperator == nil || !reflect.DeepEqual(*g.lastOperator, next) {
			g.lastOperator = &next
			if g.opts.Events != nil {
				e := g.observerEvent(events.OperatorChanged)
				e.OperatorID = string(next.ID)
				e.Version = next.Version
				e.Attempt = next.Attempt
				e.StepSeq = next.CurrentStep
				e.State = string(next.State)
				e.Code = next.ErrorCode
				e.GroupID = next.GroupID
				e.SubjectNode = next.NodeName
				if next.CurrentStep > 0 && next.CurrentStep <= len(ex.Plan.Steps) {
					e.ReplicaID = ex.Plan.Steps[next.CurrentStep-1].ReplicaID
				}
				g.opts.Events.Publish(e)
			}
		}
	}
}
