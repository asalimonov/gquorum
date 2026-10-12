package metaservice

import (
	"context"
	"encoding/json"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
	"slices"
)

// localReplicaMetrics is intentionally served by voters and stable learners.
// The caller and the reply are pinned to exact process and replica identities.
// It never forwards to a leader or drives a collection round.
func (g *Group) localReplicaMetrics(ctx context.Context, header *metapb.Header) *metapb.Reply {
	if header == nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	peer := topology.Identity{ClusterID: header.ClusterId, NodeName: header.NodeName, MemberID: header.MemberId, InstanceID: header.InstanceId}
	if err := g.ValidateLoadNode(ctx, peer); err != nil {
		return metatransport.Reply(err)
	}
	engine := g.engine.Load()
	if engine == nil || !engine.Running() {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	identity := g.currentIdentity()
	active := g.state().Active()
	meta := active.MetaGroup()
	if meta == nil {
		return metatransport.Reply(fault.ErrReplicaNotReady)
	}
	assigned := false
	for _, r := range meta.Replicas {
		if r.NodeName == identity.NodeName && r.ReplicaID == identity.MemberID {
			assigned = true
			break
		}
	}
	cs := engine.Status().ConfState
	if !assigned || !slices.Contains(cs.Voters, identity.MemberID) && !slices.Contains(cs.Learners, identity.MemberID) {
		return metatransport.Reply(fault.ErrReplicaNotReady)
	}
	sample := engine.Metrics().Sample
	select {
	case <-g.ready:
	default:
		sample.Ready = false
	}
	encoded, err := json.Marshal(sample)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) Metrics() metrics.ClusterSnapshot {
	if g == nil || g.collector == nil {
		return (metrics.ClusterSnapshot{}).Clone()
	}
	return g.collector.Snapshot()
}
func (g *Group) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	if g == nil || g.collector == nil {
		return metrics.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	return g.collector.History(q)
}
func (g *Group) localMetrics() []metrics.ReplicaObservation {
	out := []metrics.ReplicaObservation{}
	engine := g.engine.Load()
	if engine != nil && engine.Running() {
		id := g.currentIdentity()
		doc := g.state().Active()
		if group := doc.MetaGroup(); group != nil {
			for _, r := range group.Replicas {
				if r.NodeName == id.NodeName && r.ReplicaID == id.MemberID {
					sample := engine.Metrics()
					select {
					case <-g.ready:
					default:
						sample.Sample.Ready = false
					}
					out = append(out, sample)
					break
				}
			}
		}
	}
	if g.opts.LocalLoadMetrics != nil {
		out = append(out, g.opts.LocalLoadMetrics()...)
	}
	return out
}

type metricsSource struct {
	g      *Group
	cursor int
	// Last authenticated process pins survive a lease expiry for read-only
	// probes. A new authoritative process pin replaces them immediately.
	pins map[topology.ReplicaKey]topology.Identity
}

func (s *metricsSource) Active(ctx context.Context) (topology.Document, error) {
	return s.g.Active(ctx)
}
func (s *metricsSource) Epoch() (observe.LeaderEpoch, bool) {
	engine := s.g.engine.Load()
	if engine == nil {
		return observe.LeaderEpoch{}, false
	}
	status := engine.Status()
	own := s.g.currentIdentity()
	return observe.LeaderEpoch{InstanceID: own.InstanceID, Term: status.Term, LeaderID: status.LeaderID}, engine.Running() && status.LeaderID == own.MemberID && status.Term != 0 && s.g.IsLeader()
}
func (s *metricsSource) Collect(ctx context.Context, doc topology.Document, pool *observe.ReadPool) (observe.Round, error) {
	epoch, _ := s.Epoch()
	round := observe.Round{ActiveVersion: doc.Version, Epoch: epoch, Groups: map[string]observe.GroupSamples{}}
	reads := []observe.ReplicaRead{}
	kept := map[topology.ReplicaKey]bool{}
	if group := doc.MetaGroup(); group != nil {
		for _, r := range group.Replicas {
			key := topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
			kept[key] = true
			own := s.g.currentIdentity()
			pin, found := s.g.state().ProcessIdentity(r.NodeName)
			if r.NodeName == own.NodeName {
				pin, found = own, true
			}
			if pin.MemberID != r.ReplicaID || pin.ClusterID != doc.ClusterID {
				delete(s.pins, key)
				found = false
			} else if found {
				s.pins[key] = pin
			} else {
				pin, found = s.pins[key]
			}
			id := metrics.ReplicaIdentity{ClusterID: doc.ClusterID, GroupID: topology.MetaGroupID, NodeName: r.NodeName, ReplicaID: r.ReplicaID}
			read := observe.ReplicaRead{Identity: id, Error: fault.ErrReplicaNotReady}
			if found {
				id.InstanceID = pin.InstanceID
				read.Identity = id
				read.Error = nil
				read.Read = func(ctx context.Context, attempt *observe.ReadAttempt) (observe.Probe, error) {
					var observation metrics.ReplicaObservation
					if pin == s.g.currentIdentity() {
						attempt.Dispatch()
						for _, local := range s.g.localMetrics() {
							if local.Sample.Identity.GroupID == topology.MetaGroupID {
								observation = local
								break
							}
						}
					} else {
						address, err := s.g.resolveMember(ctx, r.ReplicaID)
						if err != nil {
							return observe.Probe{}, err
						}
						// A newly known process must never inherit this old process's probe.
						current, known := s.g.state().ProcessIdentity(r.NodeName)
						if known && current != pin {
							return observe.Probe{}, fault.ErrIdentityMismatch
						}
						attempt.Dispatch()
						reply, err := s.g.transport.CallAddress(ctx, address, "MetaReplicaMetrics", nil)
						if err != nil {
							return observe.Probe{}, err
						}
						if err := metatransport.ReplyError(reply); err != nil {
							return observe.Probe{}, err
						}
						var sample metrics.ReplicaSample
						if err := json.Unmarshal(reply.Data, &sample); err != nil {
							return observe.Probe{}, fault.New(fault.Unreadable, "invalid meta metrics", err)
						}
						observation = metrics.ReplicaObservation{State: metrics.Current, Sample: sample, Reachability: "alive"}
					}
					got := observation.Sample.Identity
					if got.ClusterID != id.ClusterID || got.GroupID != id.GroupID || got.NodeName != id.NodeName || got.ReplicaID != id.ReplicaID || got.InstanceID != id.InstanceID || got.RunID == "" {
						return observe.Probe{}, fault.ErrIdentityMismatch
					}
					return observe.Probe{Reachability: "alive", Observation: observation}, nil
				}
			}
			reads = append(reads, read)
		}
	}
	for key := range s.pins {
		if !kept[key] {
			delete(s.pins, key)
		}
	}
	if s.g.opts.LoadMetricsReads != nil {
		reads = append(reads, s.g.opts.LoadMetricsReads(ctx, doc)...)
	}
	probes, next := observe.CollectReads(ctx, pool, reads, s.cursor, s.g.opts.Metrics.MaxParallelReads, s.g.opts.Metrics.MaxSampleAge)
	s.cursor = next
	for _, probe := range probes {
		id := probe.Identity.GroupID
		group := round.Groups[id]
		group.Probes = append(group.Probes, probe)
		group.Replicas = append(group.Replicas, probe.Observation.Clone())
		round.Groups[id] = group
	}
	return round, nil
}
