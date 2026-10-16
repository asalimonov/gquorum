package metaservice

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// localReplicaMetrics is intentionally served by voters and stable learners.
// The caller and the reply are pinned to exact process and replica identities.
// It never forwards to a leader or drives a collection round.
func (g *Group) localReplicaMetrics(ctx context.Context, header *metapb.Header) *metapb.Reply {
	if header == nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	peer := topology.Identity{
		ClusterID:  header.ClusterId,
		NodeName:   header.NodeName,
		MemberID:   header.MemberId,
		InstanceID: header.InstanceId,
	}
	if err := g.ValidateLoadNode(ctx, peer); err != nil {
		return metatransport.Reply(err)
	}
	engine := g.engine.Load()
	if engine == nil || !engine.Running() {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	identity := g.currentIdentity()
	meta := metricsConvergedGroup(g.metricsTopology().Groups, topology.MetaGroupID)
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

// Metrics returns an owned local and collected metrics snapshot.
func (g *Group) Metrics() metrics.ClusterSnapshot {
	if g == nil || g.collector == nil {
		return (metrics.ClusterSnapshot{}).Clone()
	}
	return g.collector.Snapshot()
}

// MetricsHistory reads a bounded page of retained replica observation history.
func (g *Group) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	if g == nil || g.collector == nil {
		return metrics.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	return g.collector.History(q)
}
func (g *Group) localMetrics() []metrics.ReplicaObservation {
	out := []metrics.ReplicaObservation{}
	if sample, found := g.localMetaMetrics(); found {
		out = append(out, sample)
	}
	if g.opts.LocalLoadMetrics != nil {
		out = append(out, g.opts.LocalLoadMetrics()...)
	}
	return out
}

func (g *Group) localMetaMetrics() (metrics.ReplicaObservation, bool) {
	engine := g.engine.Load()
	if engine == nil || !engine.Running() {
		return metrics.ReplicaObservation{}, false
	}
	id := g.currentIdentity()
	group := metricsConvergedGroup(g.metricsTopology().Groups, topology.MetaGroupID)
	if group == nil {
		return metrics.ReplicaObservation{}, false
	}
	for _, r := range group.Replicas {
		if r.NodeName != id.NodeName || r.ReplicaID != id.MemberID {
			continue
		}
		sample := engine.Metrics()
		select {
		case <-g.ready:
		default:
			sample.Sample.Ready = false
		}
		return sample, true
	}
	return metrics.ReplicaObservation{}, false
}

type metricsSource struct {
	g      *Group
	cursor int
	// Last authenticated process pins survive a lease expiry for read-only
	// probes. A new authoritative process pin replaces them immediately.
	pins map[topology.ReplicaKey]topology.Identity
}

func (s *metricsSource) Topology(ctx context.Context) (topology.ProgressPage, error) {
	if err := s.g.ensureRunning(ctx); err != nil {
		return topology.ProgressPage{}, err
	}
	return s.g.metricsTopology(), nil
}
func (s *metricsSource) Epoch() (observe.LeaderEpoch, bool) {
	engine := s.g.engine.Load()
	if engine == nil {
		return observe.LeaderEpoch{}, false
	}
	status := engine.Status()
	own := s.g.currentIdentity()
	running := engine.Running()
	return observe.LeaderEpoch{
		InstanceID: own.InstanceID,
		Term:       status.Term,
		LeaderID:   status.LeaderID,
	}, running && status.LeaderID == own.MemberID && status.Term != 0 && s.g.IsLeader()
}

func (s *metricsSource) Collect(ctx context.Context, doc topology.ProgressPage,
	pool *observe.ReadPool) (observe.Round, error) {
	epoch, _ := s.Epoch()
	round := observe.Round{Epoch: epoch, Groups: map[string]observe.GroupSamples{}}
	for _, row := range doc.Groups {
		if row.Converged != nil && row.ConvergedVersion != 0 {
			round.Groups[row.GroupID] = observe.GroupSamples{SourceVersion: row.ConvergedVersion}
		}
	}
	reads := []observe.ReplicaRead{}
	kept := map[topology.ReplicaKey]bool{}
	if group := metricsConvergedGroup(doc.Groups, topology.MetaGroupID); group != nil {
		for _, r := range group.Replicas {
			key := topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
			kept[key] = true
			reads = append(reads, s.metaMetricsRead(r))
		}
	}
	for key := range s.pins {
		if !kept[key] {
			delete(s.pins, key)
		}
	}
	if s.g.opts.LoadMetricsReads != nil {
		reads = append(reads, s.g.opts.LoadMetricsReads(ctx, doc.Groups)...)
	}
	options := observe.ReadOptions{Start: s.cursor, Parallel: s.g.opts.Metrics.MaxParallelReads,
		MaxAge: s.g.opts.Metrics.MaxSampleAge}
	probes, next := observe.CollectReads(ctx, pool, reads, options)
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

func (s *metricsSource) metricsPin(r topology.Replica) (topology.Identity, bool) {
	key := topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
	own := s.g.currentIdentity()
	pin, found := s.g.state().ProcessIdentity(r.NodeName)
	if r.NodeName == own.NodeName {
		pin, found = own, true
	}
	if pin.MemberID != r.ReplicaID || pin.ClusterID != own.ClusterID {
		delete(s.pins, key)
		return pin, false
	}
	if found {
		s.pins[key] = pin
	} else {
		pin, found = s.pins[key]
	}
	return pin, found
}

func (s *metricsSource) metaMetricsRead(r topology.Replica) observe.ReplicaRead {
	pin, found := s.metricsPin(r)
	id := metrics.ReplicaIdentity{
		ClusterID: s.g.currentIdentity().ClusterID, GroupID: topology.MetaGroupID,
		NodeName: r.NodeName, ReplicaID: r.ReplicaID,
	}
	if !found {
		return observe.ReplicaRead{Identity: id, Error: fault.ErrReplicaNotReady}
	}
	id.InstanceID = pin.InstanceID
	return observe.ReplicaRead{Identity: id,
		Read: func(ctx context.Context, attempt *observe.ReadAttempt) (observe.Probe, error) {
			return s.readMetaMetrics(ctx, attempt, pin, id)
		},
	}
}

func (s *metricsSource) readMetaMetrics(ctx context.Context, attempt *observe.ReadAttempt,
	pin topology.Identity, id metrics.ReplicaIdentity) (observe.Probe, error) {
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
		var err error
		observation, err = s.remoteMetaMetrics(ctx, attempt, pin, id)
		if err != nil {
			return observe.Probe{}, err
		}
	}
	got := observation.Sample.Identity
	if got.ClusterID != id.ClusterID || got.GroupID != id.GroupID || got.NodeName != id.NodeName ||
		got.ReplicaID != id.ReplicaID || got.InstanceID != id.InstanceID || got.RunID == "" {
		return observe.Probe{}, fault.ErrIdentityMismatch
	}
	return observe.Probe{Reachability: "alive", Observation: observation}, nil
}

func (s *metricsSource) remoteMetaMetrics(ctx context.Context, attempt *observe.ReadAttempt,
	pin topology.Identity, id metrics.ReplicaIdentity) (metrics.ReplicaObservation, error) {
	address, err := s.g.resolveMember(ctx, id.ReplicaID)
	if err != nil {
		return metrics.ReplicaObservation{}, err
	}
	// A newly known process must never inherit this old process's probe.
	current, known := s.g.state().ProcessIdentity(id.NodeName)
	if known && current != pin {
		return metrics.ReplicaObservation{}, fault.ErrIdentityMismatch
	}
	attempt.Dispatch()
	reply, err := s.g.transport.CallAddress(ctx, address, "MetaReplicaMetrics", nil)
	if err != nil {
		return metrics.ReplicaObservation{}, err
	}
	if err := metatransport.ReplyError(reply); err != nil {
		return metrics.ReplicaObservation{}, err
	}
	var sample metrics.ReplicaSample
	if err := json.Unmarshal(reply.Data, &sample); err != nil {
		return metrics.ReplicaObservation{}, fault.New(fault.Unreadable, "invalid meta metrics", err)
	}
	return metrics.ReplicaObservation{State: metrics.Current, Sample: sample, Reachability: "alive"}, nil
}
