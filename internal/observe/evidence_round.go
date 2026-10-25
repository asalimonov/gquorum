package observe

import (
	"slices"
	"time"

	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

type evidenceRoundContext struct {
	clusterID     string
	observer      metrics.ReplicaSample
	observerValid bool
	records       map[string]topology.VoterSwitchState
	round         Round
}

type evidenceSource struct {
	at                         time.Time
	clusterID, groupID, digest string
	leader, observer           metrics.ReplicaSample
	leaderValid                bool
}

type replicaEvidenceInput struct {
	source  evidenceSource
	replica topology.Replica
	probe   Probe
}

type replicaEvidenceFrame struct {
	maxSampleAge            time.Duration
	input                   replicaEvidenceInput
	scope                   EvidenceScope
	validTarget, replyValid bool
	observed                metrics.ReplicaObservation
	base                    []Generation
	progress                metrics.PeerProgress
	lagKnown, ownFresh      bool
	advanced                bool
	lag, backlog            uint64
}

func (t *evidenceTracker) applyGroup(now time.Time, group topology.Group, input evidenceRoundContext) {
	g := t.groups[group.ID]
	g.current = true
	g.voterConfig = *group.VoterConfig
	g.observeRecords(now, input.records[group.ID])
	samples := input.round.Groups[group.ID]
	leader, leaderValid := t.leader(samples, group, input.clusterID)
	leaderValid = leaderValid && input.observerValid && samples.Error == nil
	g.observation = metrics.GroupObservation{
		GroupID:          group.ID,
		SourceVersion:    g.sourceVersion,
		LeaderID:         leader.LeaderID,
		Term:             leader.Term,
		MembershipDigest: leader.MembershipDigest,
		State:            metrics.Unknown,
		TargetVoters:     g.voterConfig.TargetVoters,
		NativeVoters:     len(leader.Voters),
		Shortfall:        max(0, g.voterConfig.TargetVoters-len(leader.Voters)),
	}
	if leaderValid {
		g.observation.State = metrics.Current
		g.confirmed = now
		g.sourceAge = max(leader.Meter.Age, leader.Raft.Age, input.observer.Meter.Age, input.observer.Raft.Age,
			input.observer.Writer.Age)
	}
	probes := map[topology.ReplicaKey]Probe{}
	for _, probe := range samples.Probes {
		probes[topology.ReplicaKey{NodeName: probe.Identity.NodeName, ReplicaID: probe.Identity.ReplicaID}] = probe
	}
	kept := map[topology.ReplicaKey]bool{}
	digest := voterConfigDigest(g.voterConfig)
	for _, replica := range group.Replicas {
		key := topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		kept[key] = true
		t.applyReplica(g, replicaEvidenceInput{replica: replica, probe: probes[key], source: evidenceSource{
			at: now, clusterID: input.clusterID, groupID: group.ID, leader: leader,
			observer: input.observer, leaderValid: leaderValid, digest: digest}})
	}
	for key := range g.replicas {
		if !kept[key] {
			delete(g.replicas, key)
		}
	}
}

func (t *evidenceTracker) applyReplica(g *groupEvidence, input replicaEvidenceInput) {
	key := topology.ReplicaKey{NodeName: input.replica.NodeName, ReplicaID: input.replica.ReplicaID}
	r := g.replicas[key]
	if r == nil {
		r = &replicaEvidence{}
		g.replicas[key] = r
	}
	frame := t.prepareReplicaScope(g, r, input)
	t.prepareReplicaObservation(r, &frame)
	t.observeReplicaUnavailable(r, &frame)
	t.observeReplicaProgress(g, r, &frame)
	t.observeReplicaStandby(g, r, &frame)
	t.finishReplicaObservation(g, r, &frame)
}

func (t *evidenceTracker) prepareReplicaScope(g *groupEvidence, r *replicaEvidence,
	input replicaEvidenceInput) replicaEvidenceFrame {
	replica, probe := input.replica, input.probe
	leader, digest := input.source.leader, input.source.digest
	validTarget := probe.Identity.ClusterID == input.source.clusterID &&
		probe.Identity.GroupID == input.source.groupID && probe.Identity.NodeName == replica.NodeName &&
		probe.Identity.ReplicaID == replica.ReplicaID && probe.Identity.InstanceID != ""
	replyValid := validTarget && probe.Dispatched && probe.Reachability == reachabilityAlive &&
		exactSample(probe.Identity, probe.Observation.Sample.Identity)
	// A failed exact probe can retain the last known engine run for
	// scope. It never borrows a prior process incarnation or turns
	// cached data fresh.
	id := probe.Identity
	if !validTarget {
		id = metrics.ReplicaIdentity{
			ClusterID: input.source.clusterID,
			GroupID:   input.source.groupID,
			NodeName:  replica.NodeName,
			ReplicaID: replica.ReplicaID,
		}
	}
	if replyValid {
		id = probe.Observation.Sample.Identity
	} else if validTarget && exactSample(id, r.observation.Sample.Identity) {
		id.RunID = r.observation.Sample.Identity.RunID
	}
	scope := EvidenceScope{
		Replica:           id,
		Leader:            leader.Identity,
		SchedulerEpoch:    t.epoch,
		Term:              leader.Term,
		MembershipDigest:  leader.MembershipDigest,
		VoterConfigDigest: digest,
	}
	if scope != r.scope {
		r.matchKnown = false
		r.healthyAfterRecord = false
		r.scope = scope
	}
	for _, w := range []*ConditionWindow{&r.unavailable, &r.lag, &r.stalled, &r.standby} {
		w.MaxGap, w.MaxAge = g.voterConfig.MaxObservationGap, t.settings.MaxSampleAge
	}
	return replicaEvidenceFrame{input: input, scope: scope, validTarget: validTarget,
		replyValid: replyValid, maxSampleAge: t.settings.MaxSampleAge}
}

func (t *evidenceTracker) prepareReplicaObservation(r *replicaEvidence, frame *replicaEvidenceFrame) {
	now, leader, observer := frame.input.source.at, frame.input.source.leader, frame.input.source.observer
	probe := frame.input.probe
	validTarget, replyValid, id := frame.validTarget, frame.replyValid, frame.scope.Replica
	observed := probe.Observation.Clone()
	if !replyValid {
		observed = internalmetrics.AgeObservation(r.observation, internalmetrics.Age(now,
			r.at), t.settings.MaxSampleAge)
		if !validTarget || !exactSample(id, observed.Sample.Identity) {
			observed = metrics.ReplicaObservation{Sample: metrics.ReplicaSample{
				Identity: id}, State: metrics.Unknown}
		}
		observed.State = metrics.Unknown
	}
	if replyValid {
		observed.ReceivedAt = now.UTC()
	}
	observed.Reachability = "unknown"
	if validTarget && probe.Dispatched && (probe.Reachability == reachabilityAlive ||
		probe.Reachability == "unreachable") {
		observed.Reachability = probe.Reachability
	}
	observed.Evidence = internalmetrics.UnknownEvidence()
	observed.ReplicationLag = metrics.UnsignedValue{State: metrics.Unknown}
	observed.ApplyBacklog = metrics.UnsignedValue{State: metrics.Unknown}
	base := []Generation{
		generation("leader_raft", leader, leader.Raft),
		generation("observer_raft", observer, observer.Raft),
		generation("observer_writer", observer, observer.Writer),
	}
	frame.observed, frame.base = observed, base
}

func (t *evidenceTracker) observeReplicaUnavailable(r *replicaEvidence, frame *replicaEvidenceFrame) {
	now, scope, base := frame.input.source.at, frame.scope, frame.base
	leaderValid, validTarget := frame.input.source.leaderValid, frame.validTarget
	probe, observed := frame.input.probe, frame.observed
	probeVector := append(slices.Clone(base), Generation{Source: "probe", RunID: t.epoch, Sequence: t.sequence})
	r.unavailable.Observe(now, scope, EvidenceSample{
		Valid:       leaderValid && validTarget && probe.Dispatched && observed.Reachability != "unknown",
		True:        observed.Reachability == "unreachable",
		Generations: probeVector,
	})
}

func (t *evidenceTracker) observeReplicaProgress(g *groupEvidence, r *replicaEvidence, frame *replicaEvidenceFrame) {
	now, leader := frame.input.source.at, frame.input.source.leader
	replica := frame.input.replica
	leaderValid, validTarget, replyValid := frame.input.source.leaderValid, frame.validTarget, frame.replyValid
	scope, base := frame.scope, frame.base
	observed := frame.observed
	progress, progressKnown := frame.input.source.replicaProgress(replica.ReplicaID)
	lagKnown := leaderValid && progressKnown && validTarget
	lag := uint64(0)
	if lagKnown {
		if leader.Commit > progress.Match {
			lag = leader.Commit - progress.Match
		}
		observed.ReplicationLag = metrics.UnsignedValue{State: metrics.Current, Value: lag}
	}
	catchingUp := recordSnapshotEvidence(&observed, *frame, progress)
	r.lag.Observe(now, scope, EvidenceSample{Valid: lagKnown,
		True:        !catchingUp && lag > g.voterConfig.FailureLagEntries,
		Generations: base})
	advanced := r.matchKnown && progress.Match > r.match
	if !r.matchKnown || progress.Match != r.match {
		r.stalled.Reset()
	}
	r.stalled.Observe(now, scope,
		EvidenceSample{Valid: lagKnown && g.voterConfig.ReplaceNoProgress,
			True: !catchingUp && lag > 0, Generations: base})
	if lagKnown {
		r.match, r.matchKnown = progress.Match, true
	}
	ownFresh := replyValid && observed.Sample.Term == leader.Term &&
		observed.Sample.LeaderID == leader.LeaderID &&
		observed.Sample.MembershipDigest == leader.MembershipDigest &&
		freshRaft(observed.Sample, t.settings.MaxSampleAge) &&
		currentStamp(observed.Sample.Writer, t.settings.MaxSampleAge)
	backlog := uint64(0)
	if ownFresh {
		if observed.Sample.Commit > observed.Sample.Applied {
			backlog = observed.Sample.Commit - observed.Sample.Applied
		}
		observed.ApplyBacklog = metrics.UnsignedValue{State: metrics.Current, Value: backlog}
	}
	frame.observed, frame.progress, frame.lagKnown = observed, progress, lagKnown
	frame.lag, frame.advanced, frame.ownFresh, frame.backlog = lag, advanced, ownFresh, backlog
}

func (t *evidenceTracker) observeReplicaStandby(g *groupEvidence, r *replicaEvidence, frame *replicaEvidenceFrame) {
	now, leader, replica := frame.input.source.at, frame.input.source.leader, frame.input.replica
	key := topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	probe, observed, progress, base, scope := frame.input.probe, frame.observed, frame.progress, frame.base, frame.scope
	lagKnown, ownFresh, lag, backlog := frame.lagKnown, frame.ownFresh, frame.lag, frame.backlog
	demotion := cooldown(g.voterConfig.DemotionCooldown, g.demotions[key].at, now)
	stable := lagKnown && ownFresh && replica.Role == topology.ReplicaLearner && !replica.PinnedLearner &&
		!probe.Held && observed.Sample.Ready && !observed.Sample.Restoring &&
		observed.Sample.WALStall <= time.Second &&
		lag <= g.voterConfig.StandbyLagEntries && backlog <= g.voterConfig.StandbyApplyBacklogEntries &&
		progress.AckKnown && progress.AckAge >= 0 &&
		progress.AckAge <= t.settings.MaxSampleAge && demotion == 0 &&
		slices.Contains(leader.Learners, replica.ReplicaID)
	standbyVector := append(slices.Clone(base),
		generation("replica_meter", observed.Sample, observed.Sample.Meter),
		generation("replica_raft", observed.Sample, observed.Sample.Raft),
		generation("replica_writer", observed.Sample, observed.Sample.Writer), Generation{
			Source:   "leader_ack",
			RunID:    leader.Identity.RunID,
			Sequence: progress.AckSequence,
			Age:      progress.AckAge,
		})
	r.standby.Observe(now, scope, EvidenceSample{Valid: lagKnown && ownFresh &&
		progress.AckKnown, True: stable, Generations: standbyVector})
}

func (t *evidenceTracker) finishReplicaObservation(g *groupEvidence, r *replicaEvidence, frame *replicaEvidenceFrame) {
	now, leader, replica := frame.input.source.at, frame.input.source.leader, frame.input.replica
	probe, observed := frame.input.probe, frame.observed
	lagKnown, ownFresh, lag, advanced := frame.lagKnown, frame.ownFresh, frame.lag, frame.advanced
	if replica.Role == topology.ReplicaVoter && ownFresh && lagKnown &&
		observed.Reachability == reachabilityAlive && lag <= g.voterConfig.FailureLagEntries &&
		(lag == 0 || advanced) && slices.Contains(leader.Voters, replica.ReplicaID) {
		r.healthyAfterRecord = true
	}
	if replica.PinnedLearner {
		observed.Evidence.Reasons = append(observed.Evidence.Reasons, "pinned_learner")
	}
	if probe.Held {
		observed.Evidence.Reasons = append(observed.Evidence.Reasons, "local_stop")
	}
	if g.voterConfig.MaxObservationGap < t.settings.CollectInterval ||
		g.voterConfig.MaxObservationGap < t.settings.SampleInterval {
		observed.Evidence.Reasons = append(observed.Evidence.Reasons, "insufficient_observation_frequency")
		r.standby.Reset()
		r.unavailable.Reset()
		r.lag.Reset()
		r.stalled.Reset()
	}
	r.observation, r.at = observed, now
}

func (source evidenceSource) replicaProgress(replicaID uint64) (metrics.PeerProgress, bool) {
	var progress metrics.PeerProgress
	progressKnown := false
	for _, p := range source.leader.Progress {
		if p.ReplicaID == replicaID {
			progress, progressKnown = p, true
			break
		}
	}
	if replicaID == source.leader.LeaderID && source.leaderValid {
		progress, progressKnown = metrics.PeerProgress{ReplicaID: replicaID, Match: source.leader.Commit}, true
	}
	return progress, progressKnown
}
