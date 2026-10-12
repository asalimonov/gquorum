package observe

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"

	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

type replicaEvidence struct {
	scope                              EvidenceScope
	unavailable, lag, stalled, standby ConditionWindow
	match                              uint64
	matchKnown                         bool
	healthyAfterRecord                 bool
	observation                        metrics.ReplicaObservation
	at                                 time.Time
}
type groupEvidence struct {
	confirmed   time.Time
	sourceAge   time.Duration
	policy      topology.VoterPolicy
	replicas    map[topology.ReplicaKey]*replicaEvidence
	recordIndex uint64
	recordAt    time.Time
	demotions   map[topology.ReplicaKey]roleTimer
	observation metrics.GroupObservation
}
type roleTimer struct {
	index uint64
	at    time.Time
}

// evidenceTracker is owned by the collector's state lock. Its clock is local
// and monotonic; durable At values are display/audit data only.
type evidenceTracker struct {
	settings MetricsSettings
	epoch    string
	groups   map[string]*groupEvidence
	sequence uint64
}

func newEvidenceTracker(settings MetricsSettings) *evidenceTracker {
	return &evidenceTracker{settings: settings, groups: map[string]*groupEvidence{}}
}
func (t *evidenceTracker) reset(epoch string) {
	t.epoch = epoch
	clear(t.groups)
}
func exactSample(id metrics.ReplicaIdentity, sample metrics.ReplicaIdentity) bool {
	return id.ClusterID != "" && id.GroupID != "" && id.NodeName != "" && id.ReplicaID != 0 && id.InstanceID != "" &&
		id.ClusterID == sample.ClusterID && id.GroupID == sample.GroupID && id.NodeName == sample.NodeName && id.ReplicaID == sample.ReplicaID && id.InstanceID == sample.InstanceID && sample.RunID != ""
}
func currentStamp(s metrics.Stamp, bound time.Duration) bool {
	return s.State == metrics.Current && s.Sequence > 0 && s.Age >= 0 && s.Age <= bound
}
func freshRaft(s metrics.ReplicaSample, bound time.Duration) bool {
	return s.Running && s.Identity.RunID != "" && currentStamp(s.Meter, bound) && currentStamp(s.Raft, bound)
}
func generation(source string, s metrics.ReplicaSample, stamp metrics.Stamp) Generation {
	return Generation{Source: source, RunID: s.Identity.RunID, Sequence: stamp.Sequence, Age: stamp.Age}
}
func (t *evidenceTracker) leader(g GroupSamples) (metrics.ReplicaSample, bool) {
	var leader metrics.ReplicaSample
	ambiguous := false
	for _, p := range g.Probes {
		s := p.Observation.Sample
		if !p.Dispatched || p.Reachability != "alive" || p.Observation.State != metrics.Current || !exactSample(p.Identity, s.Identity) || !freshRaft(s, t.settings.MaxSampleAge) || s.LeaderID != s.Identity.ReplicaID || s.Term == 0 || s.MembershipDigest == "" || !slices.Contains(s.Voters, s.LeaderID) {
			continue
		}
		if leader.Term < s.Term {
			leader, ambiguous = s, false
		} else if leader.Term == s.Term && leader.Identity != s.Identity {
			ambiguous = true
		}
	}
	return leader, leader.LeaderID != 0 && !ambiguous
}
func policyDigest(p topology.VoterPolicy) string {
	raw, _ := json.Marshal(p)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func cooldown(duration time.Duration, start, now time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	return max(0, duration-internalmetrics.Age(now, start))
}
func (g *groupEvidence) observeRecords(now time.Time, state topology.VoterSwitchState) {
	if state.Last.Index != g.recordIndex {
		g.recordIndex = state.Last.Index
		g.recordAt = time.Time{}
		if state.Last.Index != 0 {
			g.recordAt = now
		}
		for _, r := range g.replicas {
			r.healthyAfterRecord = false
		}
	}
	retained := map[topology.ReplicaKey]bool{}
	for _, record := range state.Replicas {
		key := topology.ReplicaKey{NodeName: record.NodeName, ReplicaID: record.ReplicaID}
		if record.To != topology.ReplicaLearner || record.Index == 0 {
			continue
		}
		retained[key] = true
		if old, ok := g.demotions[key]; !ok || old.index != record.Index {
			g.demotions[key] = roleTimer{record.Index, now}
		}
	}
	for key := range g.demotions {
		if !retained[key] {
			delete(g.demotions, key)
		}
	}
}
func (t *evidenceTracker) apply(now time.Time, doc topology.Document, round Round, local []metrics.ReplicaObservation, switches []topology.VoterSwitchState) {
	if t.sequence == math.MaxUint64 {
		// Probe generations cannot be reused within this collector lifetime.
		// Exhaustion removes actionable windows until a fresh collector exists.
		clear(t.groups)
		return
	}
	t.sequence++
	var observer metrics.ReplicaSample
	for _, v := range local {
		if v.Sample.Identity.GroupID == topology.MetaGroupID && v.Sample.Identity.InstanceID == round.Epoch.InstanceID && v.Sample.Identity.ReplicaID == round.Epoch.LeaderID {
			observer = v.Sample
			break
		}
	}
	observerValid := freshRaft(observer, t.settings.MaxSampleAge) && observer.Term == round.Epoch.Term && observer.LeaderID == round.Epoch.LeaderID && currentStamp(observer.Writer, t.settings.MaxSampleAge)
	records := map[string]topology.VoterSwitchState{}
	for _, record := range switches {
		records[record.GroupID] = record
	}
	retained := map[string]bool{}
	for _, group := range doc.Groups {
		if group.VoterPolicy == nil {
			continue
		}
		retained[group.ID] = true
		g := t.groups[group.ID]
		if g == nil {
			g = &groupEvidence{replicas: map[topology.ReplicaKey]*replicaEvidence{}, demotions: map[topology.ReplicaKey]roleTimer{}}
			t.groups[group.ID] = g
		}
		g.policy = *group.VoterPolicy
		g.observeRecords(now, records[group.ID])
		samples := round.Groups[group.ID]
		leader, leaderValid := t.leader(samples)
		leaderValid = leaderValid && observerValid && samples.Error == nil
		g.observation = metrics.GroupObservation{GroupID: group.ID, LeaderID: leader.LeaderID, Term: leader.Term, MembershipDigest: leader.MembershipDigest, State: metrics.Unknown, TargetVoters: g.policy.TargetVoters, NativeVoters: len(leader.Voters), Shortfall: max(0, g.policy.TargetVoters-len(leader.Voters))}
		if leaderValid {
			g.observation.State = metrics.Current
			g.confirmed = now
			g.sourceAge = max(leader.Meter.Age, leader.Raft.Age, observer.Meter.Age, observer.Raft.Age, observer.Writer.Age)
		}
		probes := map[topology.ReplicaKey]Probe{}
		for _, probe := range samples.Probes {
			probes[topology.ReplicaKey{NodeName: probe.Identity.NodeName, ReplicaID: probe.Identity.ReplicaID}] = probe
		}
		kept := map[topology.ReplicaKey]bool{}
		digest := policyDigest(g.policy)
		for _, replica := range group.Replicas {
			key := topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
			kept[key] = true
			r := g.replicas[key]
			if r == nil {
				r = &replicaEvidence{}
				g.replicas[key] = r
			}
			probe := probes[key]
			validTarget := probe.Identity.ClusterID == doc.ClusterID && probe.Identity.GroupID == group.ID && probe.Identity.NodeName == replica.NodeName && probe.Identity.ReplicaID == replica.ReplicaID && probe.Identity.InstanceID != ""
			replyValid := validTarget && probe.Dispatched && probe.Reachability == "alive" && exactSample(probe.Identity, probe.Observation.Sample.Identity)
			// A failed exact probe can retain the last known engine run for scope. It
			// never borrows a prior process incarnation or turns cached data fresh.
			id := probe.Identity
			if !validTarget {
				id = metrics.ReplicaIdentity{ClusterID: doc.ClusterID, GroupID: group.ID, NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
			}
			if replyValid {
				id = probe.Observation.Sample.Identity
			} else if validTarget && exactSample(id, r.observation.Sample.Identity) {
				id.RunID = r.observation.Sample.Identity.RunID
			}
			scope := EvidenceScope{Replica: id, Leader: leader.Identity, SchedulerEpoch: t.epoch, Term: leader.Term, MembershipDigest: leader.MembershipDigest, PolicyDigest: digest}
			if scope != r.scope {
				r.matchKnown = false
				r.healthyAfterRecord = false
				r.scope = scope
			}
			for _, w := range []*ConditionWindow{&r.unavailable, &r.lag, &r.stalled, &r.standby} {
				w.MaxGap, w.MaxAge = g.policy.MaxObservationGap, t.settings.MaxSampleAge
			}
			observed := probe.Observation.Clone()
			if !replyValid {
				observed = internalmetrics.AgeObservation(r.observation, internalmetrics.Age(now, r.at), t.settings.MaxSampleAge)
				if !validTarget || !exactSample(id, observed.Sample.Identity) {
					observed = metrics.ReplicaObservation{Sample: metrics.ReplicaSample{Identity: id}, State: metrics.Unknown}
				}
				observed.State = metrics.Unknown
			}
			observed.ReceivedAt = now.UTC()
			observed.Reachability = "unknown"
			if validTarget && probe.Dispatched && (probe.Reachability == "alive" || probe.Reachability == "unreachable") {
				observed.Reachability = probe.Reachability
			}
			observed.Evidence = internalmetrics.UnknownEvidence()
			observed.ReplicationLag = metrics.UnsignedValue{State: metrics.Unknown}
			observed.ApplyBacklog = metrics.UnsignedValue{State: metrics.Unknown}
			base := []Generation{generation("leader_raft", leader, leader.Raft), generation("observer_raft", observer, observer.Raft), generation("observer_writer", observer, observer.Writer)}
			probeVector := append(slices.Clone(base), Generation{Source: "probe", RunID: t.epoch, Sequence: t.sequence})
			r.unavailable.Observe(now, scope, EvidenceSample{Valid: leaderValid && validTarget && probe.Dispatched && observed.Reachability != "unknown", True: observed.Reachability == "unreachable", Generations: probeVector})
			var progress metrics.PeerProgress
			progressKnown := false
			for _, p := range leader.Progress {
				if p.ReplicaID == replica.ReplicaID {
					progress, progressKnown = p, true
					break
				}
			}
			if replica.ReplicaID == leader.LeaderID && leaderValid {
				progress, progressKnown = metrics.PeerProgress{ReplicaID: replica.ReplicaID, Match: leader.Commit}, true
			}
			lagKnown := leaderValid && progressKnown && validTarget
			lag := uint64(0)
			if lagKnown {
				if leader.Commit > progress.Match {
					lag = leader.Commit - progress.Match
				}
				observed.ReplicationLag = metrics.UnsignedValue{State: metrics.Current, Value: lag}
			}
			r.lag.Observe(now, scope, EvidenceSample{Valid: lagKnown, True: lag > g.policy.FailureLagEntries, Generations: base})
			advanced := r.matchKnown && progress.Match > r.match
			if !r.matchKnown || progress.Match != r.match {
				r.stalled.Reset()
			}
			r.stalled.Observe(now, scope, EvidenceSample{Valid: lagKnown && g.policy.ReplaceNoProgress, True: lag > 0, Generations: base})
			if lagKnown {
				r.match, r.matchKnown = progress.Match, true
			}
			ownFresh := replyValid && freshRaft(observed.Sample, t.settings.MaxSampleAge) && currentStamp(observed.Sample.Writer, t.settings.MaxSampleAge) && observed.Sample.Term == leader.Term && observed.Sample.LeaderID == leader.LeaderID && observed.Sample.MembershipDigest == leader.MembershipDigest
			backlog := uint64(0)
			if ownFresh {
				if observed.Sample.Commit > observed.Sample.Applied {
					backlog = observed.Sample.Commit - observed.Sample.Applied
				}
				observed.ApplyBacklog = metrics.UnsignedValue{State: metrics.Current, Value: backlog}
			}
			demotion := cooldown(g.policy.DemotionCooldown, g.demotions[key].at, now)
			stable := lagKnown && ownFresh && replica.Role == topology.ReplicaLearner && slices.Contains(leader.Learners, replica.ReplicaID) && !replica.PinnedLearner && !probe.Held && observed.Sample.Ready && !observed.Sample.Restoring && observed.Sample.WALStall <= time.Second && lag <= g.policy.StandbyLagEntries && backlog <= g.policy.StandbyApplyBacklogEntries && progress.AckKnown && progress.AckAge >= 0 && progress.AckAge <= t.settings.MaxSampleAge && demotion == 0
			standbyVector := append(slices.Clone(base), generation("replica_meter", observed.Sample, observed.Sample.Meter), generation("replica_raft", observed.Sample, observed.Sample.Raft), generation("replica_writer", observed.Sample, observed.Sample.Writer), Generation{Source: "leader_ack", RunID: leader.Identity.RunID, Sequence: progress.AckSequence, Age: progress.AckAge})
			r.standby.Observe(now, scope, EvidenceSample{Valid: lagKnown && ownFresh && progress.AckKnown, True: stable, Generations: standbyVector})
			if replica.Role == topology.ReplicaVoter && slices.Contains(leader.Voters, replica.ReplicaID) && ownFresh && lagKnown && observed.Reachability == "alive" && lag <= g.policy.FailureLagEntries && (lag == 0 || advanced) {
				r.healthyAfterRecord = true
			}
			if replica.PinnedLearner {
				observed.Evidence.Reasons = append(observed.Evidence.Reasons, "pinned_learner")
			}
			if probe.Held {
				observed.Evidence.Reasons = append(observed.Evidence.Reasons, "local_stop")
			}
			if g.policy.MaxObservationGap < t.settings.CollectInterval || g.policy.MaxObservationGap < t.settings.SampleInterval {
				observed.Evidence.Reasons = append(observed.Evidence.Reasons, "insufficient_observation_frequency")
				r.standby.Reset()
				r.unavailable.Reset()
				r.lag.Reset()
				r.stalled.Reset()
			}
			r.observation, r.at = observed, now
		}
		for key := range g.replicas {
			if !kept[key] {
				delete(g.replicas, key)
			}
		}
	}
	for id := range t.groups {
		if !retained[id] {
			delete(t.groups, id)
		}
	}
}
func (t *evidenceTracker) snapshot(now time.Time) []metrics.GroupObservation {
	out := make([]metrics.GroupObservation, 0, len(t.groups))
	for _, g := range t.groups {
		group := g.observation.Clone()
		if group.State == metrics.Current && (internalmetrics.Age(now, g.confirmed) > g.policy.MaxObservationGap || internalmetrics.AddDuration(g.sourceAge, internalmetrics.Age(now, g.confirmed)) > t.settings.MaxSampleAge) {
			group.State = metrics.Stale
		}
		group.CooldownRemaining = cooldown(g.policy.SwitchCooldown, g.recordAt, now)
		group.EligibleStandbys = 0
		for key, r := range g.replicas {
			o := internalmetrics.AgeObservation(r.observation, internalmetrics.Age(now, r.at), t.settings.MaxSampleAge)
			o.Evidence.UnavailableFor = r.unavailable.Duration(now)
			o.Evidence.ReplicationLagFor = r.lag.Duration(now)
			if o.Evidence.ReplicationLagFor.State != metrics.Current {
				o.ReplicationLag.State = o.Evidence.ReplicationLagFor.State
			}
			o.Evidence.NoProgressFor = r.stalled.Duration(now)
			o.Evidence.StandbyReadyFor = r.standby.Duration(now)
			o.Evidence.DemotionCooldownRemaining = cooldown(g.policy.DemotionCooldown, g.demotions[key].at, now)
			if o.Evidence.DemotionCooldownRemaining > 0 {
				o.Evidence.Reasons = append(o.Evidence.Reasons, "replica_cooldown")
			}
			if qualifies(o.Evidence.StandbyReadyFor, g.policy.StandbyWindow) {
				group.EligibleStandbys++
			}
			if r.healthyAfterRecord && g.policy.AllowNewFailureDuringCooldown && (qualifies(o.Evidence.UnavailableFor, g.policy.FailureWindow) || qualifies(o.Evidence.ReplicationLagFor, g.policy.FailureWindow) || g.policy.ReplaceNoProgress && qualifies(o.Evidence.NoProgressFor, g.policy.NoProgressWindow)) {
				o.Evidence.Reasons = append(o.Evidence.Reasons, "new_failure")
			}
			if internalmetrics.Age(now, r.at) > g.policy.MaxObservationGap {
				o.Reachability = "unknown"
				o.ReplicationLag.State = metrics.Stale
				group.State = metrics.Stale
			}
			if o.Sample.Identity.ReplicaID == group.LeaderID && !freshRaft(o.Sample, t.settings.MaxSampleAge) {
				group.State = metrics.Stale
			}
			group.Replicas = append(group.Replicas, o)
		}
		slices.SortFunc(group.Replicas, func(a, b metrics.ReplicaObservation) int {
			if a.Sample.Identity.ReplicaID < b.Sample.Identity.ReplicaID {
				return -1
			}
			if a.Sample.Identity.ReplicaID > b.Sample.Identity.ReplicaID {
				return 1
			}
			return 0
		})
		out = append(out, group)
	}
	slices.SortFunc(out, func(a, b metrics.GroupObservation) int {
		if a.GroupID < b.GroupID {
			return -1
		}
		if a.GroupID > b.GroupID {
			return 1
		}
		return 0
	})
	return out
}
func qualifies(value metrics.DurationValue, window time.Duration) bool {
	return value.State == metrics.Current && value.Value >= window
}
