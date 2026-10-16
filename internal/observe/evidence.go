package observe

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

const (
	reachabilityAlive = "alive"
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
	declaration   *topology.Group
	sourceVersion uint64
	current       bool
	confirmed     time.Time
	sourceAge     time.Duration
	voterConfig   topology.VoterConfig
	replicas      map[topology.ReplicaKey]*replicaEvidence
	recordIndex   uint64
	recordAt      time.Time
	demotions     map[topology.ReplicaKey]roleTimer
	observation   metrics.GroupObservation
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
		id.ClusterID == sample.ClusterID && id.GroupID == sample.GroupID && id.NodeName == sample.NodeName &&
		id.ReplicaID == sample.ReplicaID && id.InstanceID == sample.InstanceID && sample.RunID != ""
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
func (t *evidenceTracker) leader(g GroupSamples, group topology.Group, clusterID string) (metrics.ReplicaSample, bool) {
	var leader metrics.ReplicaSample
	ambiguous := false
	for _, p := range g.Probes {
		s := p.Observation.Sample
		if !p.Dispatched || p.Reachability != reachabilityAlive || p.Observation.State != metrics.Current ||
			s.LeaderID != s.Identity.ReplicaID || s.Term == 0 || s.MembershipDigest == "" ||
			p.Identity.ClusterID != clusterID || p.Identity.GroupID != group.ID ||
			!exactSample(p.Identity, s.Identity) || !freshRaft(s, t.settings.MaxSampleAge) ||
			!slices.Contains(s.Voters, s.LeaderID) ||
			!slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
				return r.NodeName == p.Identity.NodeName && r.ReplicaID == p.Identity.ReplicaID
			}) {
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
func voterConfigDigest(p topology.VoterConfig) string {
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

type evidenceInput struct {
	clusterID string
	local     []metrics.ReplicaObservation
	switches  []topology.VoterSwitchState
}

func (t *evidenceTracker) apply(now time.Time, doc topology.ProgressPage, round Round, input evidenceInput) {
	local, switches := input.local, input.switches
	if t.sequence == math.MaxUint64 {
		// Probe generations cannot be reused within this collector lifetime.
		// Exhaustion removes actionable windows until a fresh collector exists.
		clear(t.groups)
		return
	}
	t.sequence++
	observer := collectorObserver(local, round.Epoch)
	observerValid := observer.Term == round.Epoch.Term && observer.LeaderID == round.Epoch.LeaderID &&
		freshRaft(observer, t.settings.MaxSampleAge) && currentStamp(observer.Writer, t.settings.MaxSampleAge)
	records := map[string]topology.VoterSwitchState{}
	for _, record := range switches {
		records[record.GroupID] = record
	}
	retained := map[string]bool{}
	for _, row := range doc.Groups {
		group := row.Converged
		if group == nil || row.ConvergedVersion == 0 || group.VoterConfig == nil {
			continue
		}
		retained[group.ID] = true
		g := t.prepareGroup(row)
		samples, sampled := round.Groups[group.ID]
		if !sampled || samples.SourceVersion != row.ConvergedVersion {
			g.current = false
			clear(g.replicas)
			continue
		}
		t.applyGroup(now, *group, evidenceRoundContext{clusterID: input.clusterID, observer: observer,
			observerValid: observerValid, records: records, round: round})
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
		if !g.current {
			continue
		}
		group := t.snapshotGroup(now, g)
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

func (t *evidenceTracker) prepareGroup(row topology.GroupProgress) *groupEvidence {
	g := t.groups[row.GroupID]
	if g == nil {
		g = &groupEvidence{replicas: make(map[topology.ReplicaKey]*replicaEvidence),
			demotions: make(map[topology.ReplicaKey]roleTimer)}
		t.groups[row.GroupID] = g
	}
	if g.sourceVersion != row.ConvergedVersion || !reflect.DeepEqual(g.declaration, row.Converged) {
		// Preserve cooldown anchors for retained role records. Only this group's
		// sample windows lose continuity; independent groups keep theirs.
		clear(g.replicas)
		g.confirmed, g.sourceAge = time.Time{}, 0
		g.sourceVersion, g.declaration = row.ConvergedVersion, row.Clone().Converged
	}
	return g
}

func collectorObserver(local []metrics.ReplicaObservation, epoch LeaderEpoch) metrics.ReplicaSample {
	for _, v := range local {
		if v.Sample.Identity.GroupID == topology.MetaGroupID &&
			v.Sample.Identity.InstanceID == epoch.InstanceID && v.Sample.Identity.ReplicaID == epoch.LeaderID {
			return v.Sample
		}
	}
	return metrics.ReplicaSample{}
}
