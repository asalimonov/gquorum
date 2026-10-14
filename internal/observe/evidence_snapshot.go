package observe

import (
	"slices"
	"time"

	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

type replicaSnapshot struct {
	observed metrics.ReplicaObservation
	stale    bool
}

func (t *evidenceTracker) snapshotGroup(now time.Time, g *groupEvidence) metrics.GroupObservation {
	group := g.observation.Clone()
	if group.State == metrics.Current &&
		(internalmetrics.Age(now, g.confirmed) > g.voterConfig.MaxObservationGap ||
			internalmetrics.AddDuration(g.sourceAge, internalmetrics.Age(now, g.confirmed)) > t.settings.MaxSampleAge) {
		group.State = metrics.Stale
	}
	group.CooldownRemaining = cooldown(g.voterConfig.SwitchCooldown, g.recordAt, now)
	group.EligibleStandbys = 0
	for key, r := range g.replicas {
		result := t.snapshotReplica(now, g, key, r)
		o := result.observed
		if qualifies(o.Evidence.StandbyReadyFor, g.voterConfig.StandbyWindow) {
			group.EligibleStandbys++
		}
		if result.stale {
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
	return group
}

func (t *evidenceTracker) snapshotReplica(now time.Time, g *groupEvidence,
	key topology.ReplicaKey, r *replicaEvidence) replicaSnapshot {
	stale := false
	o := internalmetrics.AgeObservation(r.observation, internalmetrics.Age(now, r.at), t.settings.MaxSampleAge)
	o.Evidence.UnavailableFor = r.unavailable.Duration(now)
	o.Evidence.ReplicationLagFor = r.lag.Duration(now)
	if o.Evidence.ReplicationLagFor.State != metrics.Current {
		o.ReplicationLag.State = o.Evidence.ReplicationLagFor.State
	}
	o.Evidence.NoProgressFor = r.stalled.Duration(now)
	o.Evidence.StandbyReadyFor = r.standby.Duration(now)
	o.Evidence.DemotionCooldownRemaining = cooldown(g.voterConfig.DemotionCooldown, g.demotions[key].at, now)
	if o.Evidence.DemotionCooldownRemaining > 0 {
		o.Evidence.Reasons = append(o.Evidence.Reasons, "replica_cooldown")
	}
	if r.healthyAfterRecord && g.voterConfig.AllowNewFailureDuringCooldown &&
		(qualifies(o.Evidence.UnavailableFor, g.voterConfig.FailureWindow) ||
			qualifies(o.Evidence.ReplicationLagFor, g.voterConfig.FailureWindow) ||
			g.voterConfig.ReplaceNoProgress && qualifies(o.Evidence.NoProgressFor, g.voterConfig.NoProgressWindow)) {
		o.Evidence.Reasons = append(o.Evidence.Reasons, "new_failure")
	}
	if internalmetrics.Age(now, r.at) > g.voterConfig.MaxObservationGap {
		o.Reachability = "unknown"
		o.ReplicationLag.State = metrics.Stale
		stale = true
	}
	if o.Sample.Identity.ReplicaID == g.observation.LeaderID && !freshRaft(o.Sample, t.settings.MaxSampleAge) {
		stale = true
	}
	return replicaSnapshot{observed: o, stale: stale}
}
