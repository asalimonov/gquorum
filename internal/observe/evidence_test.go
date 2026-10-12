//go:build !functional

package observe

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func TestProbeGenerationExhaustionInvalidatesWindowsWithoutWrapping(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	tracker.sequence = math.MaxUint64 - 1
	round, local := evidenceRound(doc, 1)
	now := time.Unix(100, 0)
	tracker.apply(now, doc, round, local, nil)
	if tracker.sequence != math.MaxUint64 || len(tracker.snapshot(now)) == 0 {
		t.Fatal("last valid probe generation was not observed")
	}
	for i := range 2 {
		if i == 1 {
			tracker.reset("another-epoch")
		}
		round, local = evidenceRound(doc, uint64(i+2))
		tracker.apply(now.Add(time.Second), doc, round, local, nil)
		if tracker.sequence != math.MaxUint64 || len(tracker.snapshot(now.Add(time.Second))) != 0 {
			t.Fatal("exhausted probe generation became actionable")
		}
	}
}

func observedReplica(t *testing.T, tracker *evidenceTracker, now time.Time, id uint64) metrics.ReplicaObservation {
	t.Helper()
	for _, g := range tracker.snapshot(now) {
		for _, r := range g.Replicas {
			if r.Sample.Identity.ReplicaID == id {
				return r
			}
		}
	}
	t.Fatalf("missing observation %d", id)
	return metrics.ReplicaObservation{}
}
func TestEvidenceReplacementAndStandbyWindows(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	// Begin from an exact ready identity, then independently accumulate failure
	// and stability. Node reachability is not used as replication progress.
	round, local := evidenceRound(doc, 1)
	tracker.apply(start.Add(-time.Second), doc, round, local, nil)
	for i := 0; i <= 60; i++ {
		round, local = evidenceRound(doc, uint64(i+2))
		group := round.Groups[topology.MetaGroupID]
		group.Probes[2].Reachability = "unreachable"
		group.Probes[2].Observation = metrics.ReplicaObservation{}
		round.Groups[topology.MetaGroupID] = group
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, doc, round, local, nil)
		victim := observedReplica(t, tracker, now, 3)
		if got := qualifies(victim.Evidence.UnavailableFor, 30*time.Second); got != (i >= 30) {
			t.Fatalf("at %ds victim evidence %+v", i, victim.Evidence)
		}
		candidate := observedReplica(t, tracker, now, 4)
		// Candidate has been qualifying since the initial healthy observation.
		if got := qualifies(candidate.Evidence.StandbyReadyFor, 60*time.Second); got != (i >= 59) {
			t.Fatalf("at %ds standby evidence %+v", i, candidate.Evidence)
		}
	}
	snapshot := tracker.snapshot(start.Add(60 * time.Second))
	if snapshot[0].EligibleStandbys != 2 {
		t.Fatal(snapshot)
	}
	// Reading never earns time and stale cached evidence cannot approve a switch.
	late := observedReplica(t, tracker, start.Add(90*time.Second), 4)
	if late.Evidence.StandbyReadyFor.State == metrics.Current {
		t.Fatal(late.Evidence)
	}
}
func TestEvidenceLagThresholdsAndApplyOnlyBlocksPromotion(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	doc.Groups[0].VoterPolicy.ReplaceNoProgress = false
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	for i := 0; i <= 60; i++ {
		round, local := evidenceRound(doc, uint64(i+1))
		g := round.Groups[topology.MetaGroupID]
		// A 1001-entry replication lag qualifies; exactly 1000 does not.
		g.Probes[0].Observation.Sample.Progress[1].Match = 999
		g.Probes[0].Observation.Sample.Progress[2].Match = 1000
		// Apply lag alone cannot trigger replacing n3, but blocks learner n4.
		g.Probes[2].Observation.Sample.Applied = 0
		g.Probes[3].Observation.Sample.Applied = 1899
		round.Groups[topology.MetaGroupID] = g
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, doc, round, local, nil)
	}
	now := start.Add(60 * time.Second)
	if !qualifies(observedReplica(t, tracker, now, 2).Evidence.ReplicationLagFor, 30*time.Second) {
		t.Fatal("sustained replication failure did not qualify")
	}
	if qualifies(observedReplica(t, tracker, now, 3).Evidence.ReplicationLagFor, 30*time.Second) {
		t.Fatal("apply backlog or threshold equality triggered replacement")
	}
	if qualifies(observedReplica(t, tracker, now, 4).Evidence.StandbyReadyFor, 60*time.Second) {
		t.Fatal("apply backlog admitted learner")
	}
	if !qualifies(observedReplica(t, tracker, now, 5).Evidence.StandbyReadyFor, 60*time.Second) {
		t.Fatal("ready independent standby blocked")
	}
}
func TestEvidenceNoProgressRequiresPositiveLagAndResetsOnMatch(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	for i := 0; i <= 65; i++ {
		round, local := evidenceRound(doc, uint64(i+1))
		g := round.Groups[topology.MetaGroupID]
		g.Probes[0].Observation.Sample.Progress[1].Match = 1998
		if i >= 31 {
			g.Probes[0].Observation.Sample.Progress[1].Match = 1999
		}
		round.Groups[topology.MetaGroupID] = g
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, doc, round, local, nil)
		got := qualifies(observedReplica(t, tracker, now, 2).Evidence.NoProgressFor, 30*time.Second)
		if got != (i == 30 || i >= 61) {
			t.Fatalf("at %d no-progress=%v", i, got)
		}
		if qualifies(observedReplica(t, tracker, now, 3).Evidence.NoProgressFor, 30*time.Second) {
			t.Fatal("idle caught-up voter failed")
		}
	}
}
func TestEvidenceDuplicateMissingAndStaleSourcesCannotEarnTime(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"cached_learner", "frozen_writer", "unknown_probe", "undispatched", "old_term", "membership_change"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			doc, settings := evidenceFixture()
			tracker := newEvidenceTracker(settings)
			tracker.reset("epoch")
			start := time.Unix(100, 0)
			for i := 0; i <= 90; i++ {
				round, local := evidenceRound(doc, uint64(i+1))
				g := round.Groups[topology.MetaGroupID]
				switch mode {
				case "cached_learner":
					g.Probes[3].Observation.Sample.Meter.Sequence = 1
				case "frozen_writer":
					g.Probes[3].Observation.Sample.Writer.State = metrics.Stale
				case "unknown_probe":
					g.Probes[3].Reachability = "unknown"
				case "undispatched":
					g.Probes[3].Dispatched = false
				case "old_term":
					g.Probes[3].Observation.Sample.Term = 0
				case "membership_change":
					g.Probes[0].Observation.Sample.MembershipDigest = fmt.Sprint(i)
				}
				round.Groups[topology.MetaGroupID] = g
				now := start.Add(time.Duration(i) * time.Second)
				tracker.apply(now, doc, round, local, nil)
				if qualifies(observedReplica(t, tracker, now, 4).Evidence.StandbyReadyFor, 60*time.Second) {
					t.Fatalf("%s earned stability", mode)
				}
			}
		})
	}
}
func TestEvidenceCooldownNewFailureAndObserverFailover(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	record := topology.RoleChangeRecord{Index: 10, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, From: topology.ReplicaVoter, To: topology.ReplicaLearner}
	records := []topology.VoterSwitchState{{GroupID: topology.MetaGroupID, Last: record, Replicas: []topology.RoleChangeRecord{record}}}
	for i := 0; i <= 40; i++ {
		round, local := evidenceRound(doc, uint64(i+1))
		g := round.Groups[topology.MetaGroupID]
		if i > 0 {
			g.Probes[2].Reachability = "unreachable"
			g.Probes[2].Observation = metrics.ReplicaObservation{}
		}
		round.Groups[topology.MetaGroupID] = g
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, doc, round, local, records)
	}
	now := start.Add(40 * time.Second)
	if !slices.Contains(observedReplica(t, tracker, now, 3).Evidence.Reasons, "new_failure") {
		t.Fatal("healthy then failed voter lacks exception")
	}
	if got := tracker.snapshot(now)[0].CooldownRemaining; got != 260*time.Second {
		t.Fatalf("record reread rearmed cooldown: %s", got)
	}
	if got := observedReplica(t, tracker, now, 4).Evidence.DemotionCooldownRemaining; got != 260*time.Second {
		t.Fatal(got)
	}
	tracker.reset("new-observer")
	for i := 0; i <= 40; i++ {
		round, local := evidenceRound(doc, uint64(i+50))
		g := round.Groups[topology.MetaGroupID]
		g.Probes[2].Reachability = "unreachable"
		g.Probes[2].Observation = metrics.ReplicaObservation{}
		round.Groups[topology.MetaGroupID] = g
		tracker.apply(now.Add(time.Duration(i)*time.Second), doc, round, local, records)
	}
	later := now.Add(40 * time.Second)
	if slices.Contains(observedReplica(t, tracker, later, 3).Evidence.Reasons, "new_failure") {
		t.Fatal("already failed voter inherited exception after failover")
	}
	if tracker.snapshot(later)[0].CooldownRemaining != 260*time.Second {
		t.Fatal("failover did not rearm cooldown")
	}
}
