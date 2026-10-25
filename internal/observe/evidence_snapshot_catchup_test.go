//go:build !functional

package observe

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/votercheck"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func snapshotEvidenceRound(doc topology.Document, sequence uint64) (Round, []metrics.ReplicaObservation) {
	round, local := evidenceRound(doc, sequence)
	group := round.Groups[topology.MetaGroupID]
	peer := &group.Probes[0].Observation.Sample.Progress[2]
	peer.Match = 10
	peer.Snapshot = &metrics.SnapshotCatchUp{DeliveryID: sequence, SnapshotIndex: 2000, SnapshotTerm: 1, LeaderTerm: 1,
		Destination: group.Probes[2].Observation.Sample.Identity, VerifiedBytes: sequence, TotalBytes: 1000, ProgressSequence: sequence,
		ProgressTimeout: time.Second, RemainingTotal: time.Minute, EpisodeRemaining: time.Minute, EpisodeAnchorMatch: 10, Phase: "sending"}
	round.Groups[topology.MetaGroupID] = group
	return round, local
}
func TestSnapshotCatchupSuppressesOnlyLagAndStallWithinOneEpisode(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	for i := uint64(1); i <= 65; i++ {
		round, local := snapshotEvidenceRound(doc, i)
		failed := i > 32
		samples := round.Groups[topology.MetaGroupID]
		samples.Probes[0].Observation.Sample.Progress[2].Snapshot.EpisodeFailed = failed
		round.Groups[topology.MetaGroupID] = samples
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, evidenceTopology(doc), round, evidenceInput{clusterID: doc.ClusterID, local: local})
		observed := observedReplica(t, tracker, now, 3)
		if observed.ReplicationLag.Value != 1990 || observed.ReplicationLag.State != metrics.Current {
			t.Fatal("raw lag hidden", observed)
		}
		if i <= 32 {
			if votercheck.ReplacementFailure(*doc.Groups[0].VoterConfig, observed.Evidence) || !slices.Contains(observed.Evidence.Reasons, "snapshot_catchup") {
				t.Fatal("progressing snapshot failed repair protection", i, observed)
			}
		} else {
			if slices.Contains(observed.Evidence.Reasons, "snapshot_catchup") {
				t.Fatal("new progress renewed failed episode")
			}
			if !slices.Contains(observed.Evidence.Reasons, "snapshot_transfer_failed") {
				t.Fatal("terminal failure not visible")
			}
			if got := votercheck.ReplacementFailure(*doc.Groups[0].VoterConfig, observed.Evidence); got != (i >= 63) {
				t.Fatal("ordinary failure window not preserved", i, observed.Evidence)
			}
		}
	}
}
func TestSnapshotCatchupRefusesWrongRunStaleProgressAndExpiredBounds(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unknown-run", "old-run", "expired-episode", "expired-total", "stalled", "zero-bytes", "unreachable", "stopped", "old-term", "stale-target", "contrary-target-term"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			doc, settings := evidenceFixture()
			tracker := newEvidenceTracker(settings)
			tracker.reset("epoch")
			start := time.Unix(100, 0)
			for i := uint64(1); i <= 33; i++ {
				round, local := snapshotEvidenceRound(doc, i)
				group := round.Groups[topology.MetaGroupID]
				x := group.Probes[0].Observation.Sample.Progress[2].Snapshot
				switch kind {
				case "unknown-run":
					x.Destination.RunID = ""
				case "old-run":
					x.Destination.RunID = "old"
				case "expired-episode":
					x.EpisodeRemaining = 0
				case "expired-total":
					x.RemainingTotal = 0
				case "stalled":
					x.ProgressAge = x.ProgressTimeout
				case "zero-bytes":
					x.VerifiedBytes = 0
				case "unreachable":
					group.Probes[2].Reachability = "unreachable"
				case "stopped":
					group.Probes[2].Observation.Sample.Running = false
				case "old-term":
					x.LeaderTerm = 2
				case "stale-target":
					group.Probes[2].Observation.Sample.Raft.Age = time.Hour
				case "contrary-target-term":
					group.Probes[2].Observation.Sample.Term = 2
				}
				round.Groups[topology.MetaGroupID] = group
				now := start.Add(time.Duration(i) * time.Second)
				tracker.apply(now, evidenceTopology(doc), round, evidenceInput{clusterID: doc.ClusterID, local: local})
				observed := observedReplica(t, tracker, now, 3)
				if slices.Contains(observed.Evidence.Reasons, "snapshot_catchup") {
					t.Fatal("invalid snapshot evidence protected voter", kind)
				}
				if i == 33 && !votercheck.ReplacementFailure(*doc.Groups[0].VoterConfig, observed.Evidence) {
					t.Fatal("snapshot blocked ordinary repair", observed.Evidence)
				}
			}
		})
	}
}
func TestSnapshotCatchupGraceExpiresWithoutRenewingFromReads(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	for i := uint64(1); i <= 35; i++ {
		round, local := snapshotEvidenceRound(doc, i)
		group := round.Groups[topology.MetaGroupID]
		x := group.Probes[0].Observation.Sample.Progress[2].Snapshot
		x.Phase = "handoff_grace"
		x.VerifiedBytes = x.TotalBytes
		x.ProgressAge = time.Minute
		x.RemainingGrace = max(time.Duration(0), 3*time.Second-time.Duration(i)*time.Second)
		round.Groups[topology.MetaGroupID] = group
		now := start.Add(time.Duration(i) * time.Second)
		tracker.apply(now, evidenceTopology(doc), round, evidenceInput{clusterID: doc.ClusterID, local: local})
		observed := observedReplica(t, tracker, now, 3)
		if got := slices.Contains(observed.Evidence.Reasons, "snapshot_catchup"); got != (i < 3) {
			t.Fatal(i, observed.Evidence)
		}
		if i == 35 && !votercheck.ReplacementFailure(*doc.Groups[0].VoterConfig, observed.Evidence) {
			t.Fatal("grace renewal blocked repair")
		}
	}
}

func TestSnapshotCatchupCachedReadsDoNotEarnFailureOrRefreshTransfer(t *testing.T) {
	t.Parallel()
	doc, settings := evidenceFixture()
	tracker := newEvidenceTracker(settings)
	tracker.reset("epoch")
	start := time.Unix(100, 0)
	round, local := snapshotEvidenceRound(doc, 1)
	tracker.apply(start, evidenceTopology(doc), round, evidenceInput{clusterID: doc.ClusterID, local: local})
	late := observedReplica(t, tracker, start.Add(time.Minute), 3)
	if late.Evidence.ReplicationLagFor.State == metrics.Current || late.Evidence.NoProgressFor.State == metrics.Current {
		t.Fatal("cached read refreshed source predicate", late.Evidence)
	}
	for i := uint64(2); i <= 33; i++ {
		round, local = evidenceRound(doc, i)
		group := round.Groups[topology.MetaGroupID]
		group.Probes[0].Observation.Sample.Progress[2].Match = 10
		round.Groups[topology.MetaGroupID] = group
		now := start.Add(time.Minute + time.Duration(i)*time.Second)
		tracker.apply(now, evidenceTopology(doc), round, evidenceInput{clusterID: doc.ClusterID, local: local})
		observed := observedReplica(t, tracker, now, 3)
		if got := votercheck.ReplacementFailure(*doc.Groups[0].VoterConfig, observed.Evidence); got != (i >= 32) {
			t.Fatal("read time backfilled failure duration", i, observed.Evidence)
		}
	}
}
