package observe

import (
	"slices"

	"github.com/asalimonov/gquorum/metrics"
)

func snapshotCatchUpValid(frame replicaEvidenceFrame, p metrics.PeerProgress) bool {
	x := p.Snapshot
	if x == nil || !frame.input.source.leaderValid || !frame.validTarget || !frame.replyValid ||
		frame.observed.Reachability != reachabilityAlive ||
		!frame.observed.Sample.Running || !frame.observed.Sample.Ready {
		return false
	}
	if !snapshotCatchUpIdentity(frame, p, x) {
		return false
	}
	if x.EpisodeFailed || x.EpisodeRemaining <= 0 || x.RemainingTotal <= 0 ||
		x.TotalBytes == 0 || x.VerifiedBytes > x.TotalBytes ||
		x.ProgressSequence == 0 || x.ProgressTimeout <= 0 || x.ProgressAge < 0 {
		return false
	}
	target := frame.observed.Sample
	if target.Term > frame.input.source.leader.Term {
		return false
	}
	if x.Phase == "handoff_grace" {
		return x.RemainingGrace > 0
	}
	return x.Phase == "sending" && x.VerifiedBytes > 0 && x.ProgressAge < x.ProgressTimeout &&
		target.Term == frame.input.source.leader.Term && target.LeaderID == frame.input.source.leader.LeaderID &&
		freshRaft(target, frame.maxSampleAge)
}

func snapshotFailureReason(p metrics.PeerProgress) string {
	if p.Snapshot != nil && p.Snapshot.EpisodeFailed {
		return "snapshot_transfer_failed"
	}
	return ""
}

func snapshotCatchUpIdentity(frame replicaEvidenceFrame, p metrics.PeerProgress, x *metrics.SnapshotCatchUp) bool {
	leader := frame.input.source.leader
	if x.Destination.RunID == "" || x.Destination != frame.scope.Replica || x.LeaderTerm != leader.Term ||
		p.ReplicaID != x.Destination.ReplicaID || x.DeliveryID == 0 ||
		x.SnapshotIndex <= p.Match || x.SnapshotTerm == 0 ||
		x.SnapshotTerm > x.LeaderTerm {
		return false
	}
	return slices.Contains(leader.Voters, p.ReplicaID) || slices.Contains(leader.Learners, p.ReplicaID)
}
func recordSnapshotEvidence(
	observed *metrics.ReplicaObservation, frame replicaEvidenceFrame, p metrics.PeerProgress,
) bool {
	allowed := snapshotCatchUpValid(frame, p)
	if allowed {
		observed.Evidence.Reasons = append(observed.Evidence.Reasons, "snapshot_catchup")
	}
	if reason := snapshotFailureReason(p); reason != "" {
		observed.Evidence.Reasons = append(observed.Evidence.Reasons, reason)
	}
	return allowed
}
