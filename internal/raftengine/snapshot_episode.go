package raftengine

import (
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
)

// SnapshotPolicy supplies managed source deadlines without importing the load
// transfer mechanism. Zero disables repair observations for legacy owners.
type SnapshotPolicy struct{ TotalTimeout, ProgressTimeout time.Duration }

func (p SnapshotPolicy) validate() error {
	if p == (SnapshotPolicy{}) {
		return nil
	}
	if p.TotalTimeout <= 0 || p.ProgressTimeout <= 0 || p.ProgressTimeout > p.TotalTimeout ||
		p.TotalTimeout > time.Duration(math.MaxInt64)-p.ProgressTimeout {
		return fault.New(fault.Invalid, "invalid snapshot observation deadlines", nil)
	}
	return nil
}

type snapshotEpisode struct {
	Destination metrics.ReplicaIdentity
	AnchorMatch uint64
	Deadline    time.Time
	Failed      bool
}

// All episode mutations run on the native owner loop. Workers only replace a
// bounded exact-attempt event or retain a terminal report under reportMu.
func (g *Group) consumeSnapshotProgressLocked(peer uint64, record *snapshotReportState) {
	if record.progress != nil && !record.progressConsumed {
		g.recordSnapshotEpisode(*record.progress)
		record.progressConsumed = true
	}
	if record.terminalTimeout {
		if episode := g.snapshotEpisodes[peer]; episode != nil {
			episode.Failed = true
		}
	}
}
func (g *Group) recordSnapshotEpisode(event snapshotProgressEvent) {
	record := g.snapshotReports[event.Attempt.ReplicaID]
	p := event.Progress
	if record == nil || event.Attempt != record.attempt || event.Attempt.RunID != g.nonce ||
		g.opts.SnapshotPolicy.TotalTimeout <= 0 ||
		!validSnapshotDestination(p.Destination, g.opts, event.Attempt.ReplicaID) {
		return
	}
	episode := g.snapshotEpisodes[event.Attempt.ReplicaID]
	fresh := episode == nil
	if episode != nil {
		fresh = record.lastMatch > episode.AnchorMatch ||
			!sameEpisodePeerIgnoringEngineRun(episode.Destination, p.Destination)
	}
	if fresh {
		if g.snapshotEpisodes == nil {
			g.snapshotEpisodes = make(map[uint64]*snapshotEpisode)
		}
		episode = &snapshotEpisode{Destination: p.Destination, AnchorMatch: record.lastMatch,
			Deadline: event.FirstAdmittedAttemptStarted.Add(
				g.opts.SnapshotPolicy.TotalTimeout + g.opts.SnapshotPolicy.ProgressTimeout)}
		g.snapshotEpisodes[event.Attempt.ReplicaID] = episode
	}
	// An engine-only receiver run change updates freshness, never the budget.
	episode.Destination = p.Destination
}
func (g *Group) observeNativeMatchAdvance(peer, match uint64) {
	if episode := g.snapshotEpisodes[peer]; episode != nil && match > episode.AnchorMatch {
		delete(g.snapshotEpisodes, peer)
	}
}
func validSnapshotDestination(id metrics.ReplicaIdentity, o Options, peer uint64) bool {
	return id.ReplicaID == peer && id.ClusterID == o.ClusterID && id.GroupID == o.GroupID &&
		id.NodeName != "" && id.InstanceID != "" && id.RunID != ""
}
func (g *Group) snapshotTransferMetrics(now time.Time, status raft.Status) map[uint64]*metrics.SnapshotCatchUp {
	if len(g.snapshotReports) == 0 {
		return nil
	}
	g.refreshSnapshotEpochStatus(status)
	g.snapshotReportMu.Lock()
	defer g.snapshotReportMu.Unlock()
	var samples map[uint64]*metrics.SnapshotCatchUp
	if status.RaftState != raft.StateLeader || status.Lead != g.opts.ReplicaID {
		return nil
	}
	for peer, record := range g.snapshotReports {
		p := g.snapshotCatchUpSample(now, record, status.Term)
		native, known := status.Progress[peer]
		if p == nil || !known || native.Match >= p.SnapshotIndex {
			continue
		}
		if samples == nil {
			samples = make(map[uint64]*metrics.SnapshotCatchUp)
		}
		samples[peer] = p
	}
	return samples
}
func (g *Group) snapshotCatchUpSample(
	now time.Time, record *snapshotReportState, term uint64,
) *metrics.SnapshotCatchUp {
	event := record.progress
	if event == nil || record.attempt.RunID != g.nonce || record.attempt.Term != term {
		return nil
	}
	episode := g.snapshotEpisodes[record.attempt.ReplicaID]
	if episode == nil {
		return nil
	}
	if record.terminal && record.terminalResult == raft.SnapshotFailure && !episode.Failed {
		return nil
	}
	p := event.Progress
	internalmetrics.AgeSnapshot(&p, internalmetrics.Age(now, event.ReceivedAt))
	p.EpisodeAnchorMatch = episode.AnchorMatch
	p.EpisodeRemaining = max(time.Duration(0), episode.Deadline.Sub(now))
	p.EpisodeFailed = episode.Failed
	if p.Phase == "handoff_grace" {
		grace := min(g.opts.SnapshotPolicy.ProgressTimeout,
			event.Progress.ProgressTimeout, event.Progress.RemainingGrace)
		remaining := max(time.Duration(0), event.ReceivedAt.Add(grace).Sub(now))
		p.RemainingGrace = min(p.RemainingGrace, remaining, p.RemainingTotal, p.EpisodeRemaining)
	}
	return &p
}
