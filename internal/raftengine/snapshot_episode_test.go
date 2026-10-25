//go:build !functional

package raftengine

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
)

func episodeFixture() (*Group, *snapshotReportState, snapshotProgressEvent) {
	started := time.Unix(100, 0)
	attempt := SnapshotAttempt{RunID: "source", Term: 3, ReplicaID: 2, Generation: 1}
	destination := metrics.ReplicaIdentity{ClusterID: "cluster", GroupID: "work", NodeName: "two", ReplicaID: 2, InstanceID: "process", RunID: "receiver-1"}
	record := &snapshotReportState{attempt: attempt, lastMatch: 10, started: started}
	event := snapshotProgressEvent{Attempt: attempt, ReceivedAt: started, FirstAdmittedAttemptStarted: started,
		Progress: metrics.SnapshotCatchUp{DeliveryID: 1, SnapshotIndex: 100, SnapshotTerm: 2, LeaderTerm: 3, Destination: destination,
			VerifiedBytes: 1, TotalBytes: 100, ProgressSequence: 1, ProgressTimeout: 2 * time.Second, RemainingTotal: 10 * time.Second, Phase: "sending"}}
	g := &Group{nonce: "source", opts: Options{ClusterID: "cluster", GroupID: "work", ReplicaID: 1,
		SnapshotPolicy: SnapshotPolicy{TotalTimeout: 10 * time.Second, ProgressTimeout: 2 * time.Second}}, snapshotReports: map[uint64]*snapshotReportState{2: record}}
	return g, record, event
}
func TestSnapshotEpisodeRetriesTermsAndReceiverRunsCannotRenew(t *testing.T) {
	t.Parallel()
	g, record, event := episodeFixture()
	g.recordSnapshotEpisode(event)
	original := *g.snapshotEpisodes[2]
	for i := uint64(2); i < 8; i++ {
		record.attempt.Generation = i
		record.attempt.Term++
		event.Attempt = record.attempt
		event.Progress.LeaderTerm = record.attempt.Term
		event.FirstAdmittedAttemptStarted = event.FirstAdmittedAttemptStarted.Add(time.Second)
		event.Progress.DeliveryID = i
		event.Progress.Destination.RunID = "new-receiver-run"
		event.Progress.EpisodeFailed = false
		event.Progress.EpisodeRemaining = time.Hour
		event.Progress.EpisodeAnchorMatch = 999
		g.recordSnapshotEpisode(event)
		episode := g.snapshotEpisodes[2]
		if episode.Deadline != original.Deadline || episode.AnchorMatch != 10 || episode.Destination.RunID != "new-receiver-run" {
			t.Fatal("renewed run/term/delivery changed fixed episode", episode)
		}
	}
	record.terminalTimeout = true
	g.consumeSnapshotProgressLocked(2, record)
	if !g.snapshotEpisodes[2].Failed {
		t.Fatal("terminal timeout did not latch")
	}
	g.recordSnapshotEpisode(event)
	if !g.snapshotEpisodes[2].Failed {
		t.Fatal("byte progress cleared failure latch")
	}
	g.observeNativeMatchAdvance(2, 10)
	if g.snapshotEpisodes[2] == nil {
		t.Fatal("equal Match renewed episode")
	}
	g.observeNativeMatchAdvance(2, 11)
	if g.snapshotEpisodes[2] != nil {
		t.Fatal("native Match advance did not retire episode")
	}
	record.lastMatch = 11
	record.terminalTimeout = false
	g.recordSnapshotEpisode(event)
	if g.snapshotEpisodes[2].Failed || g.snapshotEpisodes[2].AnchorMatch != 11 {
		t.Fatal(g.snapshotEpisodes[2])
	}
}
func TestSnapshotEpisodeNewProcessRenewsUnknownRunDoesNot(t *testing.T) {
	t.Parallel()
	g, record, event := episodeFixture()
	event.Progress.Destination.RunID = ""
	g.recordSnapshotEpisode(event)
	if len(g.snapshotEpisodes) != 0 {
		t.Fatal("unknown run granted an episode")
	}
	event.Progress.Destination.RunID = "known"
	g.recordSnapshotEpisode(event)
	original := g.snapshotEpisodes[2].Deadline
	g.snapshotEpisodes[2].Failed = true
	event.Progress.Destination.InstanceID = "new-process"
	event.FirstAdmittedAttemptStarted = event.FirstAdmittedAttemptStarted.Add(time.Second)
	g.recordSnapshotEpisode(event)
	if g.snapshotEpisodes[2].Failed || !g.snapshotEpisodes[2].Deadline.After(original) {
		t.Fatal("new process retained old failure")
	}
	record.progress = &event
	record.terminalTimeout = true
	g.consumeSnapshotProgressLocked(2, record)
	p := g.snapshotCatchUpSample(event.ReceivedAt, record, record.attempt.Term)
	if p == nil || !p.EpisodeFailed {
		t.Fatal("timeout failure evidence absent", p)
	}
}
func TestSnapshotEpisodeAgesProgressAndBoundsHandoffGrace(t *testing.T) {
	t.Parallel()
	g, record, event := episodeFixture()
	g.recordSnapshotEpisode(event)
	event.Progress.Phase = "handoff_grace"
	event.Progress.VerifiedBytes = 100
	event.Progress.RemainingGrace = time.Hour // callback cannot extend local grace
	event.Progress.RemainingTotal = time.Second
	record.progress = &event
	record.terminal = true
	record.terminalResult = raft.SnapshotFinish
	p := g.snapshotCatchUpSample(event.ReceivedAt.Add(500*time.Millisecond), record, 3)
	if p == nil || p.RemainingGrace != 500*time.Millisecond || p.ProgressAge != 500*time.Millisecond || p.EpisodeRemaining != 11500*time.Millisecond {
		t.Fatal(p)
	}
	p = g.snapshotCatchUpSample(event.ReceivedAt.Add(3*time.Second), record, 3)
	if p.RemainingGrace != 0 || p.RemainingTotal != 0 {
		t.Fatal("expired grace renewed", p)
	}
	record.terminalResult = raft.SnapshotFailure
	if p = g.snapshotCatchUpSample(event.ReceivedAt, record, 3); p != nil {
		t.Fatal("failed transfer retained protection", p)
	}
}

func TestSnapshotProgressMailboxCannotManufactureRaftAcknowledgements(t *testing.T) {
	t.Parallel()
	g, record, event := episodeFixture()
	g.raftSample = metrics.ReplicaSample{Raft: metrics.Stamp{Sequence: 9}, Progress: []metrics.PeerProgress{{ReplicaID: 2, Match: 10, AckSequence: 7}}}
	g.NoteSnapshotProgress(record.attempt, event.Progress)
	if g.raftSample.Raft.Sequence != 9 || g.raftSample.Progress[0].AckSequence != 7 || g.raftSample.Progress[0].RecentActive ||
		g.raftSample.Progress[0].Match != 10 || g.raftSample.Progress[0].Snapshot != nil {
		t.Fatal("worker refreshed native evidence", g.raftSample)
	}
}

func TestSnapshotPolicyKeepsLegacyZeroAndRejectsUnsafeDurations(t *testing.T) {
	t.Parallel()
	if err := (SnapshotPolicy{}).validate(); err != nil {
		t.Fatal("legacy zero policy refused", err)
	}
	for _, policy := range []SnapshotPolicy{{TotalTimeout: time.Second}, {ProgressTimeout: time.Second},
		{TotalTimeout: time.Second, ProgressTimeout: 2 * time.Second}, {TotalTimeout: time.Duration(1<<63 - 1), ProgressTimeout: time.Second}} {
		if err := policy.validate(); err == nil {
			t.Fatal("unsafe episode deadline accepted", policy)
		}
	}
}
