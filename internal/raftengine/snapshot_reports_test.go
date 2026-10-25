//go:build !functional

package raftengine

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
)

func snapshotMailbox(t *testing.T, count int) *Group {
	t.Helper()
	g := &Group{nonce: "run", snapshotReports: make(map[uint64]*snapshotReportState), snapshotReportWake: make(chan struct{}, 1)}
	for i := 1; i <= count; i++ {
		id := uint64(i)
		g.snapshotReports[id] = &snapshotReportState{attempt: SnapshotAttempt{RunID: "run", Term: 2, ReplicaID: id, Generation: id}, started: time.Now()}
	}
	return g
}
func TestSnapshotReportsWakeSaturationAndExactAttempt(t *testing.T) {
	t.Parallel()
	g := snapshotMailbox(t, 128)
	g.snapshotReportWake <- struct{}{}
	var wg sync.WaitGroup
	for _, record := range g.snapshotReports {
		a := record.attempt
		wg.Go(func() {
			g.ReportSnapshotAttempt(a, raft.SnapshotFinish)
			g.ReportSnapshotAttempt(a, raft.SnapshotFailure)
		})
	}
	wg.Wait()
	if len(g.snapshotReportWake) != 1 {
		t.Fatal("wake capacity changed")
	}
	for _, record := range g.snapshotReports {
		if record.pending == nil || record.pending.result != raft.SnapshotFinish {
			t.Fatal("lost or overwritten terminal report")
		}
	}
	a := g.snapshotReports[1].attempt
	for _, bad := range []SnapshotAttempt{{RunID: "old", Term: 2, ReplicaID: 1, Generation: 1}, {RunID: "run", Term: 1, ReplicaID: 1, Generation: 1}, {RunID: "run", Term: 2, ReplicaID: 1, Generation: 2}, {RunID: "run", Term: 2, ReplicaID: 999, Generation: 1}} {
		g.ReportSnapshotAttempt(bad, raft.SnapshotFailure)
	}
	if g.snapshotReports[1].pending.attempt != a || len(g.snapshotReports) != 128 {
		t.Fatal("stale report changed mailbox")
	}
	g.closeSnapshotReportsAfterJoin()
	g.ReportSnapshotAttempt(a, raft.SnapshotFailure)
	if len(g.snapshotReports) != 0 {
		t.Fatal("late report reopened stopped run")
	}
}
func TestSnapshotReportsBackoffAndProgress(t *testing.T) {
	t.Parallel()
	record := &snapshotReportState{}
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if got := snapshotFailureDelay(record, snapshotAdmittedFailure); got != want {
			t.Fatal(got, want)
		}
	}
	for range 100 {
		if delay := snapshotFailureDelay(record, snapshotCapacityRefusal); delay < 100*time.Millisecond || delay > time.Second {
			t.Fatal(delay)
		}
	}
	if record.backoff != 30*time.Second {
		t.Fatal("capacity refusal modified remote backoff")
	}
	if delay := snapshotFailureDelay(&snapshotReportState{}, snapshotPermanentRefusal); delay != 30*time.Second {
		t.Fatal(delay)
	}
	g := snapshotMailbox(t, 1)
	record = g.snapshotReports[1]
	record.backoff = 8 * time.Second
	p := metrics.SnapshotCatchUp{ProgressSequence: 1, VerifiedBytes: 8, TotalBytes: 100, Destination: metrics.ReplicaIdentity{ReplicaID: 1}, EpisodeFailed: true, EpisodeRemaining: time.Hour}
	g.NoteSnapshotProgress(record.attempt, p)
	if record.progress == nil || record.progress.Progress.EpisodeFailed || record.progress.Progress.EpisodeRemaining != 0 || record.backoff != 8*time.Second {
		t.Fatal(record)
	}
	if record.progress.ReceivedAt.Before(record.started) || record.progress.FirstAdmittedAttemptStarted != record.started {
		t.Fatal("not locally anchored")
	}
	p.VerifiedBytes = 1
	p.ProgressSequence = 2
	g.NoteSnapshotProgress(record.attempt, p)
	if record.progress.Progress.VerifiedBytes != 8 {
		t.Fatal("progress regressed")
	}
	g.ReportSnapshotAttemptError(record.attempt, errors.Join(fault.ErrOverloaded, errors.New("busy")))
	delay := time.Until(record.pending.due)
	if delay <= 0 || delay > time.Second {
		t.Fatal(delay)
	}
	p.VerifiedBytes = 20
	p.ProgressSequence = 3
	g.NoteSnapshotProgress(record.attempt, p)
	if record.progress.Progress.VerifiedBytes != 8 {
		t.Fatal("terminal attempt accepted progress")
	}
}
func TestSnapshotReportsDeadlineIndependentOfRaftTick(t *testing.T) {
	t.Parallel()
	g := snapshotMailbox(t, 1)
	state := &loopState{}
	defer state.stopReadyTimer()
	if g.snapshotReportDeadline(state) != nil {
		t.Fatal("idle timer")
	}
	g.ReportSnapshotAttemptError(g.snapshotReports[1].attempt, fault.ErrOverloaded)
	timer := g.snapshotReportDeadline(state)
	if timer == nil {
		t.Fatal("no cooldown timer")
	}
	g.snapshotReports[1].pending.due = time.Now().Add(-time.Second)
	timer = g.snapshotReportDeadline(state)
	select {
	case <-timer:
	case <-time.After(time.Second):
		t.Fatal("due report waiting for raft tick")
	}
	g.snapshotReports[1].pending = nil
	if g.snapshotReportDeadline(state) != nil || state.snapshotReportTimer != nil {
		t.Fatal("timer not released")
	}
}

func TestSnapshotReportsPeerRestartVersusEngineRestart(t *testing.T) {
	t.Parallel()
	g := snapshotMailbox(t, 1)
	record := g.snapshotReports[1]
	p := metrics.SnapshotCatchUp{TotalBytes: 100, VerifiedBytes: 1, ProgressSequence: 1,
		Destination: metrics.ReplicaIdentity{ClusterID: "cluster", GroupID: "load", NodeName: "peer", ReplicaID: 1, InstanceID: "process1", RunID: "engine1"}}
	g.NoteSnapshotProgress(record.attempt, p)
	record.backoff = 8 * time.Second
	p.Destination.RunID = "engine2"
	p.ProgressSequence++
	g.NoteSnapshotProgress(record.attempt, p)
	if record.backoff != 8*time.Second {
		t.Fatal("engine-only restart reset backoff")
	}
	p.Destination.InstanceID = "process2"
	p.ProgressSequence++
	g.NoteSnapshotProgress(record.attempt, p)
	if record.backoff != 0 {
		t.Fatal("new peer process did not reset backoff")
	}
}
