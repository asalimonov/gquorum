package raftengine

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
)

const (
	snapshotRetryMinimum   = 100 * time.Millisecond
	snapshotRetryMaximum   = time.Second
	snapshotFailureMaximum = 30 * time.Second
)

type snapshotFailureKind uint8

const (
	snapshotAdmittedFailure snapshotFailureKind = iota
	snapshotCapacityRefusal
	snapshotPermanentRefusal
	snapshotLegacyReport
)

type snapshotReportState struct {
	destination metrics.ReplicaIdentity
	attempt     SnapshotAttempt
	pending     *snapshotAttemptReport
	terminal    bool
	backoff     time.Duration
	lastMatch   uint64
	started     time.Time
	progress    *snapshotProgressEvent
}
type snapshotAttemptReport struct {
	attempt SnapshotAttempt
	result  raft.SnapshotStatus
	due     time.Time
}
type snapshotProgressEvent struct {
	Attempt                     SnapshotAttempt
	Progress                    metrics.SnapshotCatchUp
	ReceivedAt                  time.Time
	FirstAdmittedAttemptStarted time.Time
}

// ReportSnapshotAttempt retains a terminal result before signaling the loop.
// A saturated wake signal cannot lose a report. The first exact result wins.
func (g *Group) ReportSnapshotAttempt(a SnapshotAttempt, result raft.SnapshotStatus) {
	g.recordSnapshotReport(a, result, snapshotAdmittedFailure)
}

// ReportSnapshotAttemptError supplies failure classification absent from the
// legacy status-only API. Overloaded means pre-admission capacity/group-busy;
// Invalid/Unsupported are permanent protocol or policy refusals.
func (g *Group) ReportSnapshotAttemptError(a SnapshotAttempt, err error) {
	kind := snapshotAdmittedFailure
	switch {
	case errors.Is(err, fault.ErrOverloaded):
		kind = snapshotCapacityRefusal
	case errors.Is(err, fault.ErrInvalid), errors.Is(err, fault.ErrUnsupported):
		kind = snapshotPermanentRefusal
	default:
	}
	g.recordSnapshotReport(a, raft.SnapshotFailure, kind)
}

func (g *Group) recordSnapshotReport(a SnapshotAttempt, result raft.SnapshotStatus, kind snapshotFailureKind) {
	g.snapshotReportMu.Lock()
	record := g.snapshotReports[a.ReplicaID]
	if g.snapshotReportsClosed || a.Generation == 0 || a.RunID != g.nonce || record == nil ||
		record.attempt != a || record.terminal {
		g.snapshotReportMu.Unlock()
		return
	}
	record.terminal = true
	now := time.Now()
	due := now
	if result == raft.SnapshotFailure {
		due = due.Add(snapshotFailureDelay(record, kind))
	}
	record.pending = &snapshotAttemptReport{attempt: a, result: result, due: due}
	g.snapshotReportMu.Unlock()
	g.wakeSnapshotReports()
}

func snapshotFailureDelay(record *snapshotReportState, kind snapshotFailureKind) time.Duration {
	switch kind {
	case snapshotLegacyReport:
		return 0
	case snapshotCapacityRefusal:
		// Resource retry jitter is metadata-only; no payload or permit is retained.
		//nolint:gosec // Retry jitter is not security-sensitive.
		return snapshotRetryMinimum + time.Duration(rand.Int64N(int64(snapshotRetryMaximum-snapshotRetryMinimum)+1))
	case snapshotPermanentRefusal:
		record.backoff = snapshotFailureMaximum
	default:
		record.backoff = min(snapshotFailureMaximum, max(time.Second, record.backoff*2))
	}
	return record.backoff
}

func (g *Group) wakeSnapshotReports() {
	select {
	case g.snapshotReportWake <- struct{}{}:
	default:
	}
}

// NoteSnapshotProgress stores a bounded last update for the current attempt.
// It does not refresh heartbeat evidence or reset failed-attempt backoff.
func (g *Group) NoteSnapshotProgress(a SnapshotAttempt, p metrics.SnapshotCatchUp) {
	g.snapshotReportMu.Lock()
	record := g.snapshotReports[a.ReplicaID]
	if g.snapshotReportsClosed || record == nil || record.attempt != a || record.terminal ||
		a.Generation == 0 || a.RunID != g.nonce {
		g.snapshotReportMu.Unlock()
		return
	}
	if p.TotalBytes == 0 || p.VerifiedBytes > p.TotalBytes || p.Destination.ReplicaID != a.ReplicaID {
		g.snapshotReportMu.Unlock()
		return
	}
	if old := record.progress; old != nil && (p.ProgressSequence <= old.Progress.ProgressSequence ||
		p.VerifiedBytes < old.Progress.VerifiedBytes || p.TotalBytes != old.Progress.TotalBytes) {
		g.snapshotReportMu.Unlock()
		return
	}
	if p.Destination.InstanceID != "" && p.Destination.RunID != "" {
		if record.destination.InstanceID != "" && !sameEpisodePeerIgnoringEngineRun(record.destination, p.Destination) {
			record.backoff = 0
		}
		record.destination = p.Destination
	}
	p.EpisodeAnchorMatch, p.EpisodeRemaining, p.EpisodeFailed = 0, 0, false
	record.progress = &snapshotProgressEvent{Attempt: a, Progress: p, ReceivedAt: time.Now(),
		FirstAdmittedAttemptStarted: record.started}
	g.snapshotReportMu.Unlock()
	g.wakeSnapshotReports()
}

func (g *Group) drainSnapshotReports(now time.Time) {
	g.refreshSnapshotEpoch()
	g.snapshotReportMu.Lock()
	defer g.snapshotReportMu.Unlock()
	for _, record := range g.snapshotReports {
		report := record.pending
		if report == nil || report.due.After(now) {
			continue
		}
		record.pending = nil
		if report.attempt == record.attempt && report.attempt.RunID == g.nonce &&
			report.attempt.Term == g.snapshotEpochTerm && g.snapshotEpoch != nil {
			g.rn.ReportSnapshot(report.attempt.ReplicaID, report.result)
		}
	}
}

func (g *Group) closeSnapshotReportsAfterJoin() {
	g.snapshotReportMu.Lock()
	g.snapshotReportsClosed = true
	clear(g.snapshotReports)
	g.snapshotReportMu.Unlock()
}

func (g *Group) snapshotReportDeadline(state *loopState) <-chan time.Time {
	g.snapshotReportMu.Lock()
	var due time.Time
	for _, record := range g.snapshotReports {
		if record.pending != nil && (due.IsZero() || record.pending.due.Before(due)) {
			due = record.pending.due
		}
	}
	g.snapshotReportMu.Unlock()
	if due.IsZero() {
		if state.snapshotReportTimer != nil {
			state.snapshotReportTimer.Stop()
			state.snapshotReportTimer = nil
		}
		return nil
	}
	wait := max(time.Duration(0), time.Until(due))
	if state.snapshotReportTimer == nil {
		state.snapshotReportTimer = time.NewTimer(wait)
	} else {
		state.snapshotReportTimer.Reset(wait)
	}
	return state.snapshotReportTimer.C
}

func sameEpisodePeerIgnoringEngineRun(a, b metrics.ReplicaIdentity) bool {
	return a.ClusterID == b.ClusterID && a.GroupID == b.GroupID && a.NodeName == b.NodeName &&
		a.ReplicaID == b.ReplicaID && a.InstanceID == b.InstanceID
}
