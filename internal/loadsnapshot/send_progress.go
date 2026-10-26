package loadsnapshot

import (
	"context"
	"math"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/metrics"
)

type snapshotProgressSource uint8

const (
	snapshotTransportProgress snapshotProgressSource = iota
	snapshotFinalProgress
)

func (j *snapshotSendJob) sendNetwork(parent context.Context, send snapshot.Send) (snapshot.Ack, error) {
	if err := parent.Err(); err != nil {
		return snapshot.Ack{}, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop, done := make(chan struct{}), make(chan struct{})
	now := time.Now()
	j.mu.Lock()
	j.callbackLive = true
	j.cancelNetwork = cancel
	j.lastProgress = now
	j.progressTimeout = j.c.options.ProgressTimeout
	j.networkDeadline = j.deadline
	j.mu.Unlock()
	go func() { defer close(done); j.monitorNetwork(ctx, cancel, stop) }()
	ack, err := j.c.ports.Transport.SendSnapshot(ctx, send)
	j.mu.Lock()
	j.callbackLive = false
	j.mu.Unlock()
	close(stop)
	<-done
	j.mu.Lock()
	progressErr := j.progressErr
	j.mu.Unlock()
	cause := context.Cause(ctx)
	cancel(nil)
	if progressErr != nil {
		return ack, progressErr
	}
	if cause != nil {
		return ack, cause
	}
	return ack, err
}

func (j *snapshotSendJob) monitorNetwork(ctx context.Context, cancel context.CancelCauseFunc, stop <-chan struct{}) {
	timer := time.NewTimer(j.c.options.ProgressTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-j.progressWake:
		case <-timer.C:
		}
		j.mu.Lock()
		if !j.callbackLive || j.progressErr != nil {
			j.mu.Unlock()
			return
		}
		now := time.Now()
		due := j.networkDeadline
		reason := snapshot.ReasonTotalTimeout
		if j.status.VerifiedBytes < j.status.TotalBytes {
			progressDue := j.lastProgress.Add(j.progressTimeout)
			if progressDue.Before(due) {
				due = progressDue
				reason = snapshot.ReasonProgressTimeout
			}
		}
		if !now.Before(due) {
			err := transferError(reason)
			j.progressErr = err
			j.mu.Unlock()
			cancel(err)
			return
		}
		wait := due.Sub(now)
		j.mu.Unlock()
		timer.Reset(wait)
	}
}
func (j *snapshotSendJob) recordVerifiedProgress(p snapshot.Progress) {
	j.recordProgress(p, snapshotTransportProgress)
}
func (j *snapshotSendJob) noteFinalAck(ack snapshot.Ack) {
	j.recordProgress(snapshot.Progress{ReceiverRunID: ack.ReceiverRunID, VerifiedBytes: ack.NextOffset,
		State: ack.State, RemainingTotal: ack.RemainingTotal,
		ProgressTimeout: ack.ProgressTimeout}, snapshotFinalProgress)
}
func (j *snapshotSendJob) recordProgress(p snapshot.Progress, source snapshotProgressSource) {
	now := time.Now()
	j.mu.Lock()
	if source == snapshotTransportProgress && !j.callbackLive || j.progressErr != nil {
		j.mu.Unlock()
		return
	}
	update, err := j.updateProgressLocked(p, now)
	if err != nil {
		j.progressErr = err
		cancel := j.cancelNetwork
		j.mu.Unlock()
		if cancel != nil {
			cancel(err)
		}
		return
	}
	owner, attempt, port := j.source.handle, j.attempt, j.c.ports.NoteSnapshotProgress
	j.mu.Unlock()
	select {
	case j.progressWake <- struct{}{}:
	default:
	}
	// The private managed port only enqueues exact-owner evidence, without I/O.
	if update.changed && port != nil {
		port(owner, attempt, update.progress)
	}
}

type snapshotProgressUpdate struct {
	progress metrics.SnapshotCatchUp
	changed  bool
}

func (j *snapshotSendJob) updateProgressLocked(p snapshot.Progress, now time.Time) (snapshotProgressUpdate, error) {
	if err := j.validateProgressLocked(p); err != nil {
		return snapshotProgressUpdate{}, err
	}
	first := j.receiverRun == ""
	advanced := p.VerifiedBytes > j.status.VerifiedBytes
	phaseChanged := p.State != j.status.Delivery &&
		(p.State == snapshot.DeliveryAccepted || p.State == snapshot.DeliveryNotNeeded)
	if first {
		j.receiverRun = p.ReceiverRunID
	}
	j.progressTimeout = min(j.progressTimeout, p.ProgressTimeout)
	allowance := min(p.RemainingTotal, max(time.Duration(0), time.Until(j.deadline)))
	j.networkDeadline = minTime(j.networkDeadline, now.Add(allowance))
	if !first && !advanced && !phaseChanged {
		return snapshotProgressUpdate{}, nil
	}
	if j.progressSequence == math.MaxUint64 {
		return snapshotProgressUpdate{}, transferError(snapshot.ReasonTransferConflict)
	}
	j.progressSequence++
	if advanced {
		j.lastProgress = now
		j.status.LastProgressAt = now
	}
	j.status.VerifiedBytes = p.VerifiedBytes
	j.status.Delivery = p.State
	return snapshotProgressUpdate{progress: j.progressSampleLocked(p, now), changed: true}, nil
}
func (j *snapshotSendJob) progressSampleLocked(p snapshot.Progress, now time.Time) metrics.SnapshotCatchUp {
	phase := "sending"
	grace := time.Duration(0)
	if p.State == snapshot.DeliveryAccepted || p.State == snapshot.DeliveryNotNeeded {
		phase = "handoff_grace"
		grace = j.progressTimeout
	}
	to := j.to
	return metrics.SnapshotCatchUp{DeliveryID: j.key.DeliveryID, SnapshotIndex: j.status.SnapshotIndex,
		SnapshotTerm: j.status.SnapshotTerm, LeaderTerm: j.attempt.Term,
		Destination: metrics.ReplicaIdentity{ClusterID: to.Node.ClusterID,
			GroupID: to.Replica.GroupID, NodeName: to.Node.NodeName,
			ReplicaID: to.Replica.ReplicaID, InstanceID: to.Node.InstanceID, RunID: p.ReceiverRunID},
		VerifiedBytes: p.VerifiedBytes, TotalBytes: j.status.TotalBytes, ProgressSequence: j.progressSequence,
		ProgressAge: now.Sub(j.lastProgress), ProgressTimeout: j.progressTimeout,
		RemainingTotal: max(time.Duration(0), j.networkDeadline.Sub(now)), Phase: phase, RemainingGrace: grace}
}
func (j *snapshotSendJob) validateProgressLocked(p snapshot.Progress) error {
	if p.RemainingTotal <= 0 || p.ProgressTimeout <= 0 || p.ProgressTimeout > j.c.options.ProgressTimeout ||
		p.VerifiedBytes > j.status.TotalBytes || p.VerifiedBytes < j.status.VerifiedBytes ||
		!boundedSnapshotText(p.ReceiverRunID) {
		return transferError(snapshot.ReasonMalformed)
	}
	if j.receiverRun != "" && j.receiverRun != p.ReceiverRunID {
		return transferError(snapshot.ReasonRestartRequired)
	}
	switch p.State {
	case snapshot.DeliveryReceiving:
	case snapshot.DeliveryVerifying, snapshot.DeliveryHandingOff, snapshot.DeliveryAccepted:
		if p.VerifiedBytes != j.status.TotalBytes {
			return transferError(snapshot.ReasonMalformed)
		}
	case snapshot.DeliveryNotNeeded:
		if p.VerifiedBytes != 0 || j.status.VerifiedBytes != 0 {
			return transferError(snapshot.ReasonMalformed)
		}
	default:
		return transferError(snapshot.ReasonMalformed)
	}
	return nil
}
func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
