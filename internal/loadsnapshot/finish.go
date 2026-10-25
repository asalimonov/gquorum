package loadsnapshot

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load/snapshot"
)

// FinishSnapshot claims one finalizer and waits only for native acceptance.
// The finalizer and native callback retain ownership after this waiter leaves.
func (c *Coordinator) FinishSnapshot(ctx context.Context, session snapshot.Session) (snapshot.Ack, error) {
	op, err := c.beginOperation(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer op.close()
	owner, err := c.validateSnapshotPeer(op.ctx, session.Key)
	if err != nil {
		return snapshot.Ack{}, err
	}
	tr, terminal := c.findTransfer(session.Key)
	if terminal != nil {
		if terminal.owner.key != owner.key || terminal.ack.Session.Attachment != session.Attachment {
			return snapshot.Ack{}, transferError(snapshot.ReasonRestartRequired)
		}
		return terminal.ack, nil
	}
	if tr == nil || tr.owner.key != owner.key {
		return snapshot.Ack{}, transferError(snapshot.ReasonRestartRequired)
	}
	tr.mu.Lock()
	if tr.attachment != session.Attachment || tr.attached == nil || tr.attached.ctx.Err() != nil {
		tr.mu.Unlock()
		return snapshot.Ack{}, transferError(snapshot.ReasonRestartRequired)
	}
	if tr.completion != nil {
		tr.mu.Unlock()
		return c.awaitSnapshotAcceptance(op.ctx, tr)
	}
	if tr.failed != nil {
		err := tr.failed
		tr.mu.Unlock()
		return snapshot.Ack{}, err
	}
	if tr.active != 0 || tr.resuming {
		tr.mu.Unlock()
		return snapshot.Ack{}, transferError(snapshot.ReasonGroupBusy)
	}
	if err := tr.expiredLocked(time.Now()); err != nil {
		tr.mu.Unlock()
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	if tr.next != tr.manifest.TotalBytes {
		tr.mu.Unlock()
		err := transferError(snapshot.ReasonMalformed)
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	tr.completion = &snapshotCompletion{reply: make(chan struct{}),
		nativeDone: make(chan struct{}), finalizerDone: make(chan struct{})}
	tr.state = snapshot.DeliveryVerifying
	tr.mu.Unlock()
	c.startOwnerWorker(tr.owner.key, func() { c.finalizeSnapshot(tr) })
	return c.awaitSnapshotAcceptance(op.ctx, tr)
}

func (c *Coordinator) finalizeSnapshot(tr *snapshotReceive) {
	defer close(tr.completion.finalizerDone)
	defer c.cleanupCompletedSnapshot(tr)
	if err := c.prepareAndStepSnapshot(tr); err != nil {
		err = c.finalizationError(tr, err)
		tr.mu.Lock()
		completion := tr.completion
		if !completion.ingressCalled {
			completion.err = err
			tr.failed = err
			tr.state = snapshot.DeliveryFailed
			tr.receiverOutcome = snapshot.ReceiverFailed
			completion.replyOnce.Do(func() { close(completion.reply) })
		} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// StepOwnedSnapshot invokes Done on every definite refusal. The native
			// callback supplies the definitive outcome; an ended waiter never does.
			if completion.err == nil && !completion.accepted {
				completion.err = err
			}
		}
		tr.mu.Unlock()
	}
}
func (c *Coordinator) prepareAndStepSnapshot(tr *snapshotReceive) error {
	if subtle.ConstantTimeCompare(tr.digest.Sum(nil), tr.manifest.SHA256[:]) != 1 {
		return transferError(snapshot.ReasonChecksum)
	}
	if err := tr.staging.Seal(tr.ctx); err != nil {
		return err
	}
	data, err := tr.staging.Materialize(tr.ctx)
	if err != nil {
		return err
	}
	if err := c.revalidateSnapshotPeer(tr.ctx, tr); err != nil {
		return err
	}
	message := tr.header
	native := *message.Snapshot
	native.Data = data
	message.Snapshot = &native
	decoded, err := tr.owner.handle.PrepareOwnedSnapshot(tr.ctx, message)
	if err != nil {
		return err
	}
	if err := c.revalidateSnapshotPeer(tr.ctx, tr); err != nil {
		return err
	}
	tr.mu.Lock()
	if tr.failed != nil {
		err := tr.failed
		tr.mu.Unlock()
		return err
	}
	if err := tr.ctx.Err(); err != nil {
		tr.mu.Unlock()
		return err
	}
	tr.state = snapshot.DeliveryHandingOff
	tr.completion.ingressCalled = true
	tr.mu.Unlock()
	err = tr.owner.handle.StepOwnedSnapshot(tr.ctx, raftengine.SnapshotIngress{Message: message, Decoded: decoded,
		Done: func(outcome raftengine.SnapshotInstallOutcome, err error) {
			c.completeReceivedSnapshot(tr, outcome, err)
		}})
	tr.mu.Lock()
	completion := tr.completion
	completion.stepReturned = true
	completion.stepErr = err
	if err == nil {
		completion.accepted = true
	}
	c.publishSnapshotAcceptanceLocked(tr)
	tr.mu.Unlock()
	return err
}

// completeReceivedSnapshot only records/signals; it never closes staging or
// joins a finalizer from the native owner callback.
func (*Coordinator) completeReceivedSnapshot(
	tr *snapshotReceive, outcome raftengine.SnapshotInstallOutcome, err error,
) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	completion := tr.completion
	completion.nativeOnce.Do(func() {
		completion.outcome = outcome
		completion.err = err
		tr.receiverOutcome = installedOutcome(outcome, err)
		if tr.receiverOutcome == snapshot.ReceiverInstalled || tr.receiverOutcome == snapshot.ReceiverSkipped {
			completion.accepted = true
		} else if completion.err == nil {
			completion.err = transferError(snapshot.ReasonIngressFailed)
		}
		// A failed Done can race a still-returning successful Step. Publish failure
		// only after Step itself returned an error; installed/skipped proves acceptance.
		if completion.accepted {
			tr.state = snapshot.DeliveryAccepted
			completion.replyOnce.Do(func() { close(completion.reply) })
		}
		if !completion.accepted && completion.stepReturned && completion.stepErr != nil {
			tr.state = snapshot.DeliveryFailed
			tr.failed = completion.err
			completion.replyOnce.Do(func() { close(completion.reply) })
		}
		close(completion.nativeDone)
	})
}
func (*Coordinator) awaitSnapshotAcceptance(ctx context.Context, tr *snapshotReceive) (snapshot.Ack, error) {
	tr.mu.Lock()
	reply := tr.completion.reply
	tr.mu.Unlock()
	select {
	case <-reply:
	case <-ctx.Done():
		return snapshot.Ack{}, ctx.Err()
	}
	tr.mu.Lock()
	if tr.completion.accepted {
		ack := tr.ackLocked(time.Now())
		ack.State = snapshot.DeliveryAccepted
		tr.mu.Unlock()
		return ack, nil
	}
	err := tr.completion.err
	tr.mu.Unlock()
	if err == nil {
		err = transferError(snapshot.ReasonIngressFailed)
	}
	return snapshot.Ack{}, nativeIngressError(err)
}

func installedOutcome(outcome raftengine.SnapshotInstallOutcome, err error) snapshot.ReceiverOutcome {
	if err != nil {
		return snapshot.ReceiverFailed
	}
	switch outcome {
	case raftengine.SnapshotInstalled:
		return snapshot.ReceiverInstalled
	case raftengine.SnapshotSkipped:
		return snapshot.ReceiverSkipped
	default:
		return snapshot.ReceiverFailed
	}
}

func (*Coordinator) publishSnapshotAcceptanceLocked(tr *snapshotReceive) {
	completion := tr.completion
	if completion.accepted {
		tr.state = snapshot.DeliveryAccepted
		completion.replyOnce.Do(func() { close(completion.reply) })
		return
	}
	select {
	case <-completion.nativeDone:
		tr.state = snapshot.DeliveryFailed
		tr.failed = completion.err
		completion.replyOnce.Do(func() { close(completion.reply) })
	default: // A canceled enqueued waiter is still owned by the engine.
	}
}

func nativeIngressError(err error) error {
	if err == nil {
		return nil
	}
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		return err
	}
	reason := snapshot.ReasonIngressFailed
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, fault.ErrStopped):
		reason = snapshot.ReasonStopped
	case errors.Is(err, fault.ErrUnsupported):
		reason = snapshot.ReasonUnsupported
	case errors.Is(err, fault.ErrIdentityMismatch):
		reason = snapshot.ReasonIdentity
	case errors.Is(err, fault.ErrInvalid):
		reason = snapshot.ReasonMalformed
	default:
	}
	return &snapshot.TransferError{Reason: reason, Cause: err}
}
func (*Coordinator) finalizationError(tr *snapshotReceive, err error) error {
	if errors.Is(err, context.DeadlineExceeded) && !time.Now().Before(tr.deadline) {
		return &snapshot.TransferError{Reason: snapshot.ReasonTotalTimeout, Cause: err}
	}
	return nativeIngressError(err)
}
