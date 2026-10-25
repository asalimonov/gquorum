package loadsnapshot

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
)

func (c *Coordinator) findTransfer(key snapshot.Key) (*snapshotReceive, *snapshotTerminal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	terminal := c.terminal[key]
	if terminal != nil && !time.Now().Before(terminal.expires) {
		delete(c.terminal, key)
		terminal = nil
	}
	return c.incoming[key], terminal
}
func (*Coordinator) terminalAck(
	owner snapshotOwner, b snapshot.BeginRequest, t *snapshotTerminal,
) (snapshot.Ack, error) {
	if owner.key != t.owner.key {
		return snapshot.Ack{RestartNeeded: true}, transferError(snapshot.ReasonRestartRequired)
	}
	if !sameManifest(b.Manifest, t.manifest) {
		return snapshot.Ack{}, transferError(snapshot.ReasonTransferConflict)
	}
	if b.MaxChunkBytes != 0 && t.ack.ChunkBytes > b.MaxChunkBytes {
		return snapshot.Ack{}, transferError(snapshot.ReasonFrameLimit)
	}
	ack := t.ack
	ack.RemainingTotal = max(time.Nanosecond, time.Until(t.expires.Add(-ack.ProgressTimeout)))
	ack.RemainingTotal = min(ack.RemainingTotal, b.RemainingTotal)
	return ack, nil
}
func (c *Coordinator) resumeSnapshot(
	ctx context.Context, owner snapshotOwner, b snapshot.BeginRequest,
) (snapshot.Ack, error) {
	tr, terminal := c.findTransfer(b.Manifest.Key)
	if terminal != nil {
		return c.terminalAck(owner, b, terminal)
	}
	if tr == nil || tr.owner.key != owner.key {
		return snapshot.Ack{RestartNeeded: true}, transferError(snapshot.ReasonRestartRequired)
	}
	if !sameManifest(tr.manifest, b.Manifest) {
		return snapshot.Ack{}, transferError(snapshot.ReasonTransferConflict)
	}
	if b.MaxChunkBytes != 0 && tr.chunkBytes > b.MaxChunkBytes {
		return snapshot.Ack{}, transferError(snapshot.ReasonFrameLimit)
	}
	if err := tr.obsolete(); err != nil {
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	_, err := c.replaceAttachment(ctx, tr)
	if err != nil {
		return snapshot.Ack{}, err
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.ackLocked(time.Now()), nil
}
func (c *Coordinator) replaceAttachment(ctx context.Context, tr *snapshotReceive) (uint64, error) {
	tr.mu.Lock()
	if tr.resuming {
		tr.mu.Unlock()
		return 0, transferError(snapshot.ReasonGroupBusy)
	}
	if err := tr.expiredLocked(time.Now()); err != nil {
		tr.mu.Unlock()
		c.failReceive(tr, err)
		return 0, err
	}
	if tr.failed != nil {
		err := tr.failed
		tr.mu.Unlock()
		return 0, err
	}
	tr.resuming = true
	tr.attached.cancel()
	idle := tr.idle
	tr.mu.Unlock()
	defer func() { tr.mu.Lock(); tr.resuming = false; tr.mu.Unlock() }()
	select {
	case <-idle:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if err := c.revalidateSnapshotPeer(ctx, tr); err != nil {
		return 0, err
	}
	seq, err := c.attachAfterJoin(tr)
	if snapshotTimedOut(err) {
		c.failReceive(tr, err)
	}
	return seq, err
}

func (c *Coordinator) attachAfterJoin(tr *snapshotReceive) (uint64, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.failed != nil {
		return 0, tr.failed
	}
	if tr.ctx.Err() != nil {
		return 0, transferError(snapshot.ReasonStopped)
	}
	if err := tr.expiredLocked(time.Now()); err != nil {
		return 0, err
	}
	c.mu.Lock()
	if err := c.liveLocked(); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	seq, err := c.nextAttachmentLocked()
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	next, cancel := context.WithCancel(tr.ctx)
	tr.attachment = seq
	tr.attached = &snapshotAttachment{ctx: next, cancel: cancel}
	return seq, nil
}
func (c *Coordinator) preemptObsolete(ctx context.Context, owner snapshotOwner) error {
	c.mu.Lock()
	old := c.groups[owner.key.Peer.Replica.GroupID]
	c.mu.Unlock()
	if old == nil {
		return nil
	}
	old.mu.Lock()
	prehandoff := old.state == snapshot.DeliveryReceiving || old.state == snapshot.DeliveryVerifying
	old.mu.Unlock()
	if !prehandoff {
		return transferError(snapshot.ReasonGroupBusy)
	}
	if old.owner.key == owner.key {
		if err := old.obsolete(); err == nil {
			return transferError(snapshot.ReasonGroupBusy)
		}
	} else if old.owner.handle.Context().Err() == nil {
		return transferError(snapshot.ReasonGroupBusy)
	}
	c.failReceive(old, transferError(snapshot.ReasonObsolete))
	return awaitReceiveCleanup(ctx, old)
}

// SessionDone closes on attachment fencing, coordinator stop or owner loss.
func (c *Coordinator) SessionDone(session snapshot.Session) <-chan struct{} {
	tr, _ := c.findTransfer(session.Key)
	if tr == nil {
		return closedSessionSignal
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.attachment != session.Attachment || tr.attached == nil {
		return closedSessionSignal
	}
	return tr.attached.ctx.Done()
}

// DetachSnapshot fences and joins only the exact network attachment.
func (c *Coordinator) DetachSnapshot(ctx context.Context, session snapshot.Session) error {
	return c.endAttachment(ctx, session, detachAttachment)
}

// AbortSnapshot cancels the exact transfer and waits for owned operations.
func (c *Coordinator) AbortSnapshot(ctx context.Context, session snapshot.Session) error {
	return c.endAttachment(ctx, session, abortTransfer)
}

type snapshotAttachmentEnd uint8

const (
	detachAttachment snapshotAttachmentEnd = iota
	abortTransfer
)

func (c *Coordinator) endAttachment(ctx context.Context, session snapshot.Session, mode snapshotAttachmentEnd) error {
	op, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer op.close()
	tr, _ := c.findTransfer(session.Key)
	if tr == nil {
		return transferError(snapshot.ReasonRestartRequired)
	}
	if err := c.revalidateSnapshotPeer(op.ctx, tr); err != nil {
		return err
	}
	tr.mu.Lock()
	if tr.attachment != session.Attachment || tr.attached == nil || tr.resuming {
		tr.mu.Unlock()
		return transferError(snapshot.ReasonRestartRequired)
	}
	tr.attached.cancel()
	idle := tr.idle
	if mode == abortTransfer && tr.failed == nil {
		tr.failed = transferError(snapshot.ReasonStopped)
		tr.state = snapshot.DeliveryFailed
		tr.cancel()
	}
	tr.mu.Unlock()
	if mode == abortTransfer {
		c.failReceive(tr, transferError(snapshot.ReasonStopped))
		return awaitReceiveCleanup(op.ctx, tr)
	}
	select {
	case <-idle:
		return nil
	case <-op.ctx.Done():
		return op.ctx.Err()
	}
}
