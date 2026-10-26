package loadsnapshot

import (
	"context"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
)

const minimumSnapshotOperations = 8

type coordinatorOperation struct {
	c      *Coordinator
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
	once   sync.Once
}

func (c *Coordinator) beginOperation(parent context.Context) (*coordinatorOperation, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if err := c.liveLocked(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.operations >= max(minimumSnapshotOperations, 2*c.options.MaxIncoming) {
		c.mu.Unlock()
		return nil, transferError(snapshot.ReasonCapacity)
	}
	lifetime := c.ctx
	c.operations++
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return &coordinatorOperation{c: c, ctx: ctx, cancel: cancel, stop: stop}, nil
}
func (op *coordinatorOperation) close() {
	op.once.Do(func() {
		op.stop()
		op.cancel()
		op.c.mu.Lock()
		op.c.operations--
		op.c.signalLocked()
		op.c.mu.Unlock()
	})
}

func (tr *snapshotReceive) pin(session snapshot.Session) (*snapshotAttachment, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.failed != nil {
		return nil, tr.failed
	}
	if tr.ctx.Err() != nil {
		return nil, transferError(snapshot.ReasonStopped)
	}
	if tr.attachment != session.Attachment || tr.attached == nil || tr.attached.ctx.Err() != nil {
		return nil, transferError(snapshot.ReasonRestartRequired)
	}
	if tr.cleanupRunning || tr.active != 0 || tr.resuming {
		return nil, transferError(snapshot.ReasonGroupBusy)
	}
	tr.active = 1
	tr.idle = make(chan struct{})
	return tr.attached, nil
}
func (tr *snapshotReceive) unpin() {
	tr.mu.Lock()
	tr.active--
	if tr.active == 0 {
		close(tr.idle)
	}
	tr.mu.Unlock()
}
func (op *coordinatorOperation) bindAttachment(attachment *snapshotAttachment) func() bool {
	stop := context.AfterFunc(attachment.ctx, op.cancel)
	if attachment.ctx.Err() != nil {
		op.cancel()
	}
	return stop
}
func (tr *snapshotReceive) ackLocked(now time.Time) snapshot.Ack {
	return snapshot.Ack{Session: snapshot.Session{Key: tr.key, Attachment: tr.attachment}, NextOffset: tr.next,
		ChunkBytes: tr.chunkBytes, State: tr.state, RemainingTotal: max(time.Nanosecond, tr.deadline.Sub(now)),
		ProgressTimeout: tr.progressTimeout, ReceiverRunID: tr.owner.key.RunID}
}

func awaitReceiveCleanup(ctx context.Context, tr *snapshotReceive) error {
	select {
	case <-tr.cleaned:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
