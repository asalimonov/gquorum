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
	if tr.active != 0 || tr.resuming {
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
func bindAttachment(parent context.Context, attachment *snapshotAttachment) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(attachment.ctx, cancel)
	if attachment.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}
func (tr *snapshotReceive) ackLocked(now time.Time) snapshot.Ack {
	return snapshot.Ack{Session: snapshot.Session{Key: tr.key, Attachment: tr.attachment}, NextOffset: tr.next,
		ChunkBytes: tr.chunkBytes, State: tr.state, RemainingTotal: max(time.Nanosecond, tr.deadline.Sub(now)),
		ProgressTimeout: tr.progressTimeout, ReceiverRunID: tr.owner.key.RunID}
}

// failReceive fences first, then joins mutation on a separate cleanup worker.
// The caller may still own a pin; it never waits for its own pin to drain.
func (c *Coordinator) failReceive(tr *snapshotReceive, cause error) {
	tr.mu.Lock()
	if tr.failed == nil {
		tr.failed = cause
		tr.state = snapshot.DeliveryFailed
	}
	tr.cancel()
	if tr.attached != nil {
		tr.attached.cancel()
	}
	idle := tr.idle
	tr.mu.Unlock()
	tr.cleanupOnce.Do(func() {
		c.mu.Lock()
		c.operations++
		c.mu.Unlock()
		go func() {
			defer func() { c.mu.Lock(); c.operations--; c.signalLocked(); c.mu.Unlock() }()
			<-idle
			if err := tr.staging.Close(); err != nil {
				return
			} // Keep the reservation if backend release did not finish.
			tr.ownerCancelStop()
			c.mu.Lock()
			if c.incoming[tr.key] == tr {
				delete(c.incoming, tr.key)
			}
			if c.groups[tr.key.To.Replica.GroupID] == tr {
				delete(c.groups, tr.key.To.Replica.GroupID)
			}
			c.mu.Unlock()
			c.release(tr.reservation)
			close(tr.cleaned)
		}()
	})
}
func awaitReceiveCleanup(ctx context.Context, tr *snapshotReceive) error {
	select {
	case <-tr.cleaned:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
