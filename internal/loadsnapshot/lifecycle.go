package loadsnapshot

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
)

const snapshotExpiryTick = 100 * time.Millisecond

func (c *Coordinator) startOwnerWorker(owner OwnerKey, work func()) {
	c.mu.Lock()
	c.workers++
	if owner.RunID != "" {
		c.ownerWorkers[owner]++
	}
	c.mu.Unlock()
	go func() { defer c.workerReturned(owner); work() }()
}
func (c *Coordinator) workerReturned(owner OwnerKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers--
	if owner.RunID != "" {
		c.ownerWorkers[owner]--
		if c.ownerWorkers[owner] == 0 {
			delete(c.ownerWorkers, owner)
		}
	}
	c.signalLocked()
}
func (c *Coordinator) expiryLoop() {
	ticker := time.NewTicker(snapshotExpiryTick)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.cancelReceives(OwnerKey{})
			return
		case now := <-ticker.C:
			c.expire(now)
		}
	}
}
func (c *Coordinator) activeReceives(owner OwnerKey) []*snapshotReceive {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*snapshotReceive, 0, len(c.incoming))
	for _, tr := range c.incoming {
		if owner.RunID == "" || tr.owner.key == owner {
			result = append(result, tr)
		}
	}
	return result
}
func (c *Coordinator) cancelReceives(owner OwnerKey) {
	for _, tr := range c.activeReceives(owner) {
		c.cleanupReceive(tr, transferError(snapshot.ReasonStopped))
	}
}
func (c *Coordinator) expire(now time.Time) {
	for _, tr := range c.activeReceives(OwnerKey{}) {
		tr.mu.Lock()
		err := tr.expiredLocked(now)
		if err == nil && tr.ctx.Err() != nil {
			err = transferError(snapshot.ReasonStopped)
			if errors.Is(tr.ctx.Err(), context.DeadlineExceeded) {
				err = transferError(snapshot.ReasonTotalTimeout)
			}
		}
		tr.mu.Unlock()
		if err != nil {
			c.cleanupReceive(tr, err)
		}
	}
	c.mu.Lock()
	for key, record := range c.terminal {
		if !now.Before(record.expires) {
			delete(c.terminal, key)
		}
	}
	c.mu.Unlock()
}

// StopOwner requires the managed owner to request engine stop first. It joins
// exact-run transfers before that owner's application/storage can be released.
func (c *Coordinator) StopOwner(ctx context.Context, owner OwnerKey) error {
	c.mu.Lock()
	c.fencedOwners[owner] = struct{}{}
	c.mu.Unlock()
	c.cancelReceives(owner)
	if err := c.joinSnapshotOwnership(ctx, owner); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.fencedOwners, owner)
	for key, record := range c.terminal {
		if record.owner.key == owner {
			delete(c.terminal, key)
		}
	}
	c.clearDigestsLocked(owner.RunID)
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) joinSnapshotOwnership(ctx context.Context, owner OwnerKey) error {
	for {
		c.mu.Lock()
		joined := c.snapshotOwnershipJoinedLocked(owner)
		if joined {
			c.mu.Unlock()
			return nil
		}
		c.waiters++
		changed := c.changed
		c.mu.Unlock()
		err := c.snapshotCleanupError(owner)
		if err == nil {
			select {
			case <-changed:
			case <-ctx.Done():
				err = fault.New(fault.ShutdownTimeout, "snapshot ownership has not joined", ctx.Err())
			}
		}
		c.mu.Lock()
		c.waiters--
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (c *Coordinator) failReceive(tr *snapshotReceive, cause error) { c.cleanupReceive(tr, cause) }
func (c *Coordinator) cleanupReceive(tr *snapshotReceive, cause error) {
	tr.mu.Lock()
	tr.fenceLocked(cause)
	tr.mu.Unlock()
	c.cleanupCompletedSnapshot(tr)
}
func (c *Coordinator) cleanupCompletedSnapshot(tr *snapshotReceive) {
	tr.mu.Lock()
	select {
	case <-tr.cleaned:
		tr.mu.Unlock()
		return
	default:
	}
	if tr.cleanupRunning {
		tr.mu.Unlock()
		return
	}
	tr.cleanupRunning = true
	tr.cleanupErr = nil
	idle, completion := tr.idle, tr.completion
	tr.mu.Unlock()
	c.startOwnerWorker(tr.owner.key, func() { c.runReceiveCleanup(tr, idle, completion) })
}
func (c *Coordinator) runReceiveCleanup(tr *snapshotReceive, idle <-chan struct{}, completion *snapshotCompletion) {
	defer func() {
		tr.mu.Lock()
		tr.cleanupRunning = false
		tr.mu.Unlock()
		c.mu.Lock()
		c.signalLocked()
		c.mu.Unlock()
	}()
	<-idle
	if completion != nil {
		<-completion.finalizerDone
		tr.mu.Lock()
		ingress := completion.ingressCalled
		tr.mu.Unlock()
		if ingress {
			<-completion.nativeDone
		}
		tr.mu.Lock()
		raw := completion.err
		tr.mu.Unlock()
		normalized := nativeIngressError(raw)
		tr.mu.Lock()
		completion.err = normalized
		tr.mu.Unlock()
	}
	if err := tr.staging.Close(); err != nil {
		tr.mu.Lock()
		tr.cleanupErr = err
		tr.mu.Unlock()
		return
	}
	c.releaseCompletedReceive(tr, completion)
}

func (c *Coordinator) releaseCompletedReceive(tr *snapshotReceive, completion *snapshotCompletion) {
	tr.cancel()
	tr.ownerCancelStop()
	tr.mu.Lock()
	ack := tr.ackLocked(time.Now())
	accepted := completion != nil && completion.accepted
	if accepted {
		ack.State = snapshot.DeliveryAccepted
	}
	status := tr.statusLocked()
	status.ReservedBytes = 0
	tr.mu.Unlock()
	if accepted {
		c.retainTerminal(tr, ack)
	}
	c.mu.Lock()
	if c.incoming[tr.key] == tr {
		delete(c.incoming, tr.key)
	}
	if c.groups[tr.key.To.Replica.GroupID] == tr {
		delete(c.groups, tr.key.To.Replica.GroupID)
	}
	c.mu.Unlock()
	c.release(tr.reservation)
	c.observeTransfer(tr.statusID, status)
	close(tr.cleaned)
}

func (c *Coordinator) retainTerminal(tr *snapshotReceive, ack snapshot.Ack) {
	if ack.State != snapshot.DeliveryAccepted && ack.State != snapshot.DeliveryNotNeeded {
		return
	}
	tr.mu.Lock()
	record := snapshotTerminal{owner: tr.owner, manifest: tr.manifest, ack: ack, outcome: tr.receiverOutcome,
		expires: tr.deadline.Add(tr.progressTimeout)}
	tr.mu.Unlock()
	c.mu.Lock()
	c.putTerminalLocked(tr.key, record)
	c.mu.Unlock()
}

func (tr *snapshotReceive) fenceLocked(cause error) {
	select {
	case <-tr.cleaned:
		return
	default:
	}
	if tr.failed == nil {
		tr.failed = cause
	}
	if tr.completion == nil || !tr.completion.accepted {
		tr.state = snapshot.DeliveryFailed
	}
	if tr.completion == nil || !tr.completion.ingressCalled {
		tr.receiverOutcome = snapshot.ReceiverFailed
	}
	tr.cancel()
	if tr.attached != nil {
		tr.attached.cancel()
	}
}

func (c *Coordinator) snapshotOwnershipJoinedLocked(owner OwnerKey) bool {
	if owner.RunID == "" {
		return c.incomingCount == 0 && c.outgoingCount == 0 && c.operations == 0 && c.workers == 0
	}
	if c.ownerWorkers[owner] != 0 {
		return false
	}
	for r := range c.reservations {
		if r.owner.key == owner {
			return false
		}
	}
	return true
}
func (c *Coordinator) snapshotCleanupError(owner OwnerKey) error {
	for _, tr := range c.activeReceives(owner) {
		tr.mu.Lock()
		err, running := tr.cleanupErr, tr.cleanupRunning
		tr.mu.Unlock()
		if err != nil && !running {
			return fault.New(fault.Unavailable, "snapshot staging release failed", err)
		}
	}
	return nil
}
