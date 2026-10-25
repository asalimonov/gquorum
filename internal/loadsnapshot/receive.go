package loadsnapshot

import (
	"context"
	"crypto/sha256"
	"math"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

// BeginSnapshot authenticates and preflights before reserving/allocating data.
func (c *Coordinator) BeginSnapshot(ctx context.Context, b snapshot.BeginRequest) (snapshot.Ack, error) {
	op, err := c.beginOperation(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer op.close()
	validated, err := c.validateManifest(b)
	if err != nil {
		return snapshot.Ack{}, err
	}
	m, header := validated.manifest, validated.header
	b.Manifest = m
	owner, err := c.validateSnapshotPeer(op.ctx, m.Key)
	if err != nil {
		return snapshot.Ack{}, err
	}
	active, terminal := c.findTransfer(m.Key)
	if terminal != nil {
		return c.terminalAck(owner, b, terminal)
	}
	if active != nil {
		return c.resumeSnapshot(op.ctx, owner, b)
	}
	if b.Resume {
		return snapshot.Ack{RestartNeeded: true}, transferError(snapshot.ReasonRestartRequired)
	}
	return c.beginFresh(op.ctx, owner, b, header)
}
func (c *Coordinator) beginFresh(ctx context.Context, owner snapshotOwner, b snapshot.BeginRequest,
	header raftpb.Message) (snapshot.Ack, error) {
	m := b.Manifest
	preflight, err := owner.handle.PreflightSnapshot(ctx, header)
	if err != nil {
		return snapshot.Ack{}, snapshotPreflightError(err)
	}
	current, err := c.validateSnapshotPeer(ctx, m.Key)
	if err != nil {
		return snapshot.Ack{}, err
	}
	if current.key != owner.key {
		return snapshot.Ack{}, transferError(snapshot.ReasonRestartRequired)
	}
	if preflight.NotNeeded {
		if !preflight.HeaderStepped {
			return snapshot.Ack{}, transferError(snapshot.ReasonIngressFailed)
		}
		return c.notNeeded(owner, b, header)
	}
	if err := c.preemptObsolete(ctx, owner); err != nil {
		return snapshot.Ack{}, err
	}
	r, err := c.reserve(incoming, owner, m.TotalBytes)
	if err != nil {
		return snapshot.Ack{}, err
	}
	staging, err := newMemorySnapshotStaging(m.TotalBytes)
	if err != nil {
		c.release(r)
		return snapshot.Ack{}, err
	}
	tr := c.newReceive(b, header, staging, r)
	if err := c.revalidateSnapshotPeer(ctx, tr); err != nil {
		c.discardUnpublished(tr)
		return snapshot.Ack{}, err
	}
	ack, err := c.publishReceive(tr, b)
	if err != nil {
		c.discardUnpublished(tr)
	}
	return ack, err
}

func (c *Coordinator) newReceive(b snapshot.BeginRequest, header raftpb.Message,
	staging snapshotStaging, r *snapshotReservation) *snapshotReceive {
	owner := r.owner
	c.mu.Lock()
	lifetime := c.ctx
	c.mu.Unlock()
	now := time.Now()
	deadline := now.Add(min(c.options.TotalTimeout, b.RemainingTotal))
	ctx, cancel := context.WithDeadline(lifetime, deadline)
	tr := &snapshotReceive{key: b.Manifest.Key, manifest: b.Manifest, owner: owner, header: header, staging: staging,
		state: snapshot.DeliveryReceiving, digest: sha256.New(), started: now, progress: now,
		deadline:        deadline,
		progressTimeout: min(c.options.ProgressTimeout, b.ProgressTimeout),
		reservation:     r, ctx: ctx, cancel: cancel, idle: make(chan struct{}), cleaned: make(chan struct{})}
	close(tr.idle)
	tr.ownerCancelStop = context.AfterFunc(owner.handle.Context(), cancel)
	return tr
}
func (c *Coordinator) publishReceive(tr *snapshotReceive, b snapshot.BeginRequest) (snapshot.Ack, error) {
	ownerCtx := tr.owner.handle.Context()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.liveLocked(); err != nil {
		return snapshot.Ack{}, err
	}
	if tr.ctx.Err() != nil || ownerCtx.Err() != nil {
		return snapshot.Ack{}, transferError(snapshot.ReasonStopped)
	}
	seq, err := c.nextAttachmentLocked()
	if err != nil {
		return snapshot.Ack{}, err
	}
	size, err := c.negotiatedChunk(b)
	if err != nil {
		return snapshot.Ack{}, err
	}
	tr.chunkBytes = size

	tr.attachment = seq
	ctx, cancel := context.WithCancel(tr.ctx)
	tr.attached = &snapshotAttachment{ctx: ctx, cancel: cancel}
	id, err := c.nextObservationLocked()
	if err != nil {
		return snapshot.Ack{}, err
	}
	tr.statusID = id
	tr.receiverOutcome = snapshot.ReceiverUnknown
	c.incoming[tr.key] = tr
	c.groups[tr.key.To.Replica.GroupID] = tr
	return tr.ackLocked(time.Now()), nil
}
func (c *Coordinator) discardUnpublished(tr *snapshotReceive) {
	tr.cancel()
	tr.ownerCancelStop()
	if err := tr.staging.Close(); err == nil {
		c.release(tr.reservation)
	}
}
func (c *Coordinator) notNeeded(
	owner snapshotOwner, b snapshot.BeginRequest, header raftpb.Message,
) (snapshot.Ack, error) {
	ownerCtx := owner.handle.Context()
	now := time.Now()
	ack := snapshot.Ack{Session: snapshot.Session{Key: b.Manifest.Key}, State: snapshot.DeliveryNotNeeded,
		RemainingTotal:  min(c.options.TotalTimeout, b.RemainingTotal),
		ProgressTimeout: min(c.options.ProgressTimeout, b.ProgressTimeout), ReceiverRunID: owner.key.RunID}
	record := snapshotTerminal{owner: owner, manifest: b.Manifest, ack: ack, outcome: snapshot.ReceiverSkipped,
		expires: now.Add(ack.RemainingTotal).Add(ack.ProgressTimeout)}
	if err := c.cacheNotNeeded(ownerCtx, b.Manifest.Key, record); err != nil {
		return snapshot.Ack{}, err
	}
	c.observe(snapshot.Status{Key: b.Manifest.Key, Direction: "incoming",
		SnapshotIndex: header.Snapshot.Metadata.Index,
		SnapshotTerm:  header.Snapshot.Metadata.Term, TotalBytes: b.Manifest.TotalBytes,
		StartedAt: now, LastProgressAt: now,
		Deadline: now.Add(ack.RemainingTotal), Delivery: snapshot.DeliveryNotNeeded,
		ReceiverOutcome: snapshot.ReceiverSkipped})
	return ack, nil
}

func (c *Coordinator) cacheNotNeeded(ownerCtx context.Context, key snapshot.Key, record snapshotTerminal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.liveLocked(); err != nil {
		return err
	}
	if ownerCtx.Err() != nil {
		return transferError(snapshot.ReasonStopped)
	}
	c.putTerminalLocked(key, record)

	return nil
}

func (c *Coordinator) negotiatedChunk(b snapshot.BeginRequest) (uint32, error) {
	if c.options.ChunkBytes <= 0 {
		return 0, transferError(snapshot.ReasonFrameLimit)
	}
	capBytes := min(uint64(c.options.ChunkBytes), uint64(b.Manifest.ChunkBytes))
	if b.MaxChunkBytes != 0 {
		capBytes = min(capBytes, uint64(b.MaxChunkBytes))
	}
	if capBytes == 0 || capBytes > math.MaxUint32 {
		return 0, transferError(snapshot.ReasonFrameLimit)
	}
	return uint32(capBytes), nil
}
