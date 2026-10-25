package loadsnapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

const snapshotSendLifetimes = 3

const (
	snapshotJobPrepared uint32 = iota
	snapshotJobRunning
	snapshotJobDiscarded
	snapshotJobFinished
)

type snapshotSendJob struct {
	preparedTimer     *time.Timer
	stopPreparedEpoch func() bool
	c                 *Coordinator
	source            snapshotOwner
	native            raftpb.Message
	attempt           raftengine.SnapshotAttempt
	publication       uint64
	epoch             context.Context
	reservation       *snapshotReservation
	state             atomic.Uint32
	reported          atomic.Bool
	started, deadline time.Time
	statusID          uint64
	mu                sync.Mutex
	status            snapshot.Status
	to                load.Peer
	key               snapshot.Key
	callbackLive      bool
	receiverRun       string
	progressSequence  uint64
	lastProgress      time.Time
	progressTimeout   time.Duration
	networkDeadline   time.Time
	progressErr       error
	progressWake      chan struct{}
	cancelNetwork     context.CancelCauseFunc
}

// PrepareSend performs bounded admission and pins the native slice read-only.
// Resolution, encoding and hashing run only after native Ready durability.
func (c *Coordinator) PrepareSend(owner Owner, offer raftengine.SnapshotOffer) (raftengine.SnapshotJob, error) {
	c.mu.Lock()
	ports, err := c.ports, c.liveLocked()
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if ports.ResolvePeer == nil || ports.ValidateOutgoing == nil || ports.RegisterDelivery == nil ||
		ports.ReportDelivery == nil || ports.ReportSnapshotFailure == nil || ports.NoteSnapshotProgress == nil ||
		isNilTransport(ports.Transport) {
		return nil, transferError(snapshot.ReasonUnsupported)
	}
	source, err := validateSnapshotOffer(owner, offer)
	if err != nil {
		return nil, err
	}
	r, err := c.reserve(outgoing, source, uint64(len(offer.Message.Snapshot.Data)))
	if err != nil {
		return nil, err
	}
	j := c.newSendJob(source, offer, r)
	c.mu.Lock()
	err = c.liveLocked()
	if err == nil {
		j.statusID, err = c.nextObservationLocked()
	}
	if err == nil {
		c.outgoing[j] = struct{}{}
		c.digestOwners[source.key.RunID] = source.key
	}
	c.mu.Unlock()
	if err != nil {
		c.release(r)
		return nil, err
	}
	j.watchPrepared()
	return j, nil
}
func (c *Coordinator) newSendJob(
	source snapshotOwner, offer raftengine.SnapshotOffer, r *snapshotReservation,
) *snapshotSendJob {
	now := time.Now()
	j := &snapshotSendJob{c: c, source: source, native: offer.Message,
		attempt: offer.Attempt, publication: offer.PublicationGeneration,
		epoch: offer.Epoch, reservation: r, started: now, deadline: now.Add(c.options.TotalTimeout),
		progressWake: make(chan struct{}, 1)}
	j.status = snapshot.Status{Key: snapshot.Key{From: source.key.Peer, To: load.Peer{Replica: load.ReplicaRef{
		GroupID: source.key.Peer.Replica.GroupID, ReplicaID: offer.Message.To}}},
		Direction:     "outgoing",
		SnapshotIndex: offer.Message.Snapshot.Metadata.Index, SnapshotTerm: offer.Message.Snapshot.Metadata.Term,
		TotalBytes: r.bytes, ReservedBytes: r.bytes, StartedAt: now, LastProgressAt: now, Deadline: j.deadline,
		Delivery: snapshot.DeliveryVerifying, ReceiverOutcome: snapshot.ReceiverUnknown}
	native := *offer.Message.Snapshot
	j.native.Snapshot = &native
	return j
}

func validateSnapshotOffer(owner Owner, offer raftengine.SnapshotOffer) (snapshotOwner, error) {
	if isNilTransport(owner) {
		return snapshotOwner{}, transferError(snapshot.ReasonIdentity)
	}
	key := owner.Key()
	m := offer.Message
	if offer.Attempt.RunID != key.RunID || offer.Attempt.Generation == 0 || m.To == 0 ||
		offer.Attempt.Term != m.Term || offer.Attempt.ReplicaID != m.To || m.From != key.Peer.Replica.ReplicaID ||
		m.To == m.From || !validOwnerKey(key) {
		return snapshotOwner{}, transferError(snapshot.ReasonIdentity)
	}
	if offer.Epoch == nil || offer.Epoch.Err() != nil {
		return snapshotOwner{}, transferError(snapshot.ReasonStopped)
	}
	if m.Type != raftpb.MsgSnap || m.Term == 0 || offer.PublicationGeneration == 0 || m.Snapshot == nil ||
		len(m.Snapshot.Data) == 0 ||
		len(m.Entries) != 0 || len(m.Responses) != 0 {
		return snapshotOwner{}, transferError(snapshot.ReasonMalformed)
	}
	header := m
	ss := *m.Snapshot
	ss.Data = nil
	header.Snapshot = &ss
	if header.Size() > snapshot.MaxNativeHeaderBytes {
		return snapshotOwner{}, transferError(snapshot.ReasonFrameLimit)
	}
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || ss.Metadata.Term > m.Term {
		return snapshotOwner{}, transferError(snapshot.ReasonMalformed)
	}
	if err := validateSnapshotMembership(ss.Metadata.ConfState); err != nil {
		return snapshotOwner{}, err
	}
	return snapshotOwner{key: key, handle: owner}, nil
}

func (j *snapshotSendJob) Run(parent context.Context) {
	if !j.state.CompareAndSwap(snapshotJobPrepared, snapshotJobRunning) {
		return
	}
	j.stopPrepared()
	ctx, release := j.context(parent)
	defer release()
	ack, err := j.send(ctx)
	if err != nil {
		err = j.classifyError(ctx, err)
	}
	j.c.finishSnapshotDelivery(j, ack, err)
	j.state.Store(snapshotJobFinished)
}
func (j *snapshotSendJob) Discard(err error) {
	if !j.state.CompareAndSwap(snapshotJobPrepared, snapshotJobDiscarded) {
		return
	}
	cause := err
	if cause == nil {
		cause = transferError(snapshot.ReasonStopped)
	}
	j.c.finishSnapshotDelivery(j, snapshot.Ack{}, cause)
}
func (j *snapshotSendJob) context(parent context.Context) (context.Context, func()) {
	bounded, cancelDeadline := context.WithDeadline(parent, j.deadline)
	ctx, cancel := context.WithCancelCause(bounded)
	stops := make([]func() bool, 0, snapshotSendLifetimes)
	for _, lifetime := range []context.Context{j.source.handle.Context(), j.epoch, j.c.ctx} {
		stops = append(stops, context.AfterFunc(lifetime, func() { cancel(transferError(snapshot.ReasonStopped)) }))
		if lifetime.Err() != nil {
			cancel(transferError(snapshot.ReasonStopped))
		}
	}
	return ctx, func() {
		for _, stop := range stops {
			stop()
		}
		cancel(nil)
		cancelDeadline()
	}
}
func (j *snapshotSendJob) send(ctx context.Context) (snapshot.Ack, error) {
	if err := ctx.Err(); err != nil {
		return snapshot.Ack{}, err
	}
	to, err := j.c.ports.ResolvePeer(ctx, j.source.handle, j.native.To)
	if err != nil {
		return snapshot.Ack{}, err
	}
	if to.Replica.ReplicaID != j.native.To || to.Replica.GroupID != j.source.key.Peer.Replica.GroupID ||
		to.Node.ClusterID != j.source.key.Peer.Node.ClusterID || !validSnapshotPeer(to) {
		return snapshot.Ack{}, transferError(snapshot.ReasonIdentity)
	}
	if err := j.c.ports.ValidateOutgoing(ctx, j.source.handle, to); err != nil {
		return snapshot.Ack{}, err
	}
	id := SourceSnapshotID{RunID: j.source.key.RunID, Generation: j.publication, Index: j.status.SnapshotIndex,
		Term: j.status.SnapshotTerm, Bytes: uint64(len(j.native.Snapshot.Data))}
	sum, err := j.c.snapshotDigest(ctx, id, j.native.Snapshot.Data)
	if err != nil {
		return snapshot.Ack{}, err
	}
	if err := j.validateCurrent(ctx, to); err != nil {
		return snapshot.Ack{}, err
	}
	send, err := j.prepareNetworkSend(to, sum)
	if err != nil {
		return snapshot.Ack{}, err
	}
	ack, err := j.sendNetwork(ctx, send)
	if err != nil {
		return ack, err
	}
	if err := j.validateCurrent(ctx, to); err != nil {
		return ack, err
	}
	if err := j.validateFinalAck(ack); err != nil {
		return ack, err
	}
	j.noteFinalAck(ack)
	return ack, nil
}
func (j *snapshotSendJob) prepareNetworkSend(to load.Peer, sum [sha256.Size]byte) (snapshot.Send, error) {
	delivery, err := j.c.ports.RegisterDelivery(j.source.handle, j.attempt, to, j.deadline)
	if err != nil {
		return snapshot.Send{}, err
	}
	if delivery == 0 {
		return snapshot.Send{}, transferError(snapshot.ReasonTransferConflict)
	}
	j.key = snapshot.Key{From: j.source.key.Peer, To: to, DeliveryID: delivery}
	j.mu.Lock()
	j.to = to
	j.status.Key = j.key
	j.status.Delivery = snapshot.DeliveryReceiving
	j.mu.Unlock()
	message := j.native
	native := *message.Snapshot
	native.Data = nil
	message.Snapshot = &native
	header, err := message.Marshal()
	if err != nil {
		return snapshot.Send{}, err
	}
	chunkBytes, err := j.c.negotiatedChunk(snapshot.BeginRequest{
		Manifest: snapshot.Manifest{ChunkBytes: math.MaxUint32}})
	if err != nil {
		return snapshot.Send{}, err
	}
	manifest := snapshot.Manifest{Key: j.key, NativeHeader: header,
		TotalBytes: uint64(len(j.native.Snapshot.Data)), SHA256: sum, ChunkBytes: chunkBytes}
	return snapshot.Send{Manifest: manifest, Data: j.native.Snapshot.Data,
		ReportProgress: j.recordVerifiedProgress}, nil
}

func (j *snapshotSendJob) validateCurrent(ctx context.Context, to load.Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j.source.handle.Key() != j.source.key || j.source.handle.Context().Err() != nil || j.epoch.Err() != nil {
		return transferError(snapshot.ReasonStopped)
	}
	return j.c.ports.ValidateOutgoing(ctx, j.source.handle, to)
}
func (j *snapshotSendJob) validateFinalAck(ack snapshot.Ack) error {
	if ack.Session.Key != j.key || ack.RestartNeeded || ack.RemainingTotal <= 0 || ack.ProgressTimeout <= 0 ||
		ack.ProgressTimeout > j.c.options.ProgressTimeout || !boundedSnapshotText(ack.ReceiverRunID) {
		return transferError(snapshot.ReasonMalformed)
	}
	j.mu.Lock()
	run := j.receiverRun
	j.mu.Unlock()
	if run != "" && run != ack.ReceiverRunID {
		return transferError(snapshot.ReasonRestartRequired)
	}
	switch ack.State {
	case snapshot.DeliveryNotNeeded:
		if ack.NextOffset != 0 {
			return transferError(snapshot.ReasonMalformed)
		}
	case snapshot.DeliveryAccepted:
		limit := j.c.options.ChunkBytes
		if limit <= 0 {
			return transferError(snapshot.ReasonMalformed)
		}
		if ack.NextOffset != j.status.TotalBytes || ack.Session.Attachment == 0 || ack.ChunkBytes == 0 ||
			uint64(ack.ChunkBytes) > uint64(limit) {
			return transferError(snapshot.ReasonMalformed)
		}
	default:
		return transferError(snapshot.ReasonMalformed)
	}
	return nil
}
func (j *snapshotSendJob) classifyError(ctx context.Context, err error) error {
	j.mu.Lock()
	progressErr := j.progressErr
	j.mu.Unlock()
	if progressErr != nil {
		return progressErr
	}
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		return err
	}
	if !time.Now().Before(j.deadline) {
		return &snapshot.TransferError{Reason: snapshot.ReasonTotalTimeout, Cause: err}
	}
	if cause := context.Cause(ctx); cause != nil {
		if errors.As(cause, &detail) {
			return cause
		}
		return &snapshot.TransferError{Reason: snapshot.ReasonStopped, Cause: cause}
	}
	return err
}

func (c *Coordinator) finishSnapshotDelivery(j *snapshotSendJob, ack snapshot.Ack, err error) {
	if !j.reported.CompareAndSwap(false, true) {
		return
	}
	c.workerStarted(j.source.key)
	defer c.workerReturned(j.source.key)
	j.stopPrepared()
	reason := snapshotErrorReason(err)
	j.mu.Lock()
	j.callbackLive = false
	if err != nil {
		j.status.Delivery = snapshot.DeliveryFailed
		j.status.ErrorReason = reason
	} else {
		j.status.Delivery = ack.State
		if ack.State == snapshot.DeliveryAccepted {
			j.status.VerifiedBytes = j.status.TotalBytes
		}
	}
	status := j.status
	status.ReservedBytes = 0
	j.mu.Unlock()
	if j.key.DeliveryID != 0 {
		c.ports.ReportDelivery(j.source.handle, j.key, ack, err)
	} else {
		c.ports.ReportSnapshotFailure(j.source.handle, j.attempt, err)
	}
	j.mu.Lock()
	j.native = raftpb.Message{}
	j.source.handle = nil
	j.cancelNetwork = nil
	j.mu.Unlock()
	c.mu.Lock()
	delete(c.outgoing, j)
	c.mu.Unlock()
	c.release(j.reservation)
	c.observeTransfer(j.statusID, status)
}
func snapshotErrorReason(err error) string {
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		return detail.Reason
	}
	if errors.Is(err, fault.ErrOverloaded) {
		return snapshot.ReasonCapacity
	}
	return snapshot.ReasonIngressFailed
}

func (j *snapshotSendJob) watchPrepared() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state.Load() != snapshotJobPrepared {
		return
	}
	j.preparedTimer = time.AfterFunc(max(time.Duration(0), time.Until(j.deadline)), func() {
		j.Discard(transferError(snapshot.ReasonTotalTimeout))
	})
	j.stopPreparedEpoch = context.AfterFunc(j.epoch, func() { j.Discard(transferError(snapshot.ReasonStopped)) })
}
func (j *snapshotSendJob) stopPrepared() {
	j.mu.Lock()
	timer, stop := j.preparedTimer, j.stopPreparedEpoch
	j.preparedTimer = nil
	j.stopPreparedEpoch = nil
	j.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stop != nil {
		stop()
	}
}
