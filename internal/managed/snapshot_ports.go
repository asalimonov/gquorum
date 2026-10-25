package managed

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadsnapshot"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type managedSnapshotOwner struct {
	host *Host
	slot *replica
	core *raftengine.Group
	key  loadsnapshot.OwnerKey
	ctx  context.Context
}

func (o *managedSnapshotOwner) Key() loadsnapshot.OwnerKey { return o.key }
func (o *managedSnapshotOwner) Context() context.Context   { return o.ctx }
func (o *managedSnapshotOwner) SnapshotBounds() raftengine.SnapshotBounds {
	return o.core.SnapshotBounds()
}
func (o *managedSnapshotOwner) PreflightSnapshot(
	ctx context.Context, m raftpb.Message,
) (raftengine.SnapshotPreflight, error) {
	if err := o.host.validateSnapshotOwner(ctx, o); err != nil {
		return raftengine.SnapshotPreflight{}, err
	}
	result, err := o.core.PreflightSnapshot(ctx, m)
	if err == nil {
		err = o.host.validateSnapshotOwner(ctx, o)
	}
	return result, err
}
func (o *managedSnapshotOwner) PrepareOwnedSnapshot(ctx context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
	if err := o.host.validateSnapshotOwner(ctx, o); err != nil {
		return raftstate.Snapshot{}, err
	}
	result, err := o.core.PrepareOwnedSnapshot(ctx, m)
	if err == nil {
		err = o.host.validateSnapshotOwner(ctx, o)
	}
	return result, err
}
func (o *managedSnapshotOwner) StepOwnedSnapshot(ctx context.Context, in raftengine.SnapshotIngress) error {
	if err := o.host.validateSnapshotOwner(ctx, o); err != nil {
		if in.Done != nil {
			in.Done(raftengine.SnapshotInstallFailed, err)
		}
		return err
	}
	// Accepted native ownership cannot be undone by a later publication check.
	return o.core.StepOwnedSnapshot(ctx, in)
}

// SnapshotCoordinator is the exact Host-owned transfer mechanism. Its lifetime
// starts with this Host and never redirects into a replacement Host.
func (h *Host) SnapshotCoordinator() *loadsnapshot.Coordinator { return h.snapshots }
func (h *Host) snapshotPorts() loadsnapshot.Ports {
	var transport snapshot.Transport
	if capable, ok := h.cfg.Transport.(snapshot.Transport); ok {
		transport = capable
	}
	return loadsnapshot.Ports{Authorize: h.authorizeSnapshot, ResolvePeer: h.resolveSnapshotPeer,
		ValidateOutgoing: h.validateOutgoingSnapshot, RegisterDelivery: h.registerSnapshotDelivery,
		ReportDelivery: h.reportSnapshotDelivery, ReportSnapshotFailure: h.reportSnapshotFailure,
		NoteSnapshotProgress: h.noteSnapshotProgress, Transport: transport}
}
func (h *Host) snapshotOwnerLocked(slot *replica) (*managedSnapshotOwner, error) {
	if !h.started || h.quiescing || h.stopped || h.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if slot == nil || slot.core == nil || slot.released || slot.state == "stopping" ||
		slot.ctx.Err() != nil || !slot.core.Running() {
		return nil, fault.ErrReplicaNotReady
	}
	return &managedSnapshotOwner{host: h, slot: slot, core: slot.core,
		key: loadsnapshot.OwnerKey{Peer: localPeer(slot), RunID: slot.core.RunID()}, ctx: slot.core.RunContext()}, nil
}
func (h *Host) exactSnapshotOwnerLocked(o *managedSnapshotOwner) bool {
	if o == nil || o.host != h || o.slot == nil || o.core == nil {
		return false
	}
	return !o.slot.released && o.slot.core == o.core && h.slots[o.key.Peer.Replica.GroupID] == o.slot &&
		localPeer(o.slot) == o.key.Peer && o.core.RunID() == o.key.RunID
}
func (h *Host) validateSnapshotOwner(ctx context.Context, o *managedSnapshotOwner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	if !h.exactSnapshotOwnerLocked(o) {
		h.mu.Unlock()
		return fault.ErrStopped
	}
	valid := h.started && !h.quiescing && !h.stopped && o.slot.state != "stopping" &&
		o.slot.ctx.Err() == nil && o.core.Running()
	h.mu.Unlock()
	if !valid {
		return fault.ErrStopped
	}
	if h.cfg.Identity() != o.key.Peer.Node {
		return fault.ErrIdentityMismatch
	}
	return ctx.Err()
}
func (h *Host) authorizeSnapshot(ctx context.Context, key snapshot.Key) (loadsnapshot.Owner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key.DeliveryID == 0 || key.From.Replica.GroupID != key.To.Replica.GroupID ||
		key.From.Node.ClusterID != key.To.Node.ClusterID || !completePeer(key.From) || !completePeer(key.To) {
		return nil, fault.ErrIdentityMismatch
	}
	if h.cfg.Identity() != key.To.Node {
		return nil, fault.ErrIdentityMismatch
	}
	// Node composition supplies applied process/load identity indexes here. These
	// exact lookups perform no topology scan, remote query or cached attestation.
	if err := h.cfg.ValidatePeer(ctx, key.From); err != nil {
		return nil, err
	}
	if err := h.cfg.ValidatePeer(ctx, key.To); err != nil {
		return nil, err
	}
	h.mu.Lock()
	owner, err := h.snapshotOwnerLocked(h.slots[key.To.Replica.GroupID])
	if err == nil && owner.key.Peer != key.To {
		err = fault.ErrIdentityMismatch
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err = h.validateSnapshotOwner(ctx, owner); err != nil {
		return nil, err
	}
	return owner, nil
}
func (h *Host) resolveSnapshotPeer(ctx context.Context, owner loadsnapshot.Owner, id uint64) (load.Peer, error) {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	if err := h.validateSnapshotOwner(ctx, o); err != nil {
		return load.Peer{}, err
	}
	peer, err := h.cfg.ResolvePeer(ctx, load.ReplicaRef{GroupID: o.key.Peer.Replica.GroupID, ReplicaID: id})
	if err == nil {
		err = h.validateOutgoingSnapshot(ctx, o, peer)
	}
	return peer, err
}
func (h *Host) validateOutgoingSnapshot(ctx context.Context, owner loadsnapshot.Owner, to load.Peer) error {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil {
		return fault.ErrIdentityMismatch
	}
	if err := h.validateSnapshotOwner(ctx, o); err != nil {
		return err
	}
	if to.Replica.GroupID != o.key.Peer.Replica.GroupID ||
		to.Node.ClusterID != o.key.Peer.Node.ClusterID || !completePeer(to) {
		return fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidatePeer(ctx, to); err != nil {
		return err
	}
	if err := h.cfg.ValidatePeer(ctx, o.key.Peer); err != nil {
		return err
	}
	return h.validateSnapshotOwner(ctx, o)
}
func (h *Host) registerSnapshotDelivery(owner loadsnapshot.Owner, attempt raftengine.SnapshotAttempt,
	to load.Peer, deadline time.Time) (uint64, error) {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil || o.host != h || attempt.RunID != o.key.RunID || attempt.ReplicaID != to.Replica.ReplicaID {
		return 0, fault.ErrIdentityMismatch
	}
	if !time.Now().Before(deadline) {
		return 0, &snapshot.TransferError{Reason: snapshot.ReasonTotalTimeout}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.quiescing || h.stopped || !h.exactSnapshotOwnerLocked(o) {
		return 0, fault.ErrStopped
	}
	return h.registerDeliveryLocked(o.slot, delivery{from: o.key.Peer, to: to,
		snapshot: true, deadline: deadline, attempt: attempt})
}
func (h *Host) reportSnapshotDelivery(owner loadsnapshot.Owner, key snapshot.Key, ack snapshot.Ack, cause error) {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil {
		return
	}
	h.mu.Lock()
	if !h.exactSnapshotOwnerLocked(o) {
		h.mu.Unlock()
		return
	}
	record, found := o.slot.deliveries[key.DeliveryID]
	if !found || record.from != key.From || record.to != key.To || record.attempt.RunID != o.key.RunID {
		h.mu.Unlock()
		return
	}
	delete(o.slot.deliveries, key.DeliveryID) // terminal CAS against expiry
	h.mu.Unlock()
	err := cause
	if !time.Now().Before(record.deadline) {
		err = &snapshot.TransferError{Reason: snapshot.ReasonTotalTimeout, Cause: cause}
	}
	if err == nil && (ack.Session.Key != key ||
		(ack.State != snapshot.DeliveryAccepted && ack.State != snapshot.DeliveryNotNeeded)) {
		err = &snapshot.TransferError{Reason: snapshot.ReasonMalformed}
	}
	if err != nil {
		reportSnapshotAttemptFailure(o.core, record.attempt, err)
	} else {
		o.core.ReportSnapshotAttempt(record.attempt, raft.SnapshotFinish)
	}
}
func (h *Host) reportSnapshotFailure(owner loadsnapshot.Owner, attempt raftengine.SnapshotAttempt, err error) {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil || attempt.RunID != o.key.RunID {
		return
	}
	h.mu.Lock()
	valid := h.exactSnapshotOwnerLocked(o)
	h.mu.Unlock()
	if valid {
		reportSnapshotAttemptFailure(o.core, attempt, err)
	}
}
func (h *Host) noteSnapshotProgress(
	owner loadsnapshot.Owner, attempt raftengine.SnapshotAttempt, p metrics.SnapshotCatchUp,
) {
	o, ok := owner.(*managedSnapshotOwner)
	if !ok || o == nil || o.host != h || attempt.RunID != o.key.RunID {
		return
	}
	// The immutable core handle never redirects. Its bounded mailbox validates
	// this exact run/attempt and refuses updates after join, without Host locking.
	o.core.NoteSnapshotProgress(attempt, p)
}
func reportSnapshotAttemptFailure(core *raftengine.Group, attempt raftengine.SnapshotAttempt, err error) {
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) {
		core.ReportSnapshotAttemptError(attempt, err)
		return
	}
	if detail.Reason == snapshot.ReasonProgressTimeout || detail.Reason == snapshot.ReasonTotalTimeout {
		core.ReportSnapshotAttemptTimeout(attempt)
	} else {
		core.ReportSnapshotAttemptError(attempt, err)
	}
}

//nolint:unused // Task 15 promotes this tested nonselecting bridge to PrepareSnapshot atomically.
func (t engineTransport) prepareSnapshot(offer raftengine.SnapshotOffer) (raftengine.SnapshotJob, error) {
	t.host.mu.Lock()
	owner, err := t.host.snapshotOwnerLocked(t.slot)
	t.host.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return t.host.snapshots.PrepareSend(owner, offer)
}

func (h *Host) joinSnapshotOwner(slot *replica, core *raftengine.Group) error {
	if core == nil {
		return nil
	}
	// Ingress cleanup can depend on the engine's post-join completion callback.
	core.RequestStop()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.EffectTimeout)
	defer cancel()
	return h.snapshots.StopOwner(ctx, loadsnapshot.OwnerKey{Peer: localPeer(slot), RunID: core.RunID()})
}
