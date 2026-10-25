package managed

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Keep a bounded acknowledgement window per engine, matching its outbound
// batch queue. Old ordinary deliveries may leave the window; snapshot tokens
// remain until completion or their explicit effect deadline.
const deliveryWindow = 256

type delivery struct {
	attempt  raftengine.SnapshotAttempt
	from, to load.Peer
	snapshot bool
	deadline time.Time
}
type engineTransport struct {
	host *Host
	slot *replica
}

func completeIdentity(id topology.Identity) bool {
	return id.NodeName != "" && id.ClusterID != "" && id.MemberID != 0 && id.InstanceID != ""
}
func completePeer(peer load.Peer) bool {
	return peer.Replica.ReplicaID != 0 && completeIdentity(peer.Node) &&
		topology.ValidateLoadID(peer.Replica.GroupID) == nil
}
func localPeer(slot *replica) load.Peer {
	return load.Peer{Node: slot.node, Replica: load.ReplicaRef{GroupID: slot.identity.GroupID,
		ReplicaID: slot.identity.ReplicaID}}
}

func (t engineTransport) Send(parent context.Context, messages []raftpb.Message) error {
	if len(messages) == 0 {
		return nil
	}
	h, slot := t.host, t.slot
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	from := localPeer(slot)
	if h.cfg.Identity() != from.Node {
		return fault.ErrIdentityMismatch
	}
	ref := load.ReplicaRef{GroupID: slot.identity.GroupID, ReplicaID: messages[0].To}
	to, err := h.cfg.ResolvePeer(ctx, ref)
	if err != nil {
		return err
	}
	if to.Replica != ref || to.Node.ClusterID != from.Node.ClusterID || !completePeer(to) {
		return fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidatePeer(ctx, to); err != nil {
		return err
	}
	snapshot := false
	for _, msg := range messages {
		if msg.From != from.Replica.ReplicaID || msg.To != to.Replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
		snapshot = snapshot || msg.Type == raftpb.MsgSnap
	}
	id, err := h.registerDelivery(slot, delivery{from: from, to: to, snapshot: snapshot})
	if err != nil {
		return err
	}
	// The engine gave us owned copies; transport owns them after this call.
	err = h.cfg.Transport.SendRaft(ctx, load.RaftBatch{From: from, To: to, DeliveryID: id, Messages: messages})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		h.mu.Lock()
		delete(slot.deliveries, id)
		h.mu.Unlock()
		// The core sender reports all messages in an unsuccessful send.
	}
	return err
}

// ReceiveRaft validates exact peer identities before delivering owned Raft
// messages.
func (h *Host) ReceiveRaft(ctx context.Context, batch load.RaftBatch) error {
	if err := h.validateReceivedBatch(ctx, batch); err != nil {
		return err
	}
	receiver, err := h.receivedReplica(batch.To)
	if err != nil {
		return err
	}
	return stepReceivedRaft(ctx, receiver, batch.Messages)
}

// ReadReplica validates exact peers and returns diagnostic or confirmed native
// membership state.
func (h *Host) ReadReplica(ctx context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	if err := ctx.Err(); err != nil {
		return load.ReplicaStatus{}, err
	}
	if !completeIdentity(query.From) || !completePeer(query.To) {
		return load.ReplicaStatus{}, fault.ErrInvalid
	}
	identity := h.cfg.Identity()
	if query.To.Node != identity || query.From.ClusterID != query.To.Node.ClusterID {
		return load.ReplicaStatus{}, fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidateNode(ctx, query.From); err != nil {
		return load.ReplicaStatus{}, err
	}
	if !query.Native {
		if err := h.cfg.ValidatePeer(ctx, query.To); err != nil {
			return load.ReplicaStatus{}, err
		}
	}
	// A native read can address an older, still-owned ready replica retained in
	// an approved plan after it leaves current topology. The exact local peer
	// and authenticated current caller are checked independently. This
	// read-only path does not relax sender admission for Raft traffic or grant
	// any effect.
	result := load.ReplicaStatus{Peer: query.To}
	h.mu.Lock()
	slot := h.slots[query.To.Replica.GroupID]
	if slot == nil {
		h.mu.Unlock()
		return result, nil
	}
	if localPeer(slot) != query.To {
		// The query's exact identity was authorized above. During reseating,
		// Convergence and Target may name different local replicas; absence of this
		// one must not expose or misidentify the other handle.
		h.mu.Unlock()
		return result, nil
	}
	fillReplicaStatusLocked(slot, query, &result)
	core := slot.core
	h.mu.Unlock()
	if query.Native && result.Found {
		return h.readNativeReplica(ctx, slot, core, result)
	}
	return result, nil
}

// ReportDelivery applies a transport outcome only to its matching engine and
// delivery token.
func (h *Host) ReportDelivery(ctx context.Context, report load.DeliveryReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if report.DeliveryID == 0 || !completePeer(report.From) || !completePeer(report.To) {
		return fault.ErrInvalid
	}
	if report.Kind != load.DeliveryUnreachable && report.Kind != load.DeliverySnapshotSucceeded &&
		report.Kind != load.DeliverySnapshotFailed {
		return fault.ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	slot := h.slots[report.From.Replica.GroupID]
	if slot == nil {
		return nil
	}
	d, found := slot.deliveries[report.DeliveryID]
	if !found {
		return nil
	} // completed/old handle; cannot reach a successor
	if d.attempt.Generation != 0 {
		return fault.ErrInvalid
	}
	if report.From != d.from || report.To != d.to {
		return fault.ErrIdentityMismatch
	}
	if report.Kind != load.DeliveryUnreachable && !d.snapshot {
		return fault.ErrInvalid
	}
	delete(slot.deliveries, report.DeliveryID)
	return applyDeliveryReport(slot.core, d, report.Kind)
}

func (h *Host) expireDeliveries(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, slot := range h.slots {
		for id, d := range slot.deliveries {
			if now.Before(d.deadline) {
				continue
			}
			delete(slot.deliveries, id)
			if d.snapshot && slot.core != nil {
				if d.attempt.Generation != 0 {
					slot.core.ReportSnapshotAttemptTimeout(d.attempt)
				} else {
					slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
				}
			}
		}
	}
}

func (h *Host) registerDelivery(slot *replica, d delivery) (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.registerDeliveryLocked(slot, d)
}
func (h *Host) registerDeliveryLocked(slot *replica, d delivery) (uint64, error) {
	if h.slots[slot.identity.GroupID] != slot || slot.ctx.Err() != nil {
		return 0, fault.ErrStopped
	}
	if len(slot.deliveries) >= deliveryWindow {
		if !slot.evictOrdinaryDelivery() {
			return 0, fault.ErrOverloaded
		}
	}
	if h.deliverySeq == math.MaxUint64 {
		return 0, fault.ErrOverloaded
	}
	h.deliverySeq++
	id := h.deliverySeq
	if d.deadline.IsZero() {
		d.deadline = time.Now().Add(h.cfg.EffectTimeout)
	}
	slot.deliveries[id] = d
	if !d.snapshot {
		slot.appendDelivery(id)
	}
	return id, nil
}

// Delivery IDs are monotonic. A FIFO avoids scanning the full map on every
// send once the acknowledgement window fills. Completion and expiry may leave
// stale IDs; skip them lazily, and compact to keep that bookkeeping bounded.
// The host mutex protects both the FIFO and the exact delivery map.
func (r *replica) evictOrdinaryDelivery() bool {
	for len(r.deliveryOrder) > 0 {
		id := r.deliveryOrder[0]
		r.deliveryOrder = r.deliveryOrder[1:]
		if _, live := r.deliveries[id]; live {
			delete(r.deliveries, id)
			return true
		}
	}
	return false
}

func (r *replica) appendDelivery(id uint64) {
	if len(r.deliveryOrder) >= 2*deliveryWindow {
		live := r.deliveryOrder[:0]
		for _, old := range r.deliveryOrder {
			if _, found := r.deliveries[old]; found {
				live = append(live, old)
			}
		}
		r.deliveryOrder = live
	}
	r.deliveryOrder = append(r.deliveryOrder, id)
}

type receivedReplica struct {
	core      *raftengine.Group
	published bool
}

func (h *Host) validateReceivedBatch(ctx context.Context, batch load.RaftBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch.DeliveryID == 0 || len(batch.Messages) == 0 || !completePeer(batch.From) || !completePeer(batch.To) {
		return fault.ErrInvalid
	}
	identity := h.cfg.Identity()
	if batch.To.Node != identity || batch.From.Node.ClusterID != batch.To.Node.ClusterID ||
		batch.From.Replica.GroupID != batch.To.Replica.GroupID {
		return fault.ErrIdentityMismatch
	}
	for _, msg := range batch.Messages {
		if msg.From != batch.From.Replica.ReplicaID || msg.To != batch.To.Replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
	}
	if err := h.cfg.ValidatePeer(ctx, batch.From); err != nil {
		return err
	}
	return h.cfg.ValidatePeer(ctx, batch.To)
}

func (h *Host) receivedReplica(to load.Peer) (receivedReplica, error) {
	h.mu.Lock()
	slot := h.slots[to.Replica.GroupID]
	if slot == nil {
		h.mu.Unlock()
		return receivedReplica{}, fault.ErrReplicaNotReady
	}
	if localPeer(slot) != to {
		h.mu.Unlock()
		return receivedReplica{}, fault.ErrIdentityMismatch
	}
	core := slot.core
	stopping := slot.state == "stopping" || slot.released
	published := slot.state == "ready" && slot.handle != nil && !slot.released
	h.mu.Unlock()
	if core == nil || stopping || !core.Running() {
		return receivedReplica{}, fault.ErrReplicaNotReady
	}
	return receivedReplica{core: core, published: published}, nil
}

func stepReceivedRaft(ctx context.Context, receiver receivedReplica, messages []raftpb.Message) error {
	core, published := receiver.core, receiver.published
	// An unpublished replica refuses forwarded proposals; messages before the
	// first one are still stepped, as when each message was delivered alone.
	if !published {
		i := slices.IndexFunc(messages, func(m raftpb.Message) bool { return m.Type == raftpb.MsgProp })
		if i >= 0 {
			if i > 0 {
				if err := core.StepBatch(ctx, messages[:i]); err != nil {
					return err
				}
			}
			return fault.ErrReplicaNotReady
		}
	}
	return core.StepBatch(ctx, messages)
}

func (h *Host) readNativeReplica(ctx context.Context, slot *replica,
	core *raftengine.Group, result load.ReplicaStatus) (load.ReplicaStatus, error) {
	state, err := core.ReadNative(ctx)
	if err != nil {
		return load.ReplicaStatus{}, err
	}
	// Never hold the host lock across a quorum wait. Recheck the
	// publication
	// reservation afterwards; a stopped/replaced handle cannot certify its
	// successor's membership, even when the durable identity was reused.
	h.mu.Lock()
	current := h.slots[result.Peer.Replica.GroupID]
	valid := current == slot && slot.core == core && (slot.state == "ready" ||
		slot.state == recoveryReady) && !slot.released && core.Running()
	h.mu.Unlock()
	if !valid {
		return load.ReplicaStatus{}, fault.ErrStopped
	}
	result.Native = &state
	return result, nil
}

func applyDeliveryReport(core *raftengine.Group, d delivery, kind load.DeliveryKind) error {
	if core == nil || !core.Running() {
		return nil
	}
	if !time.Now().Before(d.deadline) {
		if d.snapshot {
			core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
		}
		return nil
	}
	switch kind {
	case load.DeliveryUnreachable:
		core.ReportUnreachable(d.to.Replica.ReplicaID)
		if d.snapshot {
			core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
		}
	case load.DeliverySnapshotSucceeded:
		core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFinish)
	case load.DeliverySnapshotFailed:
		core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
	default: // Other delivery reports do not update Raft progress.
	}
	return nil
}

func fillReplicaStatusLocked(slot *replica, query load.ReplicaQuery, result *load.ReplicaStatus) {
	result.LocalState = slot.state
	// Only a published replica can attest to a completed topology effect.
	// During startup the engine already exchanges Raft traffic, but replay and
	// the final authorization check still belong to the startup reservation.
	// Advertising it early could advance its operator and invalidate that
	// check.
	if (slot.state == "ready" && slot.handle != nil || slot.state == recoveryReady) && slot.core != nil &&
		!slot.released && slot.core.Running() {
		result.Found, result.Status = true, slot.core.Status()
		if slot.state == recoveryReady && !query.Native {
			result.Status.ErrorCode = string(fault.ReplicaNotReady)
		}
		sample := slot.core.Metrics().Sample
		result.Metrics = &sample
	}
}
