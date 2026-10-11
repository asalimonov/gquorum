package managed

import (
	"context"
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
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
	return completeIdentity(peer.Node) && peer.Replica.ReplicaID != 0 && topology.ValidateLoadID(peer.Replica.GroupID) == nil
}
func localPeer(slot *replica) load.Peer {
	return load.Peer{Node: slot.node, Replica: load.ReplicaRef{GroupID: slot.identity.GroupID, ReplicaID: slot.identity.ReplicaID}}
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
	if !completePeer(to) || to.Replica != ref || to.Node.ClusterID != from.Node.ClusterID {
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
	h.mu.Lock()
	if h.slots[slot.identity.GroupID] != slot || slot.ctx.Err() != nil {
		h.mu.Unlock()
		return fault.ErrStopped
	}
	if len(slot.deliveries) >= deliveryWindow {
		oldest := uint64(math.MaxUint64)
		for id, d := range slot.deliveries {
			if !d.snapshot && id < oldest {
				oldest = id
			}
		}
		if oldest == math.MaxUint64 {
			h.mu.Unlock()
			return fault.ErrOverloaded
		}
		delete(slot.deliveries, oldest)
	}
	if h.deliverySeq == math.MaxUint64 {
		h.mu.Unlock()
		return fault.ErrOverloaded
	}
	h.deliverySeq++
	id := h.deliverySeq
	slot.deliveries[id] = delivery{from: from, to: to, snapshot: snapshot, deadline: time.Now().Add(h.cfg.EffectTimeout)}
	h.mu.Unlock()
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

func (h *Host) ReceiveRaft(ctx context.Context, batch load.RaftBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !completePeer(batch.From) || !completePeer(batch.To) || batch.DeliveryID == 0 || len(batch.Messages) == 0 {
		return fault.ErrInvalid
	}
	if batch.To.Node != h.cfg.Identity() || batch.From.Node.ClusterID != batch.To.Node.ClusterID || batch.From.Replica.GroupID != batch.To.Replica.GroupID {
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
	if err := h.cfg.ValidatePeer(ctx, batch.To); err != nil {
		return err
	}
	h.mu.Lock()
	slot := h.slots[batch.To.Replica.GroupID]
	if slot == nil {
		h.mu.Unlock()
		return fault.ErrReplicaNotReady
	}
	if localPeer(slot) != batch.To {
		h.mu.Unlock()
		return fault.ErrIdentityMismatch
	}
	core := slot.core
	stopping := slot.state == "stopping" || slot.released
	h.mu.Unlock()
	if core == nil || stopping || !core.Running() {
		return fault.ErrReplicaNotReady
	}
	for _, msg := range batch.Messages {
		if err := core.Step(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) ReadReplica(ctx context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	if err := ctx.Err(); err != nil {
		return load.ReplicaStatus{}, err
	}
	if !completeIdentity(query.From) || !completePeer(query.To) {
		return load.ReplicaStatus{}, fault.ErrInvalid
	}
	if query.To.Node != h.cfg.Identity() || query.From.ClusterID != query.To.Node.ClusterID {
		return load.ReplicaStatus{}, fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidateNode(ctx, query.From); err != nil {
		return load.ReplicaStatus{}, err
	}
	if err := h.cfg.ValidatePeer(ctx, query.To); err != nil {
		return load.ReplicaStatus{}, err
	}
	result := load.ReplicaStatus{Peer: query.To}
	h.mu.Lock()
	defer h.mu.Unlock()
	slot := h.slots[query.To.Replica.GroupID]
	if slot == nil {
		return result, nil
	}
	if localPeer(slot) != query.To {
		// The query's exact identity was authorized above. During reseating,
		// Active and Target may name different local replicas; absence of this
		// one must not expose or misidentify the other handle.
		return result, nil
	}
	result.LocalState = slot.state
	if slot.core != nil && !slot.released && slot.core.Running() {
		result.Found, result.Status = true, slot.core.Status()
	}
	return result, nil
}

func (h *Host) ReportDelivery(ctx context.Context, report load.DeliveryReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if report.DeliveryID == 0 || !completePeer(report.From) || !completePeer(report.To) {
		return fault.ErrInvalid
	}
	if report.Kind != load.DeliveryUnreachable && report.Kind != load.DeliverySnapshotSucceeded && report.Kind != load.DeliverySnapshotFailed {
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
	if report.From != d.from || report.To != d.to {
		return fault.ErrIdentityMismatch
	}
	if report.Kind != load.DeliveryUnreachable && !d.snapshot {
		return fault.ErrInvalid
	}
	delete(slot.deliveries, report.DeliveryID)
	if slot.core == nil || !slot.core.Running() {
		return nil
	}
	if !time.Now().Before(d.deadline) {
		if d.snapshot {
			slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
		}
		return nil
	}
	switch report.Kind {
	case load.DeliveryUnreachable:
		slot.core.ReportUnreachable(d.to.Replica.ReplicaID)
		if d.snapshot {
			slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
		}
	case load.DeliverySnapshotSucceeded:
		slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFinish)
	case load.DeliverySnapshotFailed:
		slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
	}
	return nil
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
				slot.core.ReportSnapshot(d.to.Replica.ReplicaID, raft.SnapshotFailure)
			}
		}
	}
}
