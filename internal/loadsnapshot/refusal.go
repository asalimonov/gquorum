package loadsnapshot

import (
	"errors"
	"math"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
)

type snapshotRefusalKey struct {
	owner  OwnerKey
	to     uint64
	reason string
}

type snapshotRefusal struct {
	id              uint64
	lastAt, eventAt time.Time
}

// Refusals contain bounded scalar diagnostics, never owner handles or payloads.
// The native loop can call this before a job exists: no resolution, I/O, user
// callback or waiting is introduced. Events use the borrowed bounded bus.
func (c *Coordinator) observeSendRefusal(owner OwnerKey, offer raftengine.SnapshotOffer, err error, now time.Time) {
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) {
		return
	}
	switch detail.Reason {
	case snapshot.ReasonSizeLimit, snapshot.ReasonCapacity, snapshot.ReasonGroupBusy, snapshot.ReasonUnsupported:
	default:
		return
	}
	key := snapshotRefusalKey{owner: owner, to: offer.Message.To, reason: detail.Reason}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.refusals == nil {
		c.refusals = make(map[snapshotRefusalKey]snapshotRefusal)
	}
	previous := c.refusals[key]
	changed := c.refusalReasonChangedLocked(key)

	status := snapshot.Status{Key: snapshot.Key{From: owner.Peer, To: load.Peer{Replica: load.ReplicaRef{
		GroupID: owner.Peer.Replica.GroupID, ReplicaID: key.to}}},
		Direction: "outgoing", SnapshotIndex: offer.Message.Snapshot.Metadata.Index,
		SnapshotTerm: offer.Message.Snapshot.Metadata.Term, TotalBytes: uint64(len(offer.Message.Snapshot.Data)),
		StartedAt: now, Delivery: snapshot.DeliveryFailed, ReceiverOutcome: snapshot.ReceiverUnknown,
		ErrorReason: key.reason, AdmissionRefused: true, ErrorSize: detail.Size, ErrorLimit: detail.Limit,
		Refusals: 1, LastRefusedAt: now}
	previous = c.recordRefusalHistoryLocked(previous, status)
	bus := c.ports.Events
	notify := changed || previous.eventAt.IsZero() || now.Sub(previous.eventAt) >= time.Minute
	if notify {
		notify = c.allowRefusalEventLocked(now)
	}
	previous.lastAt = now
	if notify {
		previous.eventAt = now
	}
	if _, exists := c.refusals[key]; !exists && len(c.refusals) == snapshotHistoryLimit {
		c.evictRefusalLocked()
	}
	c.refusals[key] = previous
	c.mu.Unlock()
	if notify && bus != nil {
		peer := owner.Peer
		bus.Publish(events.Event{Type: events.RuntimeError, Code: "snapshot_" + key.reason, State: "send_refused",
			NodeName: peer.Node.NodeName, InstanceID: peer.Node.InstanceID, MemberID: peer.Node.MemberID,
			ClusterID: peer.Node.ClusterID, GroupID: peer.Replica.GroupID, ReplicaID: peer.Replica.ReplicaID})
	}
}

func (c *Coordinator) refusalReasonChangedLocked(key snapshotRefusalKey) bool {
	var latest time.Time
	var reason string
	for candidate, record := range c.refusals {
		if candidate.owner == key.owner && candidate.to == key.to && record.lastAt.After(latest) {
			latest, reason = record.lastAt, candidate.reason
		}
	}
	return reason != key.reason
}

func (c *Coordinator) evictRefusalLocked() {
	var oldest snapshotRefusalKey
	var at time.Time
	for key, record := range c.refusals {
		if at.IsZero() || record.lastAt.Before(at) {
			oldest, at = key, record.lastAt
		}
	}
	delete(c.refusals, oldest)
}

// A global bound also protects against rate-limit cache churn by many groups.
func (c *Coordinator) allowRefusalEventLocked(now time.Time) bool {
	if c.refusalEventWindow.IsZero() || now.Sub(c.refusalEventWindow) >= time.Minute {
		c.refusalEventWindow, c.refusalEventCount = now, 0
	}
	if c.refusalEventCount >= snapshotHistoryLimit {
		return false
	}
	c.refusalEventCount++
	return true
}

func (c *Coordinator) recordRefusalHistoryLocked(previous snapshotRefusal, status snapshot.Status) snapshotRefusal {
	for i, id := range c.historyIDs {
		if id != previous.id {
			continue
		}
		status.StartedAt = c.history[i].StartedAt
		status.Refusals = c.history[i].Refusals
		if status.Refusals != math.MaxUint64 {
			status.Refusals++
		}
		c.history[i] = status
		return previous
	}
	c.appendHistoryLocked(status)
	previous.id = c.historySequence
	return previous
}
