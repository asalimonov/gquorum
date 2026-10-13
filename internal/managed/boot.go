package managed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type ParkReason string

const (
	ParkStoreMissing     ParkReason = "store_missing"
	ParkStoreUnreadable  ParkReason = "store_unreadable"
	ParkIdentityMismatch ParkReason = "identity_mismatch"
)

type ParkedReplica struct {
	Identity storage.Identity
	Reason   ParkReason
	Since    time.Time
}
type ParkedError struct {
	ParkedReplica
	Cause error
}

func (e *ParkedError) Error() string {
	return fmt.Sprintf("load group %s replica %d parked: %s", e.Identity.GroupID, e.Identity.ReplicaID, e.Reason)
}
func (e *ParkedError) Unwrap() error     { return e.Cause }
func (_ *ParkedError) Is(err error) bool { return errors.Is(fault.ErrReplicaNotReady, err) }

type bootResult struct {
	origin   string
	previous uint64
}

// decideLoadBoot is the existing durable-evidence decision table. An empty
// prepared learner is resumable; missing established storage never refounds.
func decideLoadBoot(want storage.Identity, record metastore.ReplicaRecord, recorded bool, probe storage.Probe, scope metaengine.LoadScope) (bootResult, ParkReason) {
	result := bootResult{origin: "fresh"}
	if recorded && record.ReplicaID != want.ReplicaID {
		result.previous, result.origin = record.ReplicaID, "reseat"
	}
	if probe.Exists && probe.Identity != want {
		return result, ParkIdentityMismatch
	}
	if !probe.Exists {
		if recorded && record.ReplicaID == want.ReplicaID || scope.Started {
			return result, ParkStoreMissing
		}
		return result, ""
	}
	if !probe.HasLog && scope.Established {
		return result, ParkStoreMissing
	}
	if recorded && record.ReplicaID == want.ReplicaID {
		result.origin = "resume"
	}
	if !recorded {
		result.origin = "adopt"
	}
	return result, ""
}

func (h *Host) preflight(parent context.Context, slot *replica) (metaengine.LoadScope, bootResult, storage.Probe, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	scope, err := h.startScope(ctx, slot.node, slot.identity)
	if err != nil {
		return scope, bootResult{}, storage.Probe{}, err
	}
	record, recorded, err := h.cfg.LookupRecord(ctx, slot.identity.GroupID)
	if err != nil {
		return scope, bootResult{}, storage.Probe{}, h.park(slot, parkReason(err), err)
	}
	if recorded && (record.NodeName != slot.identity.NodeName || record.ClusterID != slot.identity.ClusterID) {
		return scope, bootResult{}, storage.Probe{}, h.park(slot, ParkIdentityMismatch, fault.ErrIdentityMismatch)
	}
	probe, err := h.cfg.Store.Probe(ctx, slot.identity.GroupID)
	if err != nil {
		return scope, bootResult{}, probe, h.park(slot, parkReason(err), err)
	}
	boot, reason := decideLoadBoot(slot.identity, record, recorded, probe, scope)
	if reason != "" {
		return scope, boot, probe, h.park(slot, reason, nil)
	}
	if (!probe.Exists || !probe.HasLog && scope.Founding) && (!creationGrant(slot) || scope.Founding && slot.startGrant.Action.Kind != topology.StepCreateFoundingGroup) {
		return scope, boot, probe, fault.ErrAuthorityFenced
	}
	return scope, boot, probe, nil
}

func parkReason(err error) ParkReason {
	if errors.Is(err, fault.ErrIdentityMismatch) {
		return ParkIdentityMismatch
	}
	return ParkStoreUnreadable
}

func (h *Host) park(slot *replica, reason ParkReason, cause error) error {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	h.mu.Lock()
	parked, found := h.parked[slot.identity.GroupID]
	changed := !found || parked.Identity != slot.identity || parked.Reason != reason
	if changed {
		parked = ParkedReplica{Identity: slot.identity, Reason: reason, Since: time.Now().UTC()}
		h.parked[slot.identity.GroupID] = parked
	}
	h.mu.Unlock()
	if changed && h.cfg.Events != nil {
		h.cfg.Events.Publish(events.Event{Type: events.RuntimeError, NodeName: slot.identity.NodeName, ClusterID: slot.identity.ClusterID, InstanceID: slot.node.InstanceID, GroupID: slot.identity.GroupID, ReplicaID: slot.identity.ReplicaID, Code: string(reason), State: "parked"})
	}
	return &ParkedError{ParkedReplica: parked, Cause: cause}
}
