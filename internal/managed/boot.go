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

// ParkReason classifies a refused boot without discarding retained evidence.
type ParkReason string

// Parking reasons identify missing, unreadable or foreign retained replica
// state.
const (
	ParkStoreMissing     ParkReason = "store_missing"
	ParkStoreUnreadable  ParkReason = "store_unreadable"
	ParkIdentityMismatch ParkReason = "identity_mismatch"
)

// ParkedReplica records the exact refused identity, reason and first
// observation time.
type ParkedReplica struct {
	Identity storage.Identity
	Reason   ParkReason
	Since    time.Time
}

// ParkedError reports a refused boot and retains its underlying storage or
// identity error.
type ParkedError struct {
	ParkedReplica
	Cause error
}

func (e *ParkedError) Error() string {
	return fmt.Sprintf("load group %s replica %d parked: %s", e.Identity.GroupID, e.Identity.ReplicaID, e.Reason)
}
func (e *ParkedError) Unwrap() error { return e.Cause }

// Is matches a refused boot against the replica-not-ready classification.
func (*ParkedError) Is(err error) bool {
	return errors.Is(fault.ErrReplicaNotReady, err)
}

type bootResult struct {
	origin   string
	previous uint64
}

type bootEvidence struct {
	record   metastore.ReplicaRecord
	recorded bool
	probe    storage.Probe
	scope    metaengine.LoadScope
}

// decideLoadBoot is the existing durable-evidence decision table. An empty
// prepared learner is resumable; missing established storage never refounds.
func decideLoadBoot(want storage.Identity, evidence bootEvidence) (bootResult, ParkReason) {
	result := bootResult{origin: "fresh"}
	if evidence.recorded && evidence.record.ReplicaID != want.ReplicaID {
		result.previous, result.origin = evidence.record.ReplicaID, "reseat"
	}
	reason := evidence.parkReason(want)
	if reason != "" || !evidence.probe.Exists {
		return result, reason
	}
	result.origin = evidence.retainedOrigin(want, result.origin)
	return result, ""
}

func (e bootEvidence) parkReason(want storage.Identity) ParkReason {
	if e.probe.Exists && e.probe.Identity != want {
		return ParkIdentityMismatch
	}
	if !e.probe.Exists {
		if e.recorded && e.record.ReplicaID == want.ReplicaID || e.scope.Started {
			return ParkStoreMissing
		}
		return ""
	}
	if !e.probe.HasLog && e.scope.Established {
		return ParkStoreMissing
	}
	return ""
}

func (e bootEvidence) retainedOrigin(want storage.Identity, origin string) string {
	if e.recorded && e.record.ReplicaID == want.ReplicaID {
		return "resume"
	}
	if !e.recorded {
		return "adopt"
	}
	return origin
}

type bootPreflight struct {
	scope metaengine.LoadScope
	boot  bootResult
	probe storage.Probe
}

func (h *Host) preflight(parent context.Context, slot *replica) (bootPreflight, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	scope, err := h.startScope(ctx, slot.node, slot.identity)
	if err != nil {
		return bootPreflight{scope: scope}, err
	}
	evidence, err := h.loadBootEvidence(ctx, slot, scope)
	if err != nil {
		return bootPreflight{scope: scope, probe: evidence.probe}, err
	}
	return h.authorizeBoot(slot, evidence)
}

func (h *Host) loadBootEvidence(ctx context.Context, slot *replica, scope metaengine.LoadScope) (bootEvidence, error) {
	evidence, err := h.lookupBootRecord(ctx, slot)
	evidence.scope = scope
	if err != nil {
		return evidence, err
	}
	probe, err := h.probeBootStore(ctx, slot, scope)
	evidence.probe = probe
	return evidence, err
}

func (h *Host) lookupBootRecord(ctx context.Context, slot *replica) (bootEvidence, error) {
	record, err := h.cfg.LookupRecord(ctx, slot.identity.GroupID)
	if err != nil {
		return bootEvidence{}, h.park(slot, parkReason(err), err)
	}
	if record.Found && (record.Record.NodeName != slot.identity.NodeName ||
		record.Record.ClusterID != slot.identity.ClusterID) {
		return bootEvidence{}, h.park(slot, ParkIdentityMismatch, fault.ErrIdentityMismatch)
	}
	return bootEvidence{record: record.Record, recorded: record.Found}, nil
}

func (h *Host) probeBootStore(ctx context.Context, slot *replica, scope metaengine.LoadScope) (storage.Probe, error) {
	store, err := h.storeFor(scope.Group.Storage)
	if err != nil {
		return storage.Probe{}, err
	}
	located, err := h.locate(ctx, slot.identity.GroupID)
	found, probe := located.Store, located.Probe
	if err != nil {
		return probe, h.park(slot, parkReason(err), err)
	}
	if probe.Exists && found != store {
		return probe, h.park(slot, ParkIdentityMismatch,
			fault.New(fault.IdentityMismatch, "group state is held by another storage class", nil))
	}
	slot.store = store
	return probe, nil
}

func (h *Host) authorizeBoot(slot *replica, evidence bootEvidence) (bootPreflight, error) {
	boot, reason := decideLoadBoot(slot.identity, evidence)
	result := bootPreflight{scope: evidence.scope, boot: boot, probe: evidence.probe}
	if reason != "" {
		err := h.park(slot, reason, nil)
		if reason == ParkStoreMissing && evidence.scope.Group.Storage == topology.StorageMemory {
			h.startOwnReseat(slot, evidence.scope)
		}
		return result, err
	}
	return result, checkBootCreation(slot, evidence)
}

func checkBootCreation(slot *replica, evidence bootEvidence) error {
	probe, scope := evidence.probe, evidence.scope
	if !probe.Exists || !probe.HasLog && scope.Founding {
		if !creationGrant(slot) {
			return fault.ErrAuthorityFenced
		}
		if scope.Founding && slot.startGrant.Action.Kind != topology.StepCreateFoundingGroup {
			return fault.ErrAuthorityFenced
		}
	}
	return nil
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
	parked, changed := h.recordParked(slot, reason)
	if changed && h.cfg.Events != nil {
		h.cfg.Events.Publish(events.Event{
			Type:       events.RuntimeError,
			NodeName:   slot.identity.NodeName,
			ClusterID:  slot.identity.ClusterID,
			InstanceID: slot.node.InstanceID,
			GroupID:    slot.identity.GroupID,
			ReplicaID:  slot.identity.ReplicaID,
			Code:       string(reason),
			State:      "parked",
		})
	}
	return &ParkedError{ParkedReplica: parked, Cause: cause}
}

func (h *Host) recordParked(slot *replica, reason ParkReason) (ParkedReplica, bool) {
	h.mu.Lock()
	parked, found := h.parked[slot.identity.GroupID]
	changed := !found || parked.Identity != slot.identity || parked.Reason != reason
	if changed {
		parked = ParkedReplica{Identity: slot.identity, Reason: reason, Since: time.Now().UTC()}
		h.parked[slot.identity.GroupID] = parked
	}
	h.mu.Unlock()
	return parked, changed
}
