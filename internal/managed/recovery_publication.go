package managed

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func (h *Host) readyRecoverySlot(ctx context.Context, ref load.ReplicaRef) (*replica, error) {
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	if slot == nil || slot.identity.ReplicaID != ref.ReplicaID || slot.recoveryKey == nil {
		h.mu.Unlock()
		return nil, fault.ErrReplicaNotReady
	}
	h.mu.Unlock()
	if err := h.waitPrivateStarted(ctx, slot); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if slot.released || h.slots[ref.GroupID] != slot || slot.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	return slot, nil
}

// RecoveryCheckpoint uses the exact private core and never transfers ownership.
func (h *Host) RecoveryCheckpoint(ctx context.Context, ref load.ReplicaRef) (model.CheckpointProof, error) {
	if err := h.ReplayRecovery(ctx, ref); err != nil {
		return model.CheckpointProof{}, err
	}
	slot, err := h.readyRecoverySlot(ctx, ref)
	if err != nil {
		return model.CheckpointProof{}, err
	}
	h.mu.Lock()
	group, ok := h.recoveryGroupLocked(slot.identity)
	previous := slot.recoveryProof
	h.mu.Unlock()
	if !ok {
		return model.CheckpointProof{}, fault.ErrOperationAuthorization
	}
	if group.Consumed != nil {
		// Confirmed historical evidence; this group never needs a second consumption.
		c := group.Consumed
		return model.CheckpointProof{Identity: c.Identity, Position: c.Checkpoint, Digest: c.SnapshotDigest}, nil
	}
	if previous != nil {
		return *previous, nil
	}
	proof, err := slot.core.RecoveryCheckpoint(slot.ctx, *slot.recoveryKey)
	if err != nil {
		return model.CheckpointProof{}, err
	}
	h.mu.Lock()
	if h.slots[ref.GroupID] != slot || slot.released || slot.ctx.Err() != nil {
		h.mu.Unlock()
		return model.CheckpointProof{}, fault.ErrStopped
	}
	slot.recoveryProof = &proof
	h.mu.Unlock()
	if err := h.recordRecoverySeed(slot.ctx, slot, "replayed", proof.Position); err != nil {
		return model.CheckpointProof{}, err
	}
	return proof, nil
}

func (h *Host) recordConfirmedConsumption(ctx context.Context) error {
	h.mu.Lock()
	var ready []*replica
	if h.recoveryImport != nil {
		for _, group := range h.recoveryImport.Groups {
			slot := h.slots[group.GroupID]
			if group.Consumed != nil && slot != nil && slot.recoveryMarker != nil &&
				slot.identity == group.Destination {
				ready = append(ready, slot)
			}
		}
	}
	h.mu.Unlock()
	for _, slot := range ready {
		h.mu.Lock()
		group, ok := h.recoveryGroupLocked(slot.identity)
		h.mu.Unlock()
		if !ok || group.Consumed == nil {
			return fault.ErrOperationAuthorization
		}
		if err := h.recordRecoverySeed(ctx, slot, "consumed", group.Consumed.Checkpoint); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) recoveryPublicationLocked(identity topology.Identity) error {
	if identity != h.cfg.Identity() {
		return fault.ErrIdentityMismatch
	}
	if h.quiescing || h.stopped || h.ctx == nil || h.ctx.Err() != nil {
		return fault.ErrStopped
	}
	if h.recoveryManifest == nil {
		return fault.ErrOperationAuthorization
	}
	if h.recoveryImport == nil || h.recoveryImport.Phase != "complete" {
		return fault.ErrReplicaNotReady
	}
	for _, group := range h.recoveryImport.Groups {
		slot := h.slots[group.GroupID]
		if group.Consumed == nil || slot == nil || slot.core == nil {
			return fault.ErrReplicaNotReady
		}
		if slot.released || slot.state != recoveryReady && slot.state != "ready" ||
			slot.identity != group.Destination || h.holds[slot.identity] ||
			slot.ctx.Err() != nil || !slot.core.Running() {
			return fault.ErrReplicaNotReady
		}
	}
	return nil
}

// PublishRecovery exposes the complete data set for the application's readiness validation.
func (h *Host) PublishRecovery(ctx context.Context, identity topology.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.recoveryPublicationLocked(identity); err != nil {
		return err
	}
	for _, group := range h.recoveryImport.Groups {
		slot := h.slots[group.GroupID]
		if slot.state == recoveryReady {
			slot.handle = loadhandle.New(slot.core, slot.identity)
			slot.state = "ready"
		}
	}
	return nil
}

// FinishRecovery releases protection before local Complete, at joined writer boundaries.
func (h *Host) FinishRecovery(ctx context.Context, identity topology.Identity) error {
	h.mu.Lock()
	if err := h.recoveryPublicationLocked(identity); err != nil {
		h.mu.Unlock()
		return err
	}
	state := model.CloneImport(*h.recoveryImport)
	h.mu.Unlock()
	for _, group := range state.Groups {
		ref := load.ReplicaRef{GroupID: group.GroupID, ReplicaID: group.Destination.ReplicaID}
		slot, err := h.readyRecoverySlot(ctx, ref)
		if err != nil {
			return err
		}
		if err := slot.core.FinishRecovery(ctx, *slot.recoveryKey, *group.Consumed); err != nil {
			return err
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.recoveryPublicationLocked(identity)
}

// ReleaseRecoveryRun starts deferred background callbacks after durable local completion.
func (h *Host) ReleaseRecoveryRun(ctx context.Context, identity topology.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.recoveryPublicationLocked(identity); err != nil {
		return err
	}
	h.recoveryReleased = true
	for _, group := range h.recoveryImport.Groups {
		slot := h.slots[group.GroupID]
		if slot.runDone == nil && slot.app.Run != nil {
			h.startApplicationRunLocked(slot)
		}
	}
	return nil
}

// ReplayRecovery privately reopens retained B before a retry needs its native fence.
func (h *Host) ReplayRecovery(ctx context.Context, ref load.ReplicaRef) error {
	h.mu.Lock()
	if h.recoveryResumes == nil {
		h.mu.Unlock()
		return fault.ErrOperationAuthorization
	}
	h.recoveryResumes[ref.GroupID] = true
	h.mu.Unlock()
	if _, err := h.reserveStart(ctx, ref, StartRecovery); err != nil {
		if errors.Is(err, fault.ErrAuthorityPending) {
			return fault.New(fault.ReplicaNotReady, "waiting for ordinary recovery authority", err)
		}
		return err
	}

	_, err := h.readyRecoverySlot(ctx, ref)
	return err
}
