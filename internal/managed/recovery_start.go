package managed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const recoveryReady = "recovery_ready"

func (h *Host) recoveryGroupLocked(id storage.Identity) (model.ImportGroup, bool) {
	if h.recoveryManifest == nil || h.recoveryReleased {
		return model.ImportGroup{}, false
	}
	if h.recoveryImport != nil {
		for _, group := range h.recoveryImport.Groups {
			if group.Destination == id {
				return group, true
			}
		}
		return model.ImportGroup{}, false
	}
	source := model.SeedSource{}
	for _, s := range h.recoveryManifest.Sources {
		if s.GroupID == id.GroupID {
			source = s
		}
	}
	for _, floor := range h.recoveryManifest.Floors {
		if floor.GroupID == id.GroupID && floor.NextID == id.ReplicaID && source.GroupID != "" {
			return model.ImportGroup{GroupID: id.GroupID, Source: source, Destination: id}, true
		}
	}
	return model.ImportGroup{}, false
}

func (h *Host) recoveryStartAllowedLocked(why StartReason, id storage.Identity) error {
	if h.recoveryManifest == nil || h.recoveryReleased {
		return nil
	}
	if why == StartExplicit || why == StartRecovery && !h.recoveryResumes[id.GroupID] {
		return fault.ErrReplicaNotReady
	}
	if _, ok := h.cfg.Store.(recoveryStore); !ok {
		return fault.ErrUnsupported
	}
	if _, ok := h.recoveryGroupLocked(id); !ok {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func (h *Host) configureRecoverySlotLocked(slot *replica) {
	group, ok := h.recoveryGroupLocked(slot.identity)
	if !ok {
		return
	}
	source := h.recoveryManifest
	slot.recoveryKey = &model.SeedKey{RecoveryID: source.Request.RecoveryID, PlanDigest: source.PlanDigest,
		Destination: slot.identity, MemberID: slot.node.MemberID}
	if group.Consumed == nil {
		slot.recoveryInfo = &load.RecoveryInfo{RecoveryID: source.Request.RecoveryID, Source: group.Source.Identity,
			SnapshotIndex: group.Source.Snapshot.Index, CommitIndex: group.Source.Commit.Index,
			CommitTerm: group.Source.Commit.Term}
	}
}

func cloneRecoveryInfo(value *load.RecoveryInfo) *load.RecoveryInfo {
	if value == nil {
		return nil
	}
	owned := *value
	return &owned
}

func (h *Host) acquireRecoveryReplay(slot *replica) (func(), error) {
	if slot.recoveryKey == nil {
		return func() {}, nil
	}
	h.mu.Lock()
	gate := h.recoveryGate
	h.mu.Unlock()
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-slot.ctx.Done():
		return nil, slot.ctx.Err()
	}
}

func (h *Host) waitPrivateStarted(ctx context.Context, slot *replica) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-slot.startDone:
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if slot.err != nil {
		if slot.recoveryKey != nil {
			return &model.StartFailure{Cause: slot.err}
		}
		return slot.err
	}
	if (slot.state == "ready" || slot.state == recoveryReady) && !slot.released && slot.core != nil &&
		slot.core.Running() {
		return nil
	}
	return fault.ErrReplicaNotReady
}

// PrepareRecoverySeed runs only under the existing exact startup reservation.
func (h *Host) PrepareRecoverySeed(ctx context.Context, ref load.ReplicaRef, grant topology.AuthorityGrant) error {
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	if slot == nil || slot.identity.ReplicaID != ref.ReplicaID || slot.recoveryKey == nil ||
		slot.state != "starting" {
		h.mu.Unlock()
		return fault.ErrOperationAuthorization
	}
	group, ok := h.recoveryGroupLocked(slot.identity)
	h.mu.Unlock()
	if !ok || slot.startGrant == nil || !slot.startGrant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidateGrant(ctx, grant); err != nil {
		return err
	}
	if err := h.checkStartupFloor(slot); err != nil {
		return err
	}
	backend, ok := h.cfg.Store.(recoveryStore)
	if !ok {
		return fault.ErrUnsupported
	}
	if err := backend.ProtectRecovery(ctx, *slot.recoveryKey); err != nil {
		return err
	}
	marker, installed, err := backend.ReadRecoverySeed(ctx, *slot.recoveryKey)
	if err != nil {
		return h.park(slot, parkReason(err), err)
	}
	if err := h.beginRecoveryCreation(ctx, slot, backend); err != nil {
		return err
	}
	if !installed {
		marker, err = h.installRecoverySeed(ctx, slot, marker, grant)
		if err != nil {
			return err
		}
	}

	if marker.Source != group.Source || marker.Destination != slot.identity {
		return fault.ErrIdentityMismatch
	}
	if err := h.validateSeedGrant(ctx, marker.Grant, grant); err != nil {
		return err
	}
	h.mu.Lock()
	slot.recoveryMarker = &marker
	h.mu.Unlock()
	return h.recordRecoverySeed(ctx, slot, "installed", model.Position{})
}

func (h *Host) validateSeedGrant(ctx context.Context, original, current topology.AuthorityGrant) error {
	if err := h.cfg.ValidateGrant(ctx, original); err != nil {
		return err
	}
	if current.Purpose == topology.AuthorityFence && current.Action == nil {
		if original.Action == nil || original.Action.Version != 2 ||
			original.Action.Kind != topology.StepCreateFoundingGroup ||
			original.Key.ClusterID != current.Key.ClusterID ||
			original.Key.GroupID != current.Key.GroupID ||
			original.Key.Revision >= current.Key.Revision {
			return fault.ErrOperationAuthorization
		}
		return nil // Retained private B can elect only to install the newer real fence.
	}
	if original.Action == nil || current.Action == nil ||
		original.Action.Kind != topology.StepCreateFoundingGroup ||
		original.Action.Version != 2 || current.Action.Version != 2 ||
		original.Action.Attempt > current.Action.Attempt ||
		original.Key.Revision > current.Key.Revision {
		return fault.ErrOperationAuthorization
	}
	a, b := *original.Action, *current.Action
	a.OperationID = b.OperationID
	a.Attempt = b.Attempt
	if a != b || original.Key.GroupID != current.Key.GroupID || original.Key.ClusterID != current.Key.ClusterID {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func (h *Host) recordRecoverySeed(ctx context.Context, slot *replica, state string, point model.Position) error {
	h.mu.Lock()
	marker := slot.recoveryMarker
	h.mu.Unlock()
	if h.cfg.RecordRecoverySeed == nil || marker == nil {
		return fault.ErrOperationAuthorization
	}
	data, err := model.EncodeCanonical(*marker)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	return h.cfg.RecordRecoverySeed(ctx, model.SeedProgress{GroupID: slot.identity.GroupID,
		Destination: slot.identity,
		State:       state, Attempt: 1, MarkerDigest: hex.EncodeToString(digest[:]), Checkpoint: point})
}

func (h *Host) installRecoverySeed(ctx context.Context, slot *replica, marker model.SeedMarker,
	grant topology.AuthorityGrant) (model.SeedMarker, error) {
	h.mu.Lock()
	group, ok := h.recoveryGroupLocked(slot.identity)
	manifest := model.CloneManifest(*h.recoveryManifest)
	h.mu.Unlock()
	if !ok {
		return model.SeedMarker{}, fault.ErrOperationAuthorization
	}
	backend, ok := h.cfg.Store.(recoveryStore)
	if !ok {
		return model.SeedMarker{}, fault.ErrUnsupported
	}

	record, err := h.lookupBootRecord(ctx, slot)
	if err != nil {
		return model.SeedMarker{}, err
	}
	if group.Consumed != nil || record.recorded && record.record.ReplicaID == slot.identity.ReplicaID {
		return model.SeedMarker{}, h.park(slot, ParkStoreMissing, fault.ErrUnreadable)
	}
	original := grant
	if marker.RecoveryID != "" {
		original = marker.Grant
	}
	if err := h.validateSeedGrant(ctx, original, grant); err != nil {
		return model.SeedMarker{}, err
	}
	installed, err := backend.InstallRecoverySeed(ctx, model.SeedSpec{Key: *slot.recoveryKey,
		BackupName: manifest.BackupName,
		Source:     group.Source, Grant: original, Resources: manifest.Resources})
	if err != nil {
		return model.SeedMarker{}, h.park(slot, parkReason(err), err)
	}
	return installed, nil
}

func (h *Host) beginRecoveryCreation(ctx context.Context, slot *replica, backend recoveryStore) error {
	probe, err := backend.Probe(ctx, slot.identity.GroupID)
	if err != nil {
		return h.park(slot, parkReason(err), err)
	}
	mode := freshReplicaStorage
	if probe.Exists {
		mode = retainedReplicaStorage
	}
	return h.beginStartupEffect(slot, mode)
}
