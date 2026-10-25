package managed

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// SetRecovery installs the accepted barrier before any ordinary creation action.
func (h *Host) SetRecovery(ctx context.Context, state *model.ImportState, source model.LocalManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := model.ValidateManifest(source); err != nil {
		return err
	}
	if source.Request.Mode != model.Rebuild || source.SuccessorStartedAt == nil {
		return fault.ErrOperationAuthorization
	}
	identity := h.cfg.Identity()
	if identity.ClusterID != source.Identity.ClusterID || identity.NodeName != source.Identity.NodeName ||
		identity.MemberID != source.Identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	if state != nil && (state.RecoveryID != source.Request.RecoveryID || state.PlanDigest != source.PlanDigest) {
		return fault.ErrIdentityMismatch
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped || h.quiescing {
		return fault.ErrStopped
	}
	if h.recoveryManifest != nil && (h.recoveryManifest.PlanDigest != source.PlanDigest ||
		h.recoveryManifest.Request.RecoveryID != source.Request.RecoveryID) {
		return fault.ErrConflict
	}
	owned := model.CloneManifest(source)
	h.recoveryManifest = &owned
	if state != nil {
		ownedState := model.CloneImport(*state)
		h.recoveryImport = &ownedState
	}
	return nil
}

func (h *Host) recoveryStartAllowedLocked() error {
	if h.recoveryManifest != nil && !h.recoveryReleased {
		// Task12 installs the seed under the existing exact startup reservation.
		return fault.New(fault.Unsupported, "recovery seed/publication implementation pending", nil)
	}
	return nil
}

// PrepareRecoverySeed belongs to the normal owned startup, never a public bypass.
func (*Host) PrepareRecoverySeed(context.Context, load.ReplicaRef, topology.AuthorityGrant) error {
	return fault.New(fault.Unsupported, "recovery seed implementation pending", nil)
}

// RecoveryCheckpoint will wait for exact private readiness and portable durability.
func (*Host) RecoveryCheckpoint(context.Context, load.ReplicaRef) (model.CheckpointProof, error) {
	return model.CheckpointProof{}, fault.New(fault.Unsupported, "recovery checkpoint implementation pending", nil)
}

// PublishRecovery opens the data-validation barrier; nonempty seeds await Task12.
func (h *Host) PublishRecovery(ctx context.Context, identity topology.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if identity != h.cfg.Identity() {
		return fault.ErrIdentityMismatch
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.quiescing || h.stopped {
		return fault.ErrStopped
	}
	if h.recoveryManifest == nil {
		return fault.ErrOperationAuthorization
	}
	if len(h.recoveryManifest.Approved.LoadGroups()) > 0 {
		return fault.New(fault.Unsupported, "recovery publication implementation pending", nil)
	}
	return nil
}

// ReleaseRecoveryRun opens future ordinary starts after durable local completion.
func (h *Host) ReleaseRecoveryRun(ctx context.Context, identity topology.Identity) error {
	if err := h.PublishRecovery(ctx, identity); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.quiescing || h.stopped || h.ctx == nil || h.ctx.Err() != nil {
		return fault.ErrStopped
	}
	h.recoveryReleased = true
	return nil
}
