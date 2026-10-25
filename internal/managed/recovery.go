package managed

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
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
	if h.stopped || h.quiescing {
		h.mu.Unlock()
		return fault.ErrStopped
	}
	if h.recoveryManifest != nil && (h.recoveryManifest.PlanDigest != source.PlanDigest ||
		h.recoveryManifest.Request.RecoveryID != source.Request.RecoveryID) {
		h.mu.Unlock()
		return fault.ErrConflict
	}
	owned := model.CloneManifest(source)
	h.recoveryManifest = &owned
	if state != nil {
		ownedState := model.CloneImport(*state)
		h.recoveryImport = &ownedState
	}
	if h.recoveryGate == nil {
		h.recoveryGate = make(chan struct{}, 1)
		h.recoveryResumes = make(map[string]bool)
	}
	h.mu.Unlock()
	return h.recordConfirmedConsumption(ctx)
}
