package managed

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// The caller owns the group gate, including while retention waits. Preparation
// accepts deletion durably; subsequent policy decisions cannot revoke it.
func (h *Host) collectRetiredReplica(ctx context.Context, store storage.RetirementStore,
	retired storage.RetiredReplica) error {
	journal, err := h.authorityStore()
	if err != nil {
		return err
	}
	current, err := journal.Collection(ctx, retired.Identity)
	if err != nil {
		return err
	}
	if current.Found {
		return nil // The journal pass owns retries; enumeration must not repeat a failed attempt.
	}
	row, err := h.collectionCandidate(ctx, retired)
	if err != nil {
		return err
	}
	decision, err := h.retentionDecision(ctx, retired)
	if err != nil {
		decision.Reason = err.Error()
	}
	if err != nil || !decision.Collect || time.Now().Before(decision.NotBefore) {
		return errors.Join(err, h.deferCollection(ctx, row, decision))
	}
	if err := h.checkRetiredCollection(ctx, retired.Identity); err != nil {
		return err
	}
	row, err = journal.PrepareCollection(ctx, row)
	if err != nil {
		return err
	}
	return h.completePreparedCollection(ctx, store, row)
}

func (h *Host) collectionCandidate(ctx context.Context,
	retired storage.RetiredReplica) (authoritystore.CollectionRecord, error) {
	if err := h.checkRetiredCollection(ctx, retired.Identity); err != nil {
		return authoritystore.CollectionRecord{}, err
	}
	journal, err := h.authorityStore()
	if err != nil {
		return authoritystore.CollectionRecord{}, err
	}
	proof, err := journal.CompletedRetirement(ctx, retired.Identity)
	if err != nil {
		return authoritystore.CollectionRecord{}, err
	}
	if !proof.Found {
		return authoritystore.CollectionRecord{}, fault.ErrOperationAuthorization
	}
	// Complete local retirement proves permanent assignment exclusion: retired
	// replica IDs cannot be readmitted. A current Target read would strand an
	// excluded host without adding authorization (CLEANUP_PROOF).
	row := authoritystore.CollectionRecord{Retired: retired, MemberID: proof.Record.Target.MemberID,
		Grant: proof.Record.Grant, Retirement: *proof.Record.Receipt, State: "prepared", Sequence: 1}
	// Validate the candidate before callbacks; PrepareCollection allocates its real sequence.
	err = row.Validate(h.cfg.Identity())
	row.Sequence = 0
	return row, err
}

func (h *Host) deferCollection(ctx context.Context, row authoritystore.CollectionRecord,
	decision lifecycle.RetentionDecision) error {
	if h.cfg.Collection.Status == nil {
		return nil // Service callback is installed at the complete scoped cutover.
	}
	status := CleanupStatus{Identity: row.Retired.Identity, MemberID: row.MemberID,
		RetirementKey: row.Grant.Key, Blocked: &topology.BlockedReason{Code: "retention", Message: decision.Reason}}
	if !decision.NotBefore.IsZero() {
		status.NotBefore = &decision.NotBefore
	}
	return h.cfg.Collection.Status(ctx, status)
}

// Memory has no durable physical obligation or collection timestamp. Its exact
// retirement proof still gates retention, but its store-local marker is volatile.
func (h *Host) collectMemoryReplica(ctx context.Context, store storage.RetirementStore,
	retired storage.RetiredReplica) error {
	if _, err := h.collectionCandidate(ctx, retired); err != nil {
		return err
	}
	decision, err := h.retentionDecision(ctx, retired)
	if err != nil || !decision.Collect || time.Now().Before(decision.NotBefore) {
		return err
	}
	if err := h.checkRetiredCollection(ctx, retired.Identity); err != nil {
		return err
	}
	if err := store.Collect(ctx, retired); err != nil {
		return err
	}
	return store.ForgetCollection(ctx, retired)
}
