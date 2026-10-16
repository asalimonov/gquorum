package managed

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/topology"
)

// Start owns one maintenance worker through the authority lifetime. Fencing
// participation leaves reporting available; Stop/readmission cancel and join it.
// Caller holds Host.mu and has initialized both work lifetimes.
func (h *Host) startCollectionLoopLocked() {
	h.authorityDone = make(chan struct{})
	h.authorityCount++
	go func() {
		defer func() {
			h.mu.Lock()
			h.releaseWorkLocked(authorityWork)
			h.mu.Unlock()
		}()
		h.collectLoop(h.authorityContext)
	}()
}

func (h *Host) reportCollections(ctx context.Context) error {
	if h.cfg.Collection.Report == nil {
		return nil // The service callback is installed at the scoped cutover.
	}
	journal, err := h.authorityStore()
	if err != nil {
		return err
	}
	var failures []error
	var after string
	for {
		page, err := journal.Collections(ctx, after, 0)
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		for _, row := range page.Records {
			if row.State == "collected" {
				if err := h.reportCollection(ctx, row); err != nil {
					failures = append(failures, err)
				}
			}
		}
		if page.Next == "" {
			return errors.Join(failures...)
		}
		after = page.Next
	}
}

func (h *Host) reportCollection(parent context.Context, row authoritystore.CollectionRecord) error {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	owned, err := h.beginAuthorityOperation(ctx, row.Retired.Identity.GroupID)
	if err != nil {
		return err
	}
	defer owned.Release()
	journal, err := h.authorityStore()
	if err != nil {
		return err
	}
	current, err := journal.Collection(owned.Context, row.Retired.Identity)
	if err != nil || !current.Found {
		return err
	}
	if current.Record.Sequence != row.Sequence || current.Record.State != "collected" {
		return fault.ErrIdentityMismatch
	}
	record := current.Record
	receipt := authoritystore.CollectionReceipt{Identity: record.Retired.Identity, MemberID: record.MemberID,
		RetirementKey: record.Grant.Key, Token: record.Retired.Token, LocalSequence: record.Sequence,
		CollectedAt: *record.CollectedAt}
	result, err := h.cfg.Collection.Report(owned.Context, receipt)
	if err != nil {
		return err
	}
	if result.AppliedIndex == 0 || !result.Receipt.Equal(receipt) {
		return fault.ErrOperationAuthorization
	}
	return h.releaseCollection(owned.Context, journal, record, receipt)
}

func (h *Host) releaseCollection(ctx context.Context, journal *authoritystore.Store,
	row authoritystore.CollectionRecord, receipt authoritystore.CollectionReceipt) error {
	// Readmit takes this gate before installing any durable floor. Do not wait:
	// that owner may be joining this reporting work through closeAuthority.
	select {
	case h.readmissionGate <- struct{}{}:
		defer func() { <-h.readmissionGate }()
	default:
		return fault.ErrAuthorityPending
	}
	if err := collectionReadmissionReferences(ctx, journal, row); err != nil {
		return err
	}
	if err := h.cfg.Retirement.ForgetCollection(ctx, row.Retired); err != nil {
		return err
	}
	return journal.RemoveCollection(ctx, receipt)
}

// A current-member readmission floor survives a failed attempt/restart. Its
// references end only after a successor pin or a completed superseding fence.
// Historical completed readmission proof is retained, not a permanent blocker.
func collectionReadmissionReferences(ctx context.Context, journal *authoritystore.Store,
	row authoritystore.CollectionRecord) error {
	member := journal.Identity().MemberID
	for _, group := range []string{topology.MetaGroupID, row.Retired.Identity.GroupID} {
		floor, err := journal.Current(ctx, group)
		if err != nil {
			return err
		}
		if !floor.Found {
			continue
		}
		if !floor.Record.Complete {
			return fault.ErrAuthorityPending
		}
		if floor.Record.Grant.Purpose != topology.AuthorityReadmit {
			continue
		}
		for _, target := range floor.Record.Grant.Targets {
			if target.MemberID == member {
				return fault.ErrAuthorityPending
			}
		}
	}
	return ctx.Err()
}
