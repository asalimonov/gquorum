package managed

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
)

// Journal rows, not Retired enumeration or wakeups, own accepted attempts.
// An already collected row must never call Collect again: its marker may have
// been released before a crash, while the collected row remains for reporting.
func (h *Host) resumeCollections(ctx context.Context) error {
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
			if row.State == "prepared" {
				if err := h.resumeCollection(ctx, row); err != nil {
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

func (h *Host) resumeCollection(parent context.Context, row authoritystore.CollectionRecord) error {
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
	// Another owned caller may have completed this row while this one waited.
	current, err := journal.Collection(owned.Context, row.Retired.Identity)
	if err != nil || !current.Found {
		return err
	}
	if current.Record.Sequence != row.Sequence {
		return fault.ErrIdentityMismatch
	}
	if current.Record.State == "collected" {
		return nil
	}
	return h.completePreparedCollection(owned.Context, h.cfg.Retirement, current.Record)
}

// Caller holds the group gate throughout deletion and durable completion.
func (h *Host) completePreparedCollection(ctx context.Context, store storage.RetirementStore,
	row authoritystore.CollectionRecord) error {
	if row.State == "collected" {
		return nil
	}
	journal, err := h.authorityStore()
	if err != nil {
		return err
	}
	if err := h.checkRetiredCollection(ctx, row.Retired.Identity); err != nil {
		return err
	}
	if err := store.Collect(ctx, row.Retired); err != nil {
		return err
	}
	_, err = journal.CompleteCollection(ctx, row.Retired.Identity, row.Sequence, time.Now().UTC())
	return err
}
