package disk

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// PendingStartup discovers the unique accepted operation under both leases.
// It binds WAL archive exclusions to validated system history before a transfer.
// It authorizes no destination acquisition; Node validates the exact startup mode.
func (o *Owner) PendingStartup(ctx context.Context) (*Journal, error) {
	if err := o.CheckRoots(); err != nil {
		return nil, err
	}
	records, err := readNamespace(ctx, o.Paths().StorageDir)
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(records))
	var pending *model.LocalManifest
	for i := range records {
		record := &records[i]
		names[record.BackupName] = true
		if record.Phase == model.RollingBack || record.Phase == model.RolledBack {
			return nil, fault.ErrUnsupported
		}
		if !terminalPhase(record.Phase) {
			if pending != nil {
				return nil, fault.New(fault.Conflict, "conflicting pending recoveries", nil)
			}
			pending = record
		}
	}
	o.captureRecoveryManifests(records)
	if err := o.bindStartupHistory(ctx, names); err != nil {
		return nil, err
	}
	if pending != nil {
		return o.openStartupJournal(ctx, pending)
	}

	if len(records) > 0 {
		if err := requireLiveSystemIdentity(o.Paths().StorageDir); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (o *Owner) bindStartupHistory(ctx context.Context, names map[string]bool) error {
	if o.wal == nil {
		return nil
	}
	if err := checkWALNamespace(ctx, o.wal.path, names); err != nil {
		return err
	}
	o.wal.mu.Lock()
	o.wal.history = names
	o.wal.mu.Unlock()
	return nil
}

// HasLiveLoadEvidence uses this owner's exact WAL descriptor and archive proof.
func (o *Owner) HasLiveLoadEvidence(ctx context.Context) (bool, error) {
	if o.wal == nil {
		return false, fault.ErrInvalid
	}
	return o.wal.HasLiveEvidence(ctx)
}

func (o *Owner) openStartupJournal(ctx context.Context, pending *model.LocalManifest) (*Journal, error) {
	switch pending.Phase {
	case model.Approved, model.Files, model.Cancelling, model.Archiving:
		return nil, fault.New(fault.Unavailable, "recovery_pending", nil)
	default:
		return Open(ctx, o, pending.Request.RecoveryID)
	}
}

func (o *Owner) captureRecoveryManifests(records []model.LocalManifest) {
	if o.wal != nil {
		o.wal.mu.Lock()
		o.wal.manifests = make(map[string]model.LocalManifest, len(records))
		for _, record := range records {
			o.wal.manifests[record.BackupName] = model.CloneManifest(record)
		}
		o.wal.mu.Unlock()
	}
}
