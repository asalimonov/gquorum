package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// CheckStartup validates the archive namespace before ordinary store acquisition.
// Pending work stays guarded until the accepted recovery startup path is installed.
func CheckStartup(ctx context.Context, paths model.Paths) error {
	_, err := startupHistory(ctx, paths)
	return err
}

// CheckStartup binds validated system history to this owner's WAL lease.
func (o *Owner) CheckStartup(ctx context.Context, paths model.Paths) error {
	names, err := startupHistory(ctx, paths)
	if err != nil {
		return err
	}
	if o.wal != nil && len(names) > 0 {
		o.wal.history = names
	}
	return nil
}

func startupHistory(ctx context.Context, paths model.Paths) (map[string]bool, error) {
	records, err := readNamespace(ctx, paths.StorageDir)
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(records))
	for _, record := range records {
		names[record.BackupName] = true
		if record.Phase == model.RollingBack || record.Phase == model.RolledBack {
			return nil,
				fault.New(fault.Unsupported, "unsupported recovery rollback state", nil)
		}
		if !terminalPhase(record.Phase) {
			return nil, fault.New(fault.Unavailable, "recovery_pending", nil)
		}
	}
	if len(records) > 0 {
		if err := requireLiveSystemIdentity(paths.StorageDir); err != nil {
			return nil, err
		}
	}
	if paths.LoadWALDir != "" {
		if err := checkWALNamespace(ctx, paths.LoadWALDir, names); err != nil {
			return nil, err
		}
	}
	return names, nil
}

// A completed archive is not authority to refound a lost destination identity.
func requireLiveSystemIdentity(root string) error {
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return fault.New(fault.Unreadable, "recovery history has no live system identity", err)
		}
		if !info.Mode().IsRegular() {
			return fault.New(fault.Unreadable, "invalid live system identity", nil)
		}
	}
	return nil
}

func readNamespace(ctx context.Context, root string) ([]model.LocalManifest, error) {
	return scanManifests(ctx, root)
}

func terminalPhase(phase model.Phase) bool {
	return phase == model.Complete || phase == model.Cancelled || phase == model.Joined
}

func validateNamespaceRecord(root, name string, r model.LocalManifest) error {
	if err := model.ValidateManifest(r); err != nil {
		return err
	}
	if err := model.ValidateRequest(r.Request); err != nil {
		return fault.New(fault.Unreadable, "invalid archived request", err)
	}
	expected, err := model.BackupName(r.AcceptedAt, r.Request.RecoveryID)
	if err != nil || expected != name || r.BackupName != name {
		return fault.New(fault.Unreadable, "recovery backup binding mismatch", err)
	}
	sum, err := hex.DecodeString(r.PlanDigest)
	if err != nil || len(sum) != sha256.Size || r.Acceptance.PlanDigest != r.PlanDigest {
		return fault.ErrUnreadable
	}
	same, err := samePath(root, r.Paths.StorageDir)
	if err != nil || !same {
		return fault.New(fault.Unreadable, "recovery root binding mismatch", err)
	}
	return validateHistoryPhase(r)
}

func validateHistoryPhase(r model.LocalManifest) error {
	switch r.Phase {
	case model.Approved, model.Files, model.Cancelling, model.Archiving,
		model.AwaitingStart, model.Importing, model.Validating, model.JoinReady,
		model.RollingBack, model.RolledBack:
		return nil
	case model.Complete:
		if r.CompletedAt == nil {
			return fault.ErrUnreadable
		}
	case model.Cancelled:
		if r.CancelledAt == nil || r.Cancellation == nil {
			return fault.ErrUnreadable
		}
		if r.Files.Backup.Attempt > 0 && r.Files.Undo.State != "complete" {
			return fault.ErrUnreadable
		}
	case model.Joined:
		if r.JoinedIdentity == nil || r.JoinedIdentity.MemberID == 0 {
			return fault.ErrUnreadable
		}
	default:
		return fault.ErrUnreadable
	}
	return nil
}

func validateCurrent(current model.Current, records []model.LocalManifest) error {
	if current.RecoveryID == "" && current.PlanDigest == "" && current.BackupName == "" {
		return nil
	}
	for _, r := range records {
		if r.Request.RecoveryID == current.RecoveryID && r.PlanDigest == current.PlanDigest &&
			r.BackupName == current.BackupName {
			return nil
		}
	}
	return fault.New(fault.Unreadable, "recovery pointer has no matching manifest", nil)
}

func checkWALNamespace(ctx context.Context, root string, names map[string]bool) error {
	dir := filepath.Join(root, Namespace)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fault.ErrUnreadable
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() || !names[entry.Name()] {
			return fault.New(fault.Unreadable, "unowned WAL recovery namespace", nil)
		}
	}
	return nil
}

func samePath(a, b string) (bool, error) {
	first, err := filepath.EvalSymlinks(a)
	if err != nil {
		return false, err
	}
	second, err := filepath.EvalSymlinks(b)
	if err != nil {
		return false, err
	}
	first, err = filepath.Abs(first)
	if err != nil {
		return false, err
	}
	second, err = filepath.Abs(second)
	if err != nil {
		return false, err
	}
	return first == second, nil
}
