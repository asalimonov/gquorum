package recoverydisk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
)

// CheckStartup validates the archive namespace before ordinary store acquisition.
// Pending work stays guarded until the accepted recovery startup path is installed.
func CheckStartup(ctx context.Context, paths recoverymodel.Paths) error {
	_, err := startupHistory(ctx, paths)
	return err
}

// CheckStartup binds validated system history to this owner's WAL lease.
func (o *Owner) CheckStartup(ctx context.Context, paths recoverymodel.Paths) error {
	names, err := startupHistory(ctx, paths)
	if err != nil {
		return err
	}
	if o.wal != nil && len(names) > 0 {
		o.wal.history = names
	}
	return nil
}

func startupHistory(ctx context.Context, paths recoverymodel.Paths) (map[string]bool, error) {
	records, err := readNamespace(ctx, paths.StorageDir)
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(records))
	for _, record := range records {
		names[record.BackupName] = true
		if !terminalPhase(record.Phase) {
			return nil, fault.New(fault.Unavailable, "recovery_pending", nil)
		}
	}
	if len(records) > 0 {
		// A completed archive is not authority to refound a lost destination identity.
		for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
			info, err := os.Lstat(filepath.Join(paths.StorageDir, name))
			if err != nil {
				return nil, fault.New(fault.Unreadable, "recovery history has no live system identity", err)
			}
			if !info.Mode().IsRegular() {
				return nil, fault.New(fault.Unreadable, "invalid live system identity", nil)
			}
		}
	}
	if paths.LoadWALDir != "" {
		if err := checkWALNamespace(ctx, paths.LoadWALDir, names); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func readNamespace(ctx context.Context, root string) ([]recoverymodel.LocalManifest, error) {
	dir := filepath.Join(root, Namespace)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fault.New(fault.Unreadable, "invalid recovery namespace", nil)
	}
	scan := namespaceScan{root: root, dir: dir, ids: make(map[string]bool)}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := scan.add(entry); err != nil {
			return nil, err
		}
	}
	if len(scan.records) == 0 {
		return nil, fault.New(fault.Unreadable, "recovery namespace has no owned manifest", nil)
	}
	if scan.current != nil {
		if err := validateCurrent(*scan.current, scan.records); err != nil {
			return nil, err
		}
	}
	return scan.records, nil
}

type namespaceScan struct {
	root, dir string
	records   []recoverymodel.LocalManifest
	ids       map[string]bool
	current   *recoverymodel.Current
}

func (s *namespaceScan) add(entry os.DirEntry) error {
	if entry.Name() == "current.json" {
		value := new(recoverymodel.Current)
		if err := readRecord(filepath.Join(s.dir, entry.Name()), value, recoverymodel.HeaderMaxBytes); err != nil {
			return err
		}
		if value.Schema != recoverymodel.SchemaVersion {
			return fault.ErrUnreadable
		}
		s.current = value
		return nil
	}
	if !entry.IsDir() {
		return fault.New(fault.Unreadable, "unrecognized recovery namespace entry", nil)
	}
	var record recoverymodel.LocalManifest
	path := filepath.Join(s.dir, entry.Name(), "manifest.json")
	if err := readRecord(path, &record, recoverymodel.ManifestMaxBytes); err != nil {
		return err
	}
	if err := validateNamespaceRecord(s.root, entry.Name(), record); err != nil {
		return err
	}
	if s.ids[record.Request.RecoveryID] {
		return fault.New(fault.Unreadable, "duplicate recovery identity", nil)
	}
	s.ids[record.Request.RecoveryID] = true
	s.records = append(s.records, record)
	return nil
}

func terminalPhase(phase recoverymodel.Phase) bool {
	return phase == recoverymodel.Complete || phase == recoverymodel.Cancelled || phase == recoverymodel.Joined
}

func validateNamespaceRecord(root, name string, r recoverymodel.LocalManifest) error {
	if r.Schema != recoverymodel.SchemaVersion || r.Revision == 0 || r.Request.RecoveryID == "" {
		return fault.ErrUnreadable
	}
	if err := recoverymodel.ValidateRequest(r.Request); err != nil {
		return fault.New(fault.Unreadable, "invalid archived request", err)
	}
	expected, err := recoverymodel.BackupName(r.AcceptedAt, r.Request.RecoveryID)
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

func validateHistoryPhase(r recoverymodel.LocalManifest) error {
	switch r.Phase {
	case recoverymodel.Approved, recoverymodel.Files, recoverymodel.Cancelling, recoverymodel.Archiving,
		recoverymodel.AwaitingStart, recoverymodel.Importing, recoverymodel.Validating, recoverymodel.JoinReady,
		recoverymodel.RollingBack, recoverymodel.RolledBack:
		return nil
	case recoverymodel.Complete:
		if r.CompletedAt == nil {
			return fault.ErrUnreadable
		}
	case recoverymodel.Cancelled:
		if r.CancelledAt == nil || r.Cancellation == nil {
			return fault.ErrUnreadable
		}
		if r.Files.Backup.Attempt > 0 && r.Files.Undo.State != "complete" {
			return fault.ErrUnreadable
		}
	case recoverymodel.Joined:
		if r.JoinedIdentity == nil || r.JoinedIdentity.MemberID == 0 {
			return fault.ErrUnreadable
		}
	default:
		return fault.ErrUnreadable
	}
	return nil
}

func validateCurrent(current recoverymodel.Current, records []recoverymodel.LocalManifest) error {
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

func readRecord(path string, value any, maxBytes uint64) error {
	if maxBytes > math.MaxInt64-1 {
		return fault.ErrInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > int64(maxBytes) {
		return fault.ErrUnreadable
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, int64(maxBytes)+1))
	err = errors.Join(readErr, f.Close())
	if err != nil {
		return err
	}
	return recoverymodel.DecodeStrict(data, value, maxBytes)
}

func samePath(a, b string) (bool, error) {
	first, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	second, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	return first == second, nil
}
