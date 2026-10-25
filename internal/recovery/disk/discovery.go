package disk

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

const scanPageSize = 128

// List reads bounded atomic manifests without acquiring runtime leases or WALs.
// Valid records remain visible beside diagnostics; duplicate IDs are excluded.
func List(ctx context.Context, paths model.Paths) ([]model.LocalManifest, error) {
	records, err := scanManifests(ctx, paths.StorageDir)
	slices.SortFunc(records, func(a, b model.LocalManifest) int {
		if order := b.AcceptedAt.Compare(a.AcceptedAt); order != 0 {
			return order
		}
		return stringsCompare(a.Request.RecoveryID, b.Request.RecoveryID)
	})
	return records, err
}
func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Read selects a historical ID, or the exact current pointer for an empty ID.
func Read(ctx context.Context, paths model.Paths, id string) (model.LocalManifest, error) {
	if err := ctx.Err(); err != nil {
		return model.LocalManifest{}, err
	}
	if id != "" {
		return readByID(ctx, paths, id)
	}
	var pointer model.Current
	if err := readCurrent(paths.StorageDir, &pointer); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = fault.ErrHistoryUnavailable
		}
		return model.LocalManifest{}, err
	}
	if pointer.RecoveryID == "" {
		return model.LocalManifest{}, fault.ErrHistoryUnavailable
	}
	record, err := readByID(ctx, paths, pointer.RecoveryID)
	if record.BackupName != pointer.BackupName || record.PlanDigest != pointer.PlanDigest {
		return model.LocalManifest{}, errors.Join(fault.ErrUnreadable, err)
	}
	return record, err
}
func readByID(ctx context.Context, paths model.Paths, id string) (model.LocalManifest, error) {
	if err := model.ValidateRecoveryID(id); err != nil {
		return model.LocalManifest{}, err
	}
	records, scanErr := List(ctx, paths)
	for _, record := range records {
		if record.Request.RecoveryID == id {
			return record, scanErr
		}
	}
	return model.LocalManifest{}, errors.Join(fault.ErrHistoryUnavailable, scanErr)
}

func scanManifests(ctx context.Context, path string) ([]model.LocalManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := openReadRoot(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	namespace, err := openNamespace(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer namespace.Close()
	scan := manifestScan{root: root, path: path, counts: make(map[string]int)}
	for {
		entries, readErr := namespace.ReadDir(scanPageSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return scan.records, errors.Join(scan.err, err)
			}
			scan.add(entry)
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				scan.err = errors.Join(scan.err, readErr)
			}
			break
		}
	}
	return scan.finish()
}

type manifestScan struct {
	root    *os.Root
	path    string
	records []model.LocalManifest
	counts  map[string]int
	pointer *model.Current
	err     error
}

func (s *manifestScan) add(entry os.DirEntry) {
	relative := filepath.Join(Namespace, entry.Name())
	if approvalTemporary(entry.Name()) {
		s.err = errors.Join(s.err, validateApprovalTemporary(s.root, relative))
		return
	}
	if entry.Name() == "current.json" {
		var current model.Current
		if err := readRecordFrom(s.root, relative, &current, model.HeaderMaxBytes); err != nil {
			s.err = errors.Join(s.err, err)
			return
		}
		s.pointer = &current
		return
	}
	if !entry.IsDir() {
		s.err = errors.Join(s.err, fault.New(fault.Unreadable, "unowned recovery namespace entry", nil))
		return
	}
	var record model.LocalManifest
	path := filepath.Join(relative, "manifest.json")
	err := readRecordFrom(s.root, path, &record, model.ManifestMaxBytes)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return
	}
	s.counts[record.Request.RecoveryID]++
	if err := validateNamespaceRecord(s.path, entry.Name(), record); err != nil {
		s.err = errors.Join(s.err, err)
		return
	}
	s.records = append(s.records, record)
}
func (s *manifestScan) finish() ([]model.LocalManifest, error) {
	valid := s.records[:0]
	for _, record := range s.records {
		if s.counts[record.Request.RecoveryID] > 1 {
			s.err = errors.Join(s.err, fault.New(fault.Conflict, "duplicate recovery ID", nil))
			continue
		}
		valid = append(valid, record)
	}
	if s.pointer != nil {
		s.err = errors.Join(s.err, validateCurrent(*s.pointer, valid))
	}
	return valid, s.err
}
func openReadRoot(path string) (*os.Root, error) {
	if err := rejectRootLink(path); err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	if privateRoot(resolved) {
		return nil, fault.ErrInvalid
	}
	return os.OpenRoot(resolved)
}

func readCurrent(path string, pointer *model.Current) error {
	root, err := openReadRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return readRecordFrom(root, filepath.Join(Namespace, "current.json"), pointer, model.HeaderMaxBytes)
}

// Absence is an ordinary startup observation under the already-held root lease.
// Existing namespaces always enter the descriptor-based no-follow reader.
func openNamespace(root *os.Root) (*os.File, error) {
	info, err := os.Lstat(filepath.Join(root.Name(), Namespace))
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fault.ErrUnreadable
	}
	return openDirectory(root, Namespace)
}
