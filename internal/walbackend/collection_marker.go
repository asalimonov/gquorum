package walbackend

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

const collectionSuffix = ".collection"

type collectionMarker struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Retired       storage.RetiredReplica `json:"retired"`
}

func prepareCollectionRoot(path string) (os.FileInfo, error) {
	if err := os.MkdirAll(path, dirMode); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, unreadable("WAL root is not a directory", nil)
	}
	return info, nil
}

func (s *Store) validateCollectionRoot() error {
	info, err := os.Lstat(s.opts.RootDir)
	if err != nil {
		return unreadable("collection root is unavailable", err)
	}
	if s.rootInfo == nil || !info.IsDir() || !os.SameFile(info, s.rootInfo) {
		return unreadable("collection root changed since open", nil)
	}
	return nil
}

func (s *Store) validateCollectionRecord(record storage.RetiredReplica) error {
	base, ok := retirementToken(record.Token)
	if !ok || !base.At.Equal(record.QuarantinedAt) {
		return fault.ErrInvalid
	}
	want := record.Identity
	if want.NodeName != s.opts.NodeName || want.ClusterID != s.opts.ClusterID || want.GroupID != base.GroupID ||
		want.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func sameRetiredRecord(a, b storage.RetiredReplica) bool {
	return a.Identity == b.Identity && a.Token == b.Token && a.QuarantinedAt.Equal(b.QuarantinedAt)
}

// CLEANUP_CRASH: like TiKV's durable tombstone before data cleanup, persist the
// intent before deleting anything. Unlike a tombstone, this exact local marker
// is retained until the collection receipt is reported and proof is released.
// Caller holds Store.mu and has checked the record, open state and live owner.
func (s *Store) prepareCollectionMarker(record storage.RetiredReplica) (bool, error) {
	if err := s.validateCollectionRoot(); err != nil {
		return false, err
	}
	dir := filepath.Join(s.opts.RootDir, record.Token)
	marker, err := readCollectionMarker(dir + collectionSuffix)
	if err != nil {
		return false, err
	}
	if marker != nil && !sameRetiredRecord(marker.Retired, record) {
		return false, fault.ErrIdentityMismatch
	}
	if marker == nil {
		if err := startCollection(dir, record); err != nil {
			return false, err
		}
	}
	present, err := markedDirectoryMatches(dir, record.Identity)
	if err != nil {
		return false, err
	}
	// Retry a failed root sync even for an existing marker or absent directory.
	return present, syncDir(s.opts.RootDir)
}

func readCollectionMarker(path string) (*collectionMarker, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, unreadable("collection marker is not a regular file", nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, unreadable("collection marker changed while opening", err)
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var marker collectionMarker
	if err := decoder.Decode(&marker); err != nil {
		return nil, unreadable("invalid collection marker", err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, unreadable("trailing collection marker data", nil)
	}
	if marker.SchemaVersion != 1 {
		return nil, fault.ErrUnsupported
	}
	return &marker, nil
}

func writeCollectionMarker(path string, record storage.RetiredReplica) error {
	data, err := json.Marshal(collectionMarker{SchemaVersion: 1, Retired: record})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".collection-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Same-volume atomic publication cannot overwrite a conflicting marker.
	return os.Link(tmp.Name(), path)
}

func startCollection(dir string, record storage.RetiredReplica) error {
	present, err := retiredDirectoryMatches(dir, record.Identity)
	if err != nil {
		return err
	}
	if !present {
		return os.ErrNotExist // Absence alone never proves collection.
	}
	return writeCollectionMarker(dir+collectionSuffix, record)
}

func markedDirectoryMatches(dir string, want storage.Identity) (bool, error) {
	owner, err := retirementIdentity(dir)
	if err != nil {
		return false, err
	}
	if owner.Found {
		if owner.Identity != want {
			return false, &IdentityMismatchError{Stored: owner.Identity, Resolved: want}
		}
		return true, nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(entries) != 0 {
		return false, unreadable("marked retirement has unpinned contents", nil)
	}
	return true, nil // Exact marker plus empty directory is the pin-removal crash boundary.
}
