//go:build functional

package walstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

func TestCollectionMarkerPrecedesDeletionAndSurvivesRestart(t *testing.T) {
	t.Parallel()
	s, record := retiredFixture(t)
	dir := filepath.Join(s.opts.RootDir, record.Token)
	dataPath := filepath.Join(dir, "payload")
	check(t, os.WriteFile(dataPath, []byte("keep until marker"), fileMode))
	s.mu.Lock()
	present, err := s.prepareCollectionMarker(record)
	s.mu.Unlock()
	check(t, err)
	if !present {
		t.Fatal("marker preparation lost the directory")
	}
	marker, err := readCollectionMarker(dir + collectionSuffix)
	check(t, err)
	if marker == nil || !sameRetiredRecord(marker.Retired, record) {
		t.Fatal("marker did not bind full exact record", marker)
	}
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatal("marker preparation deleted data", err)
	}
	data, err := os.ReadFile(dir + collectionSuffix)
	check(t, err)
	var shape map[string]json.RawMessage
	check(t, json.Unmarshal(data, &shape))
	if len(shape) != 2 || string(shape["schema_version"]) != "1" {
		t.Fatal("unexpected marker schema", string(data))
	}
	var exact storage.RetiredReplica
	check(t, json.Unmarshal(shape["retired"], &exact))
	if !sameRetiredRecord(exact, record) {
		t.Fatal("RetiredReplica encoding changed")
	}
	check(t, s.Close())
	s = openTest(t, s.opts.RootDir, "a", "test-cluster", false)
	check(t, s.Collect(t.Context(), record))
	check(t, s.Collect(t.Context(), record))
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retired data remained", err)
	}
	after, err := os.ReadFile(dir + collectionSuffix)
	check(t, err)
	if !bytes.Equal(data, after) {
		t.Fatal("collection retry changed its marker")
	}
	rows, err := s.Retired(t.Context())
	check(t, err)
	if len(rows) != 0 {
		t.Fatal("marker was enumerated as retired data", rows)
	}
}

func TestCollectionMarkerRefusesCorruptionAndConflictsWithoutDeletion(t *testing.T) {
	t.Parallel()
	modes := []string{"garbage", "unknown field", "trailing", "schema", "identity", "token", "time", "symlink", "directory"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			dir := filepath.Join(s.opts.RootDir, record.Token)
			path := dir + collectionSuffix
			marker := collectionMarker{SchemaVersion: 1, Retired: record}
			var want error = fault.ErrUnreadable
			switch mode {
			case "schema":
				marker.SchemaVersion = 2
				want = fault.ErrUnsupported
			case "identity":
				marker.Retired.Identity.ReplicaID++
				want = fault.ErrIdentityMismatch
			case "token":
				marker.Retired.Token += "0"
				want = fault.ErrIdentityMismatch
			case "time":
				marker.Retired.QuarantinedAt = record.QuarantinedAt.Add(time.Nanosecond)
				want = fault.ErrIdentityMismatch
			}
			data, _ := json.Marshal(marker)
			switch mode {
			case "garbage":
				data = []byte("invalid")
			case "unknown field":
				data = append(data[:len(data)-1], []byte(`,"unknown":1}`)...)
			case "trailing":
				data = append(data, []byte(" {}")...)
			}
			if mode == "symlink" {
				target := filepath.Join(t.TempDir(), "external-marker")
				check(t, os.WriteFile(target, data, fileMode))
				check(t, os.Symlink(target, path))
			} else if mode == "directory" {
				check(t, os.Mkdir(path, dirMode))
			} else {
				check(t, os.WriteFile(path, data, fileMode))
			}
			if err := s.Collect(t.Context(), record); !errors.Is(err, want) {
				t.Fatal("bad marker accepted", err, want)
			}
			owner, err := readIdentity(dir)
			if err != nil || !owner.Found || owner.Identity != record.Identity {
				t.Fatal("marker refusal damaged retained data", owner, err)
			}
		})
	}
}

func TestCollectionMarkerRequiresOriginalRootAndExistingData(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing data", "missing root", "replaced root", "linked root"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			root := s.opts.RootDir
			var want error = fault.ErrUnreadable
			if mode == "missing data" {
				check(t, os.RemoveAll(filepath.Join(root, record.Token)))
				want = os.ErrNotExist
			} else {
				moved := filepath.Join(t.TempDir(), "original-root")
				check(t, os.Rename(root, moved))
				if mode == "replaced root" {
					check(t, os.Mkdir(root, dirMode))
					// Even an exact copied marker cannot bless a swapped root.
					check(t, writeCollectionMarker(filepath.Join(root, record.Token)+collectionSuffix, record))
				} else if mode == "linked root" {
					check(t, os.Symlink(moved, root))
				}
				defer func() {
					_ = os.RemoveAll(root)
					check(t, os.Rename(moved, root))
				}()
			}
			if err := s.Collect(t.Context(), record); !errors.Is(err, want) {
				t.Fatal("absence or swapped root manufactured proof", err, want)
			}
		})
	}
}

func TestCollectionMarkerPublicationNeverOverwritesEvidence(t *testing.T) {
	t.Parallel()
	s, record := retiredFixture(t)
	dir := filepath.Join(s.opts.RootDir, record.Token)
	path := dir + collectionSuffix
	original := []byte("conflicting evidence")
	check(t, os.WriteFile(path, original, fileMode))
	if err := writeCollectionMarker(path, record); !errors.Is(err, os.ErrExist) {
		t.Fatal("marker publication overwrote existing evidence", err)
	}
	retained, err := os.ReadFile(path)
	check(t, err)
	if !bytes.Equal(original, retained) {
		t.Fatal("failed publication changed the marker")
	}
	if _, err := os.Stat(filepath.Join(dir, IdentityFile)); err != nil {
		t.Fatal("failed publication deleted retired data", err)
	}
	if err := s.Collect(t.Context(), record); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("failed marker publication allowed collection", err)
	}
}

func TestCollectionMarkerRejectsMalformedTokens(t *testing.T) {
	t.Parallel()
	s, original := retiredFixture(t)
	for _, token := range []string{"group.quarantine-01", "group.quarantine--1", "group.quarantine-+1000", "group.quarantine-1000.collection", "group.quarantine-1000/../other", "group.quarantine-1000\x00"} {
		record := original
		record.Token = token
		if err := s.Collect(t.Context(), record); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("malformed token accepted", token, err)
		}
	}
	if _, err := os.Stat(filepath.Join(s.opts.RootDir, original.Token, IdentityFile)); err != nil {
		t.Fatal("invalid token damaged original data", err)
	}
}
