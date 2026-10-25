//go:build functional

package metastore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestOfflineMetaReaderRetainsPinHintAndLedgerWithoutWrites(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureIdentity("n1", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	snapshot := completeSnapshot(t, store, raftpb.Snapshot{Data: []byte("application"), Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}})
	if err := store.InstallSnapshot(snapshot, raftpb.HardState{Term: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveReady([]raftpb.Entry{{Index: 3, Term: 1, Data: []byte("committed")}, {Index: 4, Term: 1, Data: []byte("uncommitted")}}, raftpb.HardState{Term: 1, Commit: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordReplica("work", ReplicaRecord{NodeName: "n1", ClusterID: "cluster", ReplicaID: 9, Origin: "founding", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(t.Context(), path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	id, err := reader.Identity()
	if err != nil || id.NodeName != "n1" || id.MemberID != 1 {
		t.Fatal(id, err)
	}
	recovered, ledger, err := reader.Recovery(t.Context())
	if err != nil || recovered.HardState.Commit != 3 || recovered.AppliedHint != 2 || len(recovered.Entries) != 2 || ledger["work"].ReplicaID != 9 {
		t.Fatal(recovered, ledger, err)
	}
	readerCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := reader.Recovery(readerCtx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Identity(); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(t.Context(), path, 1); err == nil {
		t.Fatal("metadata bound ignored")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read-only metadata reader changed source", err)
	}
	absent := filepath.Join(t.TempDir(), "absent.bolt")
	if _, err := OpenReadOnly(t.Context(), absent, 1<<20); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := os.Stat(absent); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reader created absent source")
	}
}

func TestOfflineMetaLegacyAndRawScopedFixturesRemainUnchanged(t *testing.T) {
	t.Parallel()
	for _, fixture := range []string{"api-baseline", "scoped-v4"} {
		t.Run(fixture, func(t *testing.T) {
			t.Parallel()
			original := filepath.Join("..", "metaengine", "testdata", fixture, "meta.bolt")
			data, err := os.ReadFile(original)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "meta.bolt")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			reader, err := OpenReadOnly(t.Context(), path, 8<<20)
			if fixture == "api-baseline" {
				if !errors.Is(err, fault.ErrUnsupported) {
					t.Fatal("legacy source was initialized or migrated", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				retained, _, err := reader.Recovery(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				// The frozen scoped fixture captures FSM payloads directly. It was not
				// generated as a native engine-envelope fixture and must not be migrated.
				if _, err := raftstate.DecodeRaftSnapshot(retained.Snapshot, "c", topology.MetaGroupID); err == nil {
					t.Fatal("bare FSM payload accepted as native engine snapshot")
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, after) {
				t.Fatal("refusal rewrote source", err)
			}
		})
	}
}
