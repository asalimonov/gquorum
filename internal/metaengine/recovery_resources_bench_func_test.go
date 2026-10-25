//go:build functional

package metaengine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

// Use a compiled functional binary with -test.benchtime=1x. The fixture derives
// reachable cleanup debt from real approval/consumption evidence, not fake rows.
func BenchmarkRecoveryMetadataResources(b *testing.B) {
	for _, count := range []int{0, 10000} {
		b.Run(fmt.Sprintf("cleanup-debts-%d", count), func(b *testing.B) {
			for range b.N {
				benchmarkRecoveryMetadata(b, count)
			}
		})
	}
}
func benchmarkRecoveryMetadata(b *testing.B, count int) {
	b.StopTimer()
	machine := cleanupDebtFixture(b, count)
	application, err := machine.Snapshot(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	index, term := machine.AppliedState()
	conf := raftpb.ConfState{Voters: []uint64{1}}
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: machine.Latest().ClusterID,
		GroupID: topology.MetaGroupID, Application: application})
	if err != nil {
		b.Fatal(err)
	}
	snapshot := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: conf}}
	path := filepath.Join(b.TempDir(), "_meta.bolt")
	store, err := metastore.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := store.EnsureIdentity("n1", machine.Latest().ClusterID, 1); err != nil {
		b.Fatal(err)
	}
	if err := store.InstallSnapshot(snapshot, raftpb.HardState{Term: term, Commit: index}); err != nil {
		b.Fatal(err)
	}
	command, err := Encode(GrantSession{ProposalID: "resource", SessionID: "resource", NodeName: "n1", InstanceID: "resource", TTLSeconds: 30})
	if err != nil {
		b.Fatal(err)
	}
	entry, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application,
		Correlation: "resource", RequestID: "resource", Data: command})
	if err != nil {
		b.Fatal(err)
	}
	if err := store.SaveReady([]raftpb.Entry{{Index: index + 1, Term: term, Data: entry}}, raftpb.HardState{Term: term, Commit: index + 1}); err != nil {
		b.Fatal(err)
	}
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	logical := metadataResourceBytes(b, path)
	defaults, _ := model.ResolveResources(model.Resources{})
	b.Logf("debts=%d materialized=%d snapshot_data=%d tail=%d logical_all_records=%d default_max_metadata=%d S=%d/%d C=%d/%d",
		count, len(application), len(data), len(entry), logical, defaults.MaxMetadataBytes, index, term, index+1, term)
	b.ReportMetric(float64(logical), "logical-metadata-bytes")
	b.ReportMetric(float64(len(application)), "materialized-bytes")
	b.ReportAllocs()
	b.StartTimer()
	reader, err := metastore.OpenReadOnly(b.Context(), path, defaults.MaxMetadataBytes)
	if err != nil {
		b.Fatalf("default metadata qualification failed (logical=%d limit=%d): %v", logical, defaults.MaxMetadataBytes, err)
	}
	retained, _, err := reader.Recovery(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	recovered := mustMachine(b, machine.rules, nil)
	_, err = raftengine.Replay(b.Context(), raftengine.ReplayOptions{Identity: storage.Identity{NodeName: "n1", ClusterID: machine.Latest().ClusterID,
		GroupID: topology.MetaGroupID, ReplicaID: 1}, MemberID: 1, InstanceID: "resource-read"}, retained, recovered)
	if err != nil {
		b.Fatal(err)
	}
	inventory, err := recovered.RecoveryInventory(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if len(inventory.Archived.UnresolvedCleanup) != count {
		b.Fatal("cleanup evidence was truncated", len(inventory.Archived.UnresolvedCleanup))
	}
	if err := reader.Close(); err != nil {
		b.Fatal(err)
	}
	if _, err := metastore.OpenReadOnly(b.Context(), path, logical-1); err == nil {
		b.Fatal("one-byte-over metadata accepted")
	}
	b.StopTimer()
}
func metadataResourceBytes(b *testing.B, path string) uint64 {
	b.Helper()
	db, err := recoverydisk.OpenReadOnlyDB(context.Background(), path, 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	var total uint64
	var walk func(*bolt.Bucket) error
	walk = func(bucket *bolt.Bucket) error {
		return bucket.ForEach(func(key, value []byte) error {
			total += uint64(len(key) + len(value))
			if value == nil {
				return walk(bucket.Bucket(key))
			}
			return nil
		})
	}
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			total += uint64(len(name))
			return walk(bucket)
		})
	}); err != nil {
		b.Fatal(err)
	}
	return total
}
