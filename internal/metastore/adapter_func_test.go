//go:build functional

package metastore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestMetaAdapterSnapshotTailAndExactScope(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	adapter := &Adapter{Store: store}
	ctx := context.Background()
	id := storage.Identity{NodeName: "node", ClusterID: "cluster", GroupID: topology.MetaGroupID, ReplicaID: 7}
	if err = adapter.BindReplica(ctx, id); err != nil {
		t.Fatal(err)
	}
	wrong := id
	wrong.ReplicaID++
	if err = adapter.BindReplica(ctx, wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if _, err = adapter.Load(ctx, "load"); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	snapshot := raftpb.Snapshot{Data: []byte("complete state"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{7}, Learners: []uint64{8}}}}
	if err = adapter.SaveSnapshot(ctx, topology.MetaGroupID, snapshot); err != nil {
		t.Fatal(err)
	}
	if err = adapter.SaveReady(ctx, topology.MetaGroupID, storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 2, Term: 1, Data: []byte("committed")}, {Index: 3, Term: 1, Data: []byte("uncommitted")}}, HardState: raftpb.HardState{Term: 1, Commit: 2}}); err != nil {
		t.Fatal(err)
	}
	if err = adapter.SaveApplied(ctx, topology.MetaGroupID, 3, snapshot.Metadata.ConfState); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("hint beyond commit accepted", err)
	}
	if err = adapter.SaveApplied(ctx, topology.MetaGroupID, 2, snapshot.Metadata.ConfState); err != nil {
		t.Fatal(err)
	}
	recovery, err := adapter.Load(ctx, topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Snapshot.Metadata.Index != 1 || recovery.AppliedHint != 2 || recovery.HardState.Commit != 2 || len(recovery.Entries) != 2 || recovery.Entries[0].Index != 2 {
		t.Fatal("hint hid required replay tail", recovery)
	}
	data, err := store.LoadState()
	if err != nil || string(data) != "complete state" {
		t.Fatal("snapshot was not materialized atomically", err)
	}
	cs, found, err := store.AppliedConfState()
	if err != nil || !found || len(cs.Learners) != 1 || cs.Learners[0] != 8 {
		t.Fatal(cs, err)
	}
	if _, err = adapter.Quarantine(ctx, topology.MetaGroupID, 7); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("uncoordinated meta quarantine accepted", err)
	}
}
