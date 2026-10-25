//go:build functional

package gquorum

import (
	"path/filepath"
	"reflect"
	"testing"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestRecoveryInspectionReplaysOnlyCommitDespiteUnsyncedNewerTermTail(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, true)
	root := opts.Loads.Storage.WAL.RootDir
	source, err := walbackend.ReadRecovery(t.Context(), root, "work", model.ReadBounds{MaxRecordBytes: 4 << 20, MaxGroupBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	point := walpb.Snapshot{Index: source.Recovery.Snapshot.Metadata.Index, Term: source.Recovery.Snapshot.Metadata.Term}
	writer, err := wal.Open(zap.NewNop(), filepath.Join(root, "work", walbackend.WALDir), point)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := writer.ReadAll(); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	entry := raftpb.Entry{Index: source.LastLog.Index + 1, Term: source.Recovery.HardState.Term + 1, Data: []byte("not a committed engine envelope")}
	if err := writer.Save(raftpb.HardState{}, []raftpb.Entry{entry}); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	before := sourceFileHashes(t, opts.StorageDir)
	facade, err := NewRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "recover retained committed prefix"})
	if err != nil {
		t.Fatal(plan, err)
	}
	found := false
	for _, issue := range plan.Issues {
		if issue.Code == "unsynced_tail_term" && issue.GroupID == "work" && !issue.Blocking {
			found = true
		}
	}
	if !found {
		t.Fatal("unsynced evidence was hidden", plan.Issues)
	}
	for _, group := range plan.Groups {
		if group.GroupID == "work" && (!group.Usable || group.Commit.Index != source.Recovery.HardState.Commit || group.LastLog.Index != entry.Index || group.LastLog.Term != entry.Term) {
			t.Fatal("commit was inferred or unsynced tail interpreted", group)
		}
	}
	if _, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
		t.Fatal("inspection or approval truncated unsynced source evidence")
	}
}

func TestRecoveryUnknownLiveWALBlocksAndReportsRetainedPositions(t *testing.T) {
	t.Parallel()
	opts := recoveryFixtureOptions(t, true)
	store, err := walbackend.Open(walbackend.Options{RootDir: opts.Loads.Storage.WAL.RootDir, NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	id := storage.Identity{ClusterID: "cluster", NodeName: "n1", GroupID: "unrecognized", ReplicaID: 77}
	if err := store.BindReplica(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveReady(t.Context(), id.GroupID, storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 1, Term: 1}}, HardState: raftpb.HardState{Term: 1, Vote: id.ReplicaID, Commit: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(t.Context(), id.GroupID, raftpb.Snapshot{Data: []byte("opaque snapshot"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{id.ReplicaID}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	facade, err := NewRecovery(opts)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "must classify every local WAL", RequiredGroups: []string{topology.MetaGroupID}})
	if err == nil {
		t.Fatal("unknown live WAL was silently archived", plan)
	}
	positions := false
	covered := false
	for _, group := range plan.Groups {
		if group.GroupID == id.GroupID && !group.Usable && group.Source == id && group.Snapshot.Index == 1 && group.Commit.Index == 1 && group.LastLog.Index == 1 {
			positions = true
		}
		if group.GroupID == "work" && group.Usable {
			covered = true
		}
	}
	if !positions || !covered {
		t.Fatal("unknown positions or additive approved coverage were omitted", plan.Groups)
	}
}
