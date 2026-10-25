//go:build functional

package metaengine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestOfflineModernMetaReplayUsesNilPersisterThroughRetainedCommit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	opts := quorum.NodeOptions{StorageDir: root, Identity: topology.Identity{NodeName: "n1", ClusterID: "cluster", InstanceID: "source"}, Node: topology.Node{Name: "n1", Type: "worker"}, Bootstrap: quorum.BootstrapOptions{Founders: []quorum.FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}}, Meta: quorum.MetaOptions{Network: quorum.MetaNetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("credential")}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	node, err := quorum.NewNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := node.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := node.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	session, err := node.ActivateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.KeepAlive(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if err := node.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "_meta.bolt")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := metastore.OpenReadOnly(ctx, path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	pin, err := reader.Identity()
	if err != nil {
		t.Fatal(err)
	}
	retained, ledger, err := reader.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := raftstate.DecodeRaftSnapshot(retained.Snapshot, pin.ClusterID, topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Rules topology.Rules `json:"rules"`
	}
	if err := json.Unmarshal(decoded.Application, &header); err != nil {
		t.Fatal(err)
	}
	machine, err := metaengine.NewMachine(header.Rules, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ahead map[string]json.RawMessage
	if err := json.Unmarshal(decoded.Application, &ahead); err != nil {
		t.Fatal(err)
	}
	ahead["applied_index"], _ = json.Marshal(retained.HardState.Commit + 1)
	ahead["applied_term"], _ = json.Marshal(retained.HardState.Term)
	badApplication, _ := json.Marshal(ahead)
	badData, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: pin.ClusterID, GroupID: topology.MetaGroupID, Application: badApplication})
	if err != nil {
		t.Fatal(err)
	}
	bad := retained
	bad.Snapshot.Data = badData
	rejected, err := metaengine.NewMachine(header.Rules, nil)
	if err != nil {
		t.Fatal(err)
	}
	options := raftengine.ReplayOptions{Identity: storage.Identity{ClusterID: pin.ClusterID, NodeName: pin.NodeName, GroupID: topology.MetaGroupID, ReplicaID: pin.MemberID}, MemberID: pin.MemberID, InstanceID: "offline"}
	if _, err := raftengine.Replay(ctx, options, bad, rejected); err == nil {
		t.Fatal("application metadata ahead of native snapshot was accepted")
	}
	result, err := raftengine.Replay(ctx, raftengine.ReplayOptions{Identity: storage.Identity{ClusterID: pin.ClusterID, NodeName: pin.NodeName, GroupID: topology.MetaGroupID, ReplicaID: pin.MemberID}, MemberID: pin.MemberID, InstanceID: "offline"}, retained, machine)
	if err != nil || result.Index != retained.HardState.Commit || result.Index <= retained.Snapshot.Metadata.Index || len(ledger) != 0 {
		t.Fatal(result, ledger, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(source, after) {
		t.Fatal("nil-persister replay changed source database", err)
	}
}
