//go:build functional

package metaengine_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestBaselineNativeMetadataSnapshotAndTailRestart(t *testing.T) {
	t.Parallel()
	root := copyNativeBaseline(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(root, "_meta.bolt")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := metastore.OpenReadOnly(ctx, path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	retained, _, err := reader.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retained.Snapshot.Metadata.Index != 33 || retained.HardState.Commit != 35 || len(retained.Entries) != 2 {
		t.Fatal("baseline fixture lost its actual native snapshot and committed tail", retained)
	}
	state, err := raftstate.DecodeRaftSnapshot(retained.Snapshot, "cluster", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Rules topology.Rules `json:"rules"`
	}
	if err := json.Unmarshal(state.Application, &header); err != nil {
		t.Fatal(err)
	}
	machine, err := metaengine.NewMachine(header.Rules, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := raftengine.Replay(ctx, raftengine.ReplayOptions{
		Identity: storage.Identity{ClusterID: "cluster", NodeName: "n1", GroupID: topology.MetaGroupID, ReplicaID: 1},
		MemberID: 1, InstanceID: "baseline-replay",
	}, retained, machine)
	if err != nil || result.Index != 35 {
		t.Fatal("baseline native replay refused", result, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("offline baseline replay mutated source", err)
	}
	// The approval process can die while writing its private staging manifest.
	// This unpublished data must not prevent ordinary A from restarting.
	staging := filepath.Join(root, recoverydisk.Namespace, "@approve-"+strings.Repeat("c", 32))
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "manifest.json"), []byte(`{"schema":`), 0o600); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	node, err := quorum.NewNode(quorum.NodeOptions{
		StorageDir: root,
		Identity:   topology.Identity{NodeName: "n1", ClusterID: "cluster", MemberID: 1, InstanceID: "baseline-restart"},
		Node:       topology.Node{Name: "n1", Type: "worker"},
		Meta: quorum.MetaOptions{Network: quorum.MetaNetworkOptions{
			Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true,
			ClusterCredential: []byte("credential"),
		}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := node.Stop(stop); err != nil {
			t.Error(err)
		}
	})
	if err := node.Start(t.Context()); err != nil {
		t.Fatal("baseline ordinary restart refused", err)
	}
	if err := node.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := node.ActivateSession(ctx); err != nil {
		t.Fatal(err)
	}
}

func copyNativeBaseline(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, fixture := range []struct{ name, digest string }{
		{"_meta.bolt", "c65821529d4460ea7a789cb36f1fc97d88d7fac58dc54ffd816336ee1a192a9d"},
		{"_authority.bolt", "b108d9a7deef3f64d077753b12259c898b9bc75880cb2b5ad2545b5d4ca9fd82"},
	} {
		compressed, err := os.ReadFile(filepath.Join("testdata", "native-2684202", fixture.name+".gz"))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != fixture.digest {
			t.Fatal("frozen baseline fixture changed", fixture.name)
		}
		if err := os.WriteFile(filepath.Join(root, fixture.name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
