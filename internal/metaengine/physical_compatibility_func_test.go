//go:build functional

package metaengine

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

type baselineTransport struct{}

func (baselineTransport) Send(ctx context.Context, _ []raftpb.Message) error { return ctx.Err() }

func physicalRows(t *testing.T, path string) map[string]string {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := map[string]string{}
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(key, value []byte) error {
				rows[string(name)+"/"+string(key)] = string(value)
				return nil
			})
		})
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestBaselinePhysicalMetaStoreRestoresWithoutRewritingRecords(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "api-baseline", "meta.bolt")
	before := physicalRows(t, fixture)
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "meta.bolt")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, found, err := metastore.LoadIdentity(path)
	if err != nil || !found || identity != (metastore.Identity{NodeName: "n1", ClusterID: "compat-cluster", MemberID: 1}) {
		t.Fatal("baseline pin", identity, found, err)
	}
	db, err := metastore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if seen, err := db.MemberSeen(); err != nil || !seen {
		t.Fatal("membership marker", seen, err)
	}
	if override, err := db.LoadOverride(); err != nil || override != 2 {
		t.Fatal("serve override", override, err)
	}
	want := metastore.ReplicaRecord{NodeName: "n1", ClusterID: "compat-cluster", ReplicaID: 7, Origin: "fresh", CreatedAt: time.Unix(100, 0).UTC()}
	if got, found, err := db.LookupReplica("work"); err != nil || !found || got != want {
		t.Fatal("baseline ledger", got, found, err)
	}
	rules := topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}
	m := NewMachine(rules, db)
	g, err := raftengine.NewMeta(raftengine.Options{NodeName: "n1", ClusterID: "compat-cluster", GroupID: topology.MetaGroupID, ReplicaID: 1, Mode: raftengine.Joining, Store: &metastore.Adapter{Store: db}, Machine: m, Transport: baselineTransport{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal("baseline engine recovery", err)
	}
	defer g.Stop(context.Background())
	latest, target, active := m.Views()
	if latest.Version != 1 || target.Version != 1 || active.Version != 1 || len(active.Nodes) != 3 || active.ClusterID != "compat-cluster" {
		t.Fatal("lost recovered topology", latest, target, active)
	}
	operator, ok := m.CurrentOperator()
	if !ok || operator.Status.ID != "exec-v1-a1" || operator.Status.State != topology.OperatorSucceeded {
		t.Fatal("lost operator", operator)
	}
	admission, ok := m.Admission("n1", "process-1")
	if !ok || admission.MemberID != 1 || admission.State != AdmissionCompleted {
		t.Fatal("lost admission", admission)
	}
	session, ok := m.LockSession("node/n1")
	if !ok || session.SessionID != "session-1" || session.InstanceID != "process-1" {
		t.Fatal("lost session/lock", session)
	}
	snapshot, err := m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(snapshot, baselineBytes(t, "snapshot.json")) {
		t.Fatal("recovered snapshot encoding changed", err)
	}
	if err := g.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if after := physicalRows(t, path); !maps.Equal(before, after) {
		t.Fatal("recovery rewrote persisted records", before, after)
	}
}
