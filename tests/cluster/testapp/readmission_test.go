package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

func TestReadmissionVerifiesActualRetainedIDWithinEntirePreviousCatalog(t *testing.T) {
	t.Parallel()
	for _, retained := range []uint64{4, 20} {
		t.Run(map[uint64]string{4: "known-interrupted-target", 20: "foreign-id"}[retained], func(t *testing.T) {
			t.Parallel()
			cfg := config{NodeName: "local", NodeType: "worker", ClusterID: "cluster", InstanceID: "process", DataDir: t.TempDir(), Credential: "credential", MetaListen: "127.0.0.1:0", LoadListen: "127.0.0.1:0", HTTPListen: "127.0.0.1:0", Peers: []peer{{Name: "local", Type: "worker", MemberID: 1, MetaAddress: "127.0.0.1:1234", LoadAddress: "127.0.0.1:1235", HTTPAddress: "127.0.0.1:1236"}}, SessionTTL: 3 * time.Second, TickInterval: 50 * time.Millisecond}
			app, err := newApplication(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := app.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			g, err := raftengine.New(raftengine.Options{NodeName: cfg.NodeName, ClusterID: cfg.ClusterID, GroupID: "demo", ReplicaID: retained, Mode: raftengine.Founding, Peers: []raft.Peer{{ID: retained}}, Store: app.store, Machine: newMachine(), Transport: loadTransport{app: app, group: "demo", replica: retained}})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Construction and direct store recovery do not start Raft workers. Close
			// the meta database as the real admission path does before invoking hooks.
			before := app.meta.Identity()
			if err := app.meta.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			after := before
			after.MemberID = 2
			change := metaservice.Readmission{Previous: before, Granted: after, AdmissionID: "authorized", MetaDBPath: filepath.Join(cfg.DataDir, "meta.bolt"), PreviousGroups: []topology.Group{{ID: "demo", Replicas: []topology.Replica{{NodeName: cfg.NodeName, ReplicaID: 3}, {NodeName: cfg.NodeName, ReplicaID: 4}}}}, GrantedGroups: []topology.Group{{ID: "demo", Replicas: []topology.Replica{{NodeName: cfg.NodeName, ReplicaID: 7}}}}}
			err = app.readmission(t.Context(), change)
			probe, probeErr := app.store.Probe(t.Context(), "demo")
			if probeErr != nil {
				t.Fatal(probeErr)
			}
			if retained == 20 {
				if !errors.Is(err, fault.ErrIdentityMismatch) || !probe.Exists || probe.Identity.ReplicaID != 20 {
					t.Fatal("foreign state was changed", err, probe)
				}
				if _, found, err := metaservice.LoadStoredIdentity(change.MetaDBPath); err != nil || !found {
					t.Fatal("refusal moved meta pin", found, err)
				}
				return
			}
			if err != nil || probe.Exists {
				t.Fatal("historical ordering blocked exact known state", err, probe)
			}
			if err := app.readmission(t.Context(), change); err != nil {
				t.Fatal("completed quarantine is not idempotent", err)
			}
			backups, err := filepath.Glob(change.MetaDBPath + ".retired-*")
			if err != nil || len(backups) != 1 {
				t.Fatal(backups, err)
			}
		})
	}
}
