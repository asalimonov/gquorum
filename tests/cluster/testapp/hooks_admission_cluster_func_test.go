//go:build functional

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestAdmissionPinDriverPreservesRetainedEvidenceBeforeJoining(t *testing.T) {
	for _, kind := range []string{"metadata", "authority"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			cfg := config{DataDir: t.TempDir(), AdmissionPinPhase: "pending", Join: true, NodeName: "one", ClusterID: "cluster", InstanceID: "process", Peers: []peer{{MetaAddress: "127.0.0.1:1"}}}
			path := filepath.Join(cfg.DataDir, "_authority.bolt")
			if kind == "metadata" {
				path = filepath.Join(cfg.DataDir, "_meta.bolt")
				store, err := metastore.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(store.EnsureIdentity(cfg.NodeName, cfg.ClusterID, 4), store.Close()); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte("retained or foreign authority evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := cfg.prepareAdmissionPin(); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("protocol driver ignored retained evidence or attempted admission", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refused driver altered retained identity/evidence", err)
			}
		})
	}
}

func TestAdmissionPinObservationReadsActualUnacquiredMemberWithoutStartingNode(t *testing.T) {
	t.Parallel()
	id := topology.Identity{NodeName: "one", ClusterID: "cluster", InstanceID: "process", MemberID: 4}
	cfg := config{DataDir: t.TempDir(), Credential: "credential", InstanceID: id.InstanceID, AdmissionPinPhase: "pending"}
	path := filepath.Join(cfg.DataDir, "_authority.bolt")
	authority, err := authoritystore.Open(path, id, authoritystore.FreshEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Close(); err != nil {
		t.Fatal(err)
	}
	meta, err := metastore.Open(filepath.Join(cfg.DataDir, "_meta.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(meta.EnsureIdentity(id.NodeName, id.ClusterID, id.MemberID), meta.Close()); err != nil {
		t.Fatal(err)
	}
	constructed := id
	constructed.MemberID = 0
	node := &authorityProtocolNode{id: constructed}
	a := &application{node: node, cfg: cfg, clusterHooks: newClusterHooks(), workCtx: t.Context(), closing: make(chan struct{})}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/internal/test/admission-pin", nil)
	r.Header.Set("X-Gquorum-Test-Credential", cfg.Credential)
	w := httptest.NewRecorder()
	a.routes().ServeHTTP(w, r)
	var view struct {
		AuthorityIdentity topology.Identity `json:"authority_identity"`
		MetadataAcquired  bool              `json:"metadata_acquired"`
		Found             bool              `json:"found"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &view) != nil || view.AuthorityIdentity != id || view.MetadataAcquired || !view.Found {
		t.Fatal("pin observation fabricated acquisition or used constructed member zero", w.Code, w.Body.String())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || node.fences.Load() != 0 || node.actions.Load() != 0 {
		t.Fatal("observation changed durable authority/effects", err)
	}
}
