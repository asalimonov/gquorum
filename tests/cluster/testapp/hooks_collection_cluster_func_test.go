//go:build functional

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestCollectionReportBarrierIsExactAuthenticatedAndCancelable(t *testing.T) {
	t.Parallel()
	identity := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	a := &application{node: &authorityProtocolNode{id: identity}, cfg: config{Credential: "credential"}, metaProxy: &metaFaultProxy{}, clusterHooks: newClusterHooks(), workCtx: t.Context(), closing: make(chan struct{})}
	id := storage.Identity{ClusterID: "cluster", NodeName: "returning", GroupID: "work", ReplicaID: 2}
	control := func(method, credential string) int {
		body, _ := json.Marshal(map[string]any{"identity": id})
		r := httptest.NewRequest(method, "/internal/test/collection-report", bytes.NewReader(body))
		r.Header.Set("X-Gquorum-Test-Credential", credential)
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, r)
		return w.Code
	}
	if got := control("PUT", "foreign"); got != 403 {
		t.Fatal("foreign control accepted", got)
	}
	if got := control("PUT", "credential"); got != 200 {
		t.Fatal(got)
	}
	frame := func(id storage.Identity) []byte {
		data, _ := json.Marshal(map[string]any{"kind": "report_collection", "receipt": map[string]any{"identity": id, "local_sequence": 7}})
		return proxyTestBytes(2, data)
	}
	other := id
	other.ReplicaID++
	if err := a.waitCollectionReport(t.Context(), frame(other)); err != nil {
		t.Fatal("another identity blocked", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.waitCollectionReport(ctx, frame(id)) }()
	apiEventually(t, func() bool {
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		return a.clusterHooks.collectionReport.Hits == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("report outlived its canceled transport", err)
	}
	if got := control("DELETE", "credential"); got != 200 {
		t.Fatal(got)
	}
	if err := a.waitCollectionReport(t.Context(), frame(id)); err != nil {
		t.Fatal("released report stayed blocked", err)
	}
	a.clusterHooks.mu.Lock()
	defer a.clusterHooks.mu.Unlock()
	if a.clusterHooks.collectionReport.Hits != 2 || a.clusterHooks.collectionReport.Held {
		t.Fatal(a.clusterHooks.collectionReport)
	}
}
