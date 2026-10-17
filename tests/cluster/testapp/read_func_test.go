//go:build functional

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// One participant exercises the real caller factory, granted controller path,
// native-read transport and HTTP query adapter. Multi-replica read scenarios
// belong to the Docker suite.
func TestApplicationReadUsesManagedQueryAndStopsWithExactReplica(t *testing.T) {
	t.Parallel()
	var ls listeners
	for _, dest := range []*net.Listener{&ls.meta, &ls.load, &ls.http} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		*dest = listener
		t.Cleanup(func() { _ = listener.Close() })
	}
	cfg := config{NodeName: "one", NodeType: "worker", ClusterID: "cluster", InstanceID: "process", Credential: "credential", DataDir: t.TempDir(), TickInterval: 10 * time.Millisecond, SessionTTL: 3 * time.Second}
	cfg.Peers = []peer{{Name: cfg.NodeName, Type: cfg.NodeType, MemberID: 1, MetaAddress: ls.meta.Addr().String(), LoadAddress: ls.load.Addr().String(), HTTPAddress: ls.http.Addr().String()}}
	a, err := newApplication(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), &ls)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.node.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool { return a.cluster.IsLeader() && a.node.OwnSessionLive() })
	latest, err := a.cluster.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	voters := topology.DefaultVoterConfig()
	voters.TargetVoters = 1
	created, err := a.cluster.CreateGroup(ctx, gquorum.GroupSpec{ID: "work", VoterConfig: &voters}, gquorum.MutationOptions{ExpectedVersion: latest.Version, Actor: cfg.NodeName, Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := a.cluster.WaitOperation(ctx, created.OperationID)
	if err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	g, found := a.node.LoadGroup("work")
	if !found {
		t.Fatal("completed founding omitted local replica")
	}
	// Operator receipts precede publishing the converged Active topology.
	apiEventually(t, func() bool { _, err := a.localServing(ctx, "work"); return err == nil })
	data, _ := json.Marshal(putCommand{RequestID: "one", Key: "key", Value: "value"})
	if result, err := g.Propose(ctx, load.Proposal{RequestID: "one", Data: data}); err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, httptest.NewRequest("GET", path, nil).WithContext(ctx))
		return w
	}
	w := request("/api/load/work/read/key")
	var value struct {
		getResult
		Consistency string `json:"consistency"`
		Node        string `json:"node"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &value) != nil || !value.Found || value.Value != "value" || value.AppliedIndex == 0 || value.Consistency != "linearizable" || value.Node != cfg.NodeName {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("/api/load/work/read/missing"); w.Code != http.StatusNotFound {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err := g.Read(ctx, []byte(`{"key":"key","unknown":true}`)); !errors.Is(err, fault.ErrInvalid) || g.Error() != nil {
		t.Fatal("query error halted the managed replica", err, g.Error())
	}
	if w := request("/api/load/work/keys/key"); w.Code != http.StatusOK {
		t.Fatal("local diagnostic route stopped working", w.Code, w.Body.String())
	}
	controlPath := "/internal/test/reads/work/" + fmt.Sprint(g.ReplicaID())
	control := func(method, credential string, identity topology.Identity, holdQuery bool) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"identity": identity, "hold_query": holdQuery})
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, controlPath, bytes.NewReader(body)).WithContext(ctx)
		r.Header.Set("X-Gquorum-Test-Credential", credential)
		a.routes().ServeHTTP(w, r)
		return w
	}
	identity := a.node.Identity()
	if w := control("PUT", "foreign", identity, true); w.Code != http.StatusForbidden {
		t.Fatal("unauthenticated read control mutated the machine", w.Code, w.Body.String())
	}
	foreign := identity
	foreign.InstanceID = "another-process"
	if w := control("PUT", cfg.Credential, foreign, true); w.Code != http.StatusConflict {
		t.Fatal("read control ignored process incarnation", w.Code, w.Body.String())
	}
	if w := control("PUT", cfg.Credential, identity, true); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	blocked := make(chan error, 1)
	go func() { _, err := g.Read(readCtx, []byte(`{"key":"key"}`)); blocked <- err }()
	key := walKey{"work", g.ReplicaID()}
	apiEventually(t, func() bool { return a.clusterHooks.readMetrics(key).QueryActive == 1 })
	cancelRead()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation did not end the safe read wait", err)
	}
	apiEventually(t, func() bool { return a.clusterHooks.readMetrics(key).QueryActive == 0 })
	if g.Error() != nil {
		t.Fatal("canceling a controlled Query halted the replica", g.Error())
	}
	if w := control("DELETE", cfg.Credential, identity, false); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("/api/load/work/read/key"); w.Code != http.StatusOK {
		t.Fatal("released Query failed to recover", w.Code, w.Body.String())
	}
	// Full replica stop cancels and joins an engine-owned Query, even while its
	// test barrier remains armed. The exact application Close removes controls.
	if w := control("PUT", cfg.Credential, identity, true); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	go func() { _, err := g.Read(ctx, []byte(`{"key":"key"}`)); blocked <- err }()
	apiEventually(t, func() bool { return a.clusterHooks.readMetrics(key).QueryActive == 1 })
	if err := a.node.StopLoadGroup(ctx, load.ReplicaRef{GroupID: "work", ReplicaID: g.ReplicaID()}); err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err == nil {
		t.Fatal("stopped controlled Query reported success")
	}
	a.clusterHooks.mu.Lock()
	_, retained := a.clusterHooks.reads[key]
	a.clusterHooks.mu.Unlock()
	if retained {
		t.Fatal("joined application retained obsolete fault controls")
	}
	if w := request("/api/load/work/read/key"); w.Code != http.StatusServiceUnavailable {
		t.Fatal("stopped replica served an application observation as a safe read", w.Code, w.Body.String())
	}
}
