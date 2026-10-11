//go:build !functional

package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestClusterBarrierCancellationAndAuthenticatedRelease(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{Credential: "owned-test-credential"}, workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	handler := a.routes()
	call := func(method, path, credential string) int {
		request := httptest.NewRequest(method, path, nil)
		request.Header.Set("X-Gquorum-Test-Credential", credential)
		reply := httptest.NewRecorder()
		handler.ServeHTTP(reply, request)
		return reply.Code
	}
	path := "/internal/test/barriers/work/promote_voter"
	if code := call("PUT", path, "foreign"); code != 403 {
		t.Fatal("foreign control mutated a barrier", code)
	}
	if code := call("PUT", path, a.cfg.Credential); code != 200 {
		t.Fatal(code)
	}
	approved := topology.AuthorizedStep{OperatorID: "exec-v4-a1", Version: 4, Attempt: 1, StepSeq: 8, Kind: topology.StepPromoteVoter, GroupID: "work", NodeName: "node3", ReplicaID: 4}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if err := a.beforeClusterEffect(canceled, approved); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled effect retained its wait", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	joined := make(chan error, 1)
	go func() { joined <- a.beforeClusterEffect(ctx, approved) }()
	defer func() { cancel(); <-joined }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		a.clusterHooks.mu.Lock()
		b := a.clusterHooks.barriers[effectKey{"work", topology.StepPromoteVoter}]
		hits, last := b.hits, b.last
		a.clusterHooks.mu.Unlock()
		if hits == 2 {
			if last != approved {
				t.Fatal("barrier lost exact authorization", last)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("live effect never reached barrier")
		case <-ticker.C:
		}
	}
	if code := call("DELETE", path, a.cfg.Credential); code != 200 {
		t.Fatal(code)
	}
	// Observe release without consuming the result needed by the deferred join.
	select {
	case err := <-joined:
		joined <- err
		if err != nil {
			t.Fatal("released effect was not unblocked", err)
		}
	case <-ctx.Done():
		t.Fatal("released effect did not join")
	}
	if code := call("DELETE", path, a.cfg.Credential); code != 404 {
		t.Fatal("duplicate release changed another barrier", code)
	}
}

func TestCallerDeletionRefusalRecordsScopeAndReleases(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{Credential: "owned"}, workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	handler := a.routes()
	call := func(method, path, credential string) int {
		request := httptest.NewRequest(method, path, nil)
		request.Header.Set("X-Gquorum-Test-Credential", credential)
		reply := httptest.NewRecorder()
		handler.ServeHTTP(reply, request)
		return reply.Code
	}
	path := "/internal/test/barriers/occupied/stop_replica"
	if code := call("PUT", path+"?refuse=true", "foreign"); code != 403 {
		t.Fatal(code)
	}
	if code := call("PUT", "/internal/test/barriers/occupied/promote_voter?refuse=true", "owned"); code != 400 {
		t.Fatal(code)
	}
	if code := call("PUT", path+"?refuse=garbage", "owned"); code != 400 {
		t.Fatal(code)
	}
	if code := call("PUT", path+"?refuse=true", "owned"); code != 200 {
		t.Fatal(code)
	}
	approved := topology.AuthorizedStep{OperatorID: "exec-v3-a1", Version: 3, Attempt: 1, StepSeq: 1, Kind: topology.StepStopReplica, GroupID: "occupied", NodeName: "node1", ReplicaID: 7}
	if err := a.beforeClusterEffect(t.Context(), approved); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
	b := a.clusterHooks.barriers[effectKey{"occupied", topology.StepStopReplica}]
	if b.hits != 1 || b.last != approved || !b.refuse {
		t.Fatal(b)
	}
	other := approved
	other.GroupID = "other"
	if err := a.beforeClusterEffect(t.Context(), other); err != nil {
		t.Fatal("refusal escaped exact group", err)
	}
	if code := call("DELETE", path, "owned"); code != 200 {
		t.Fatal(code)
	}
	if err := a.beforeClusterEffect(t.Context(), approved); err != nil {
		t.Fatal("refusal survived release", err)
	}
}
