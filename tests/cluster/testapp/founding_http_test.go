package main

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
)

type unreadyFoundingNode struct{ gquorum.Node }

func (unreadyFoundingNode) Status() gquorum.NodeStatus {
	return gquorum.NodeStatus{State: gquorum.NodeStarting}
}

func TestFoundingHTTPNamesUnverifiedPeersBeforeFirstPolicyObservation(t *testing.T) {
	t.Parallel()
	a := &application{
		cfg:  config{GateFounders: true, Peers: []peer{{Name: "node1"}, {Name: "node2"}, {Name: "node3"}}},
		node: unreadyFoundingNode{}, workCtx: t.Context(), closing: make(chan struct{}),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	handler := a.routes()
	check := func() string {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/health/ready", nil))
		if response.Code != 503 || !strings.Contains(response.Body.String(), "node3") {
			t.Fatal("founding gate omitted the absent peer", response.Code, response.Body.String())
		}
		return response.Body.String()
	}
	check()
	defaults, err := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{Startup: policy.AllFounders})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := &fixtureLifecycle{DefaultLifecycle: defaults, app: a}
	if _, err := lifecycle.Readiness(t.Context(), policy.ReadinessInput{FirstBootstrap: true, MissingFounders: []topology.Identity{{NodeName: "node3"}}}); err != nil {
		t.Fatal(err)
	}
	if body := check(); strings.Contains(body, "node1") || strings.Contains(body, "node2") {
		t.Fatal("founding gate ignored the newer policy observation", body)
	}
}
