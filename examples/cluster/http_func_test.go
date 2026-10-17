//go:build functional

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type httpNode struct {
	gquorum.Node
	cluster gquorum.Cluster
	status  gquorum.NodeStatus
}

func (n httpNode) Cluster() gquorum.Cluster { return n.cluster }

func (_ httpNode) Identity() topology.Identity { return topology.Identity{NodeName: "node1"} }

func (n httpNode) Status() gquorum.NodeStatus { return n.status }

type httpCluster struct {
	gquorum.Cluster
	calls    int
	result   gquorum.MutationResult
	err      error
	progress topology.ProgressPage
	status   topology.ClusterStatus
}

func (c *httpCluster) Status(context.Context) (topology.ClusterStatus, error) {
	return c.status, c.err
}

func TestClusterStatusHTTPIncludesLeaderNodeNames(t *testing.T) {
	t.Parallel()
	cluster := &httpCluster{status: topology.ClusterStatus{
		MetaLeaderID: 2, MetaLeaderNodeName: "worker-west",
		Nodes:  []topology.NodeStatus{{Node: topology.Node{Name: "worker-west"}}},
		Groups: []topology.GroupProgress{{GroupID: topology.MetaGroupID, Observation: &topology.GroupStatus{GroupID: topology.MetaGroupID, HostNode: "worker-east", LeaderID: 2, LeaderNodeName: "worker-west"}}},
	}}
	w := httptest.NewRecorder()
	routes(httpNode{cluster: cluster}).ServeHTTP(w, httptest.NewRequest("GET", "/api/cluster/status", nil))
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	var body struct {
		MetaLeaderID       uint64                   `json:"meta_leader_id"`
		MetaLeaderNodeName string                   `json:"meta_leader_node_name"`
		Groups             []topology.GroupProgress `json:"groups"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.MetaLeaderID != 2 || body.MetaLeaderNodeName != "worker-west" || len(body.Groups) != 1 || body.Groups[0].Observation.LeaderID != 2 || body.Groups[0].Observation.LeaderNodeName != "worker-west" {
		t.Fatal(w.Body.String())
	}
}

func (c *httpCluster) Approve(context.Context, uint64) (topology.OperationID, error) {
	c.calls++
	return "op1", c.err
}

func (c *httpCluster) ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error) {
	return c.progress, c.err
}

func (c *httpCluster) DrainNode(context.Context, string, gquorum.MutationOptions) (gquorum.MutationResult, error) {
	c.calls++
	return c.result, c.err
}

func TestApprovalValidationBeforeClusterCall(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, body string
		match            []string
		want             int
	}{
		{name: "empty", want: 202},
		{name: "object", body: " {} \n", want: 202},
		{name: "matching pin", match: []string{`"2"`}, want: 202},
		{name: "null", body: "null", want: 400},
		{name: "array", body: "[]", want: 400},
		{name: "trailing JSON", body: "{} {}", want: 400},
		{name: "unknown field", body: `{"preview":true}`, want: 400},
		{name: "legacy field", body: `{"apply":true}`, want: 400},
		{name: "approve field", body: `{"approve":true}`, want: 400},
		{name: "oversize", body: strings.Repeat(" ", 1<<20) + "{}", want: 400},
		{name: "mismatched pin", match: []string{"3"}, want: 400},
		{name: "empty pin", match: []string{""}, want: 400},
		{name: "multiple headers", match: []string{"2", "2"}, want: 400},
		{name: "query", path: "/api/topology/2/approve?preview=true", want: 400},
		{name: "unknown query", path: "/api/topology/2/approve?anything", want: 400},
		{name: "invalid path", path: "/api/topology/0/approve", want: 400},
		{name: "old route", path: "/api/topology/2/apply", want: 404},
		{name: "unknown action", path: "/api/nodes/node1/unknown", want: 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &httpCluster{}
			path := tc.path
			if path == "" {
				path = "/api/topology/2/approve"
			}
			r := httptest.NewRequest("POST", path, strings.NewReader(tc.body))
			for _, match := range tc.match {
				r.Header.Add("If-Match", match)
			}
			w := httptest.NewRecorder()
			routes(httpNode{cluster: cluster}).ServeHTTP(w, r)
			if w.Code != tc.want || (cluster.calls == 1) != (tc.want == 202) {
				t.Fatalf("status %d, calls %d: %s", w.Code, cluster.calls, w.Body.String())
			}
			if tc.want == 202 && strings.TrimSpace(w.Body.String()) != `{"operation_id":"op1"}` {
				t.Fatal(w.Body.String())
			}
		})
	}
}

func TestMutationValidationAndCommittedVersionOnApprovalFailure(t *testing.T) {
	t.Parallel()
	cluster := &httpCluster{
		result: gquorum.MutationResult{Document: topology.Document{Version: 3}},
		err:    fault.ErrUnavailable,
	}
	handler := routes(httpNode{cluster: cluster})
	for _, body := range []string{`null`, `{"apply":true}`, `{"approve":true,"preview":true}`, `{"expected_version":4}`} {
		r := httptest.NewRequest("POST", "/api/nodes/node1/drain", strings.NewReader(body))
		r.Header.Set("If-Match", "2")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 400 || cluster.calls != 0 {
			t.Fatalf("%s: HTTP %d, calls %d", body, w.Code, cluster.calls)
		}
	}
	r := httptest.NewRequest("POST", "/api/nodes/node1/drain", strings.NewReader(`{"approve":true}`))
	r.Header.Set("If-Match", "2")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 503 || w.Header().Get("X-Staged-Version") != "3" || cluster.calls != 1 {
		t.Fatalf("committed stage lost: HTTP %d, header %v, calls %d", w.Code, w.Header(), cluster.calls)
	}
}

func TestReadinessRequiresNodeGateAndSession(t *testing.T) {
	t.Parallel()
	for _, status := range []gquorum.NodeStatus{{Ready: false, SessionLive: true}, {Ready: true, SessionLive: false}} {
		w := httptest.NewRecorder()
		routes(httpNode{cluster: &httpCluster{}, status: status}).ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestReadinessRequiresApprovedActiveMember(t *testing.T) {
	t.Parallel()
	for _, state := range []topology.NodeState{topology.NodeActive, topology.NodeDraining, topology.NodeDrained, topology.NodeExcluded} {
		cluster := &httpCluster{progress: topology.ProgressPage{TargetVersion: 3, Nodes: []topology.NodeProgress{{NodeName: "node1", Approved: &topology.Node{Name: "node1", State: state}}}}}
		w := httptest.NewRecorder()
		routes(httpNode{cluster: cluster, status: gquorum.NodeStatus{Ready: true, SessionLive: true}}).ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
		if state != topology.NodeActive {
			if w.Code != http.StatusServiceUnavailable {
				t.Fatal(state, w.Code, w.Body.String())
			}
			continue
		}
		var ready readiness
		if err := json.Unmarshal(w.Body.Bytes(), &ready); err != nil || w.Code != 200 || ready.Node != "node1" || !ready.SessionLive || ready.TargetVersion != 3 {
			t.Fatal(ready, w.Code, err)
		}
	}
}

func TestMutationResponseRetainsStatusAndVersionHeaders(t *testing.T) {
	t.Parallel()
	for _, operation := range []topology.OperationID{"", "op1"} {
		cluster := &httpCluster{result: gquorum.MutationResult{Document: topology.Document{Version: 3}, OperationID: operation}}
		r := httptest.NewRequest("POST", "/api/nodes/node1/drain", strings.NewReader(`{}`))
		r.Header.Set("If-Match", "2")
		w := httptest.NewRecorder()
		routes(httpNode{cluster: cluster}).ServeHTTP(w, r)
		want := http.StatusOK
		if operation != "" {
			want = http.StatusAccepted
		}
		var result gquorum.MutationResult
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != want || w.Header().Get("ETag") != `"3"` || result.OperationID != operation || result.Document.Version != 3 {
			t.Fatal(w.Code, w.Header(), result, err)
		}
	}
}

func TestHealthcheckReportsReadinessStatus(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		err := (&healthcheckCommand{URL: server.URL, Timeout: time.Second}).Run()
		server.Close()
		if (err == nil) != (status == http.StatusOK) {
			t.Fatal(status, err)
		}
	}
}
