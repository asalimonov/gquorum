package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestHTTPVersionRequiresOneExactStrongIdentity(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "*", `W/"2"`, `"1","2"`, "0", "-1", "+2", " 2", "18446744073709551616"} {
		req := httptest.NewRequest("POST", "/api/groups", nil)
		req.Header.Set("If-Match", value)
		if _, err := expectedVersion(req); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(value, err)
		}
	}
	for _, value := range []string{"2", `"2"`} {
		req := httptest.NewRequest("POST", "/api/groups", nil)
		req.Header.Set("If-Match", value)
		if n, err := expectedVersion(req); err != nil || n != 2 {
			t.Fatal(value, n, err)
		}
	}
}

type httpNode struct {
	gquorum.Node
	cluster gquorum.Cluster
	status  gquorum.NodeStatus
}

func (n httpNode) Cluster() gquorum.Cluster    { return n.cluster }
func (n httpNode) Identity() topology.Identity { return topology.Identity{NodeName: "node1"} }
func (n httpNode) Status() gquorum.NodeStatus  { return n.status }

type httpCluster struct {
	gquorum.Cluster
	calls  int
	result gquorum.MutationResult
	err    error
}

func (c *httpCluster) Approve(context.Context, uint64) (topology.OperatorID, error) {
	c.calls++
	return "op1", c.err
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
			if tc.want == 202 && strings.TrimSpace(w.Body.String()) != `{"operator_id":"op1"}` {
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
func TestHTTPFaultsHaveStableStatusAndCASVersions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err    error
		status int
	}{{fault.ErrInvalid, 400}, {fault.ErrGroupRetired, 409}, {fault.ErrUnsafeMembership, 422}, {fault.ErrUnavailable, 503}, {&fault.Error{Code: fault.VersionConflict, ExpectedVersion: 3, CurrentVersion: 4}, 409}} {
		w := httptest.NewRecorder()
		httpError(w, tc.err)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		if errors.Is(tc.err, fault.ErrVersionConflict) {
			var body struct {
				Expected uint64 `json:"expected_version"`
				Current  uint64 `json:"current_version"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Expected != 3 || body.Current != 4 {
				t.Fatal(body, err)
			}
		}
	}
}
