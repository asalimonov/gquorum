//go:build !functional

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type operationRouteCluster struct {
	gquorum.Cluster
	status topology.OperationStatus
}

func (c operationRouteCluster) Operation(context.Context, topology.OperationID) (topology.OperationStatus, error) {
	return c.status, nil
}

func TestOperationHTTPRetainsSuccessfulStepCounts(t *testing.T) {
	t.Parallel()
	cluster := operationRouteCluster{status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2,
		Attempt: 1, State: topology.OperationSucceeded, Blocks: []topology.BlockStatus{
			{GroupID: "a", State: topology.OperationSucceeded, CurrentStep: 2, TotalSteps: 2},
			{GroupID: "b", State: topology.OperationSucceeded, CurrentStep: 3, TotalSteps: 3}}, TotalBlocks: 2}}
	app := &application{cluster: cluster, workCtx: context.Background(), closing: make(chan struct{})}
	w := httptest.NewRecorder()
	app.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/operations/exec-v2-a1", nil))
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	var got struct {
		TotalSteps int    `json:"total_steps"`
		GroupID    string `json:"group_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.TotalSteps != 5 || got.GroupID != "b" {
		t.Fatal(got, err, w.Body.String())
	}
}

func TestHTTPVersionRequiresOneExactStrongIdentity(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "*", `W/"2"`, `"1","2"`, "0", "-1", " 2", "18446744073709551616"} {
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
func TestHTTPFaultsHaveStableStatusAndCASVersions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err    error
		status int
	}{{fault.ErrInvalid, 400}, {fault.ErrGroupRetired, 409}, {fault.ErrUnsafeMembership, 422}, {fault.ErrUnavailable, 503}, {fault.ErrAuthorityPending, 503}, {fault.ErrAuthorityFenced, 409}, {fault.ErrReplanRequired, 409}, {&fault.Error{Code: fault.VersionConflict, ExpectedVersion: 3, CurrentVersion: 4, RebasedTo: 4}, 409}} {
		w := httptest.NewRecorder()
		httpError(w, tc.err)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		var typed *fault.Error
		if errors.As(tc.err, &typed) && typed.Code == fault.VersionConflict {
			var reply struct {
				Expected  uint64 `json:"expected_version"`
				Current   uint64 `json:"current_version"`
				RebasedTo uint64 `json:"rebased_to"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Expected != typed.ExpectedVersion || reply.Current != typed.CurrentVersion || reply.RebasedTo != typed.RebasedTo {
				t.Fatal("conflict lost exact retry provenance", reply, err)
			}
		}
	}
}
