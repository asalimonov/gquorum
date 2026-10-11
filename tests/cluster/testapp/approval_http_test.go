//go:build !functional

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestApprovalRejectsModeFlagsBeforeCallingCluster(t *testing.T) {
	t.Parallel()
	// No cluster handle exists: an ignored flag would reach the mutating API and
	// panic. Refusals must instead identify the explicit plan endpoint.
	handler := (&application{workCtx: t.Context(), closing: make(chan struct{})}).routes()
	for _, input := range []struct {
		name, query, body string
	}{
		{name: "body-dry-run", body: `{"dry_run":true}`},
		{name: "body-apply", body: `{"apply":false}`},
		{name: "body-approve", body: `{"approve":false}`},
		{name: "body-preview", body: `{"preview":true}`},
		{name: "unknown-body", body: `{"unexpected":1}`},
		{name: "null", body: `null`},
		{name: "array", body: `[]`},
		{name: "trailing-body", body: `{}{"dry_run":true}`},
		{name: "malformed", body: `{`},
		{name: "oversized", body: strings.Repeat(" ", (1<<20)+1)},
		{name: "dry-run-query", query: "?dry_run=true"},
		{name: "bare-dry-run", query: "?dry_run"},
		{name: "empty-dry-run", query: "?dry_run="},
		{name: "apply-query", query: "?apply=false"},
		{name: "bare-apply", query: "?apply"},
		{name: "preview-query", query: "?preview=true"},
		{name: "approve-query", query: "?approve=true"},
		{name: "unknown-query", query: "?anything"},
	} {
		t.Run(input.name, func(t *testing.T) {
			request := httptest.NewRequest("POST", "/api/topology/7/approve"+input.query, strings.NewReader(input.body))
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, request)
			if reply.Code != http.StatusBadRequest || !strings.Contains(reply.Body.String(), "/api/topology/7/plan") {
				t.Fatal("approval did not refuse before mutation", reply.Code, reply.Body.String())
			}
		})
	}
}
