//go:build !clustertest

package main

import (
	"net/http/httptest"
	"testing"
)

func TestStandardExampleHasNoTestControls(t *testing.T) {
	t.Parallel()
	a := &application{workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	request := httptest.NewRequest("GET", "/internal/test/controls", nil)
	reply := httptest.NewRecorder()
	a.routes().ServeHTTP(reply, request)
	if reply.Code != 404 {
		t.Fatal("standard example exposes test controls", reply.Code, reply.Body.String())
	}
}
