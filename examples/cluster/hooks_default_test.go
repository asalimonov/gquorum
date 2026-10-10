//go:build !clustertest

package main

import (
	"net/http/httptest"
	"testing"
)

func TestStandardExampleHasNoTestControls(t *testing.T) {
	t.Parallel()
	a := &application{workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	for _, route := range []struct{ method, path string }{
		{"GET", "/internal/test/controls"},
		{"GET", "/internal/test/projection/work/7"},
		{"PUT", "/internal/test/projection/work/7"},
		{"DELETE", "/internal/test/projection/work/7"},
		{"PUT", "/internal/test/wal/work"},
		{"DELETE", "/internal/test/wal/work"},
	} {
		request := httptest.NewRequest(route.method, route.path, nil)
		reply := httptest.NewRecorder()
		a.routes().ServeHTTP(reply, request)
		if reply.Code != 404 {
			t.Fatal("standard example exposes test controls", route, reply.Code, reply.Body.String())
		}
	}
}
