//go:build !functional

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSubmissionControlsAuthenticateBeforeNodeAccess(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{Credential: "owned"}, workCtx: t.Context(),
		closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	handler := a.routes()
	for _, credential := range []string{"", "foreign"} {
		r := httptest.NewRequest(http.MethodPost, "/internal/test/submission/work/1", nil)
		r.Header.Set("X-Gquorum-Test-Credential", credential)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/internal/test/submission/_meta/1", nil)
	r.Header.Set("X-Gquorum-Test-Credential", "owned")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatal(w.Code, w.Body.String())
	}
}
