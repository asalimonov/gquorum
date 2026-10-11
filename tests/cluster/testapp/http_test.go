//go:build !functional

package main

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

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
	}{{fault.ErrInvalid, 400}, {fault.ErrGroupRetired, 409}, {fault.ErrUnsafeMembership, 422}, {fault.ErrUnavailable, 503}, {&fault.Error{Code: fault.VersionConflict, ExpectedVersion: 3, CurrentVersion: 4}, 409}} {
		w := httptest.NewRecorder()
		httpError(w, tc.err)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
