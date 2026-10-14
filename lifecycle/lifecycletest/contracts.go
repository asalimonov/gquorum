// Package lifecycletest provides result and input-ownership checks for
// application
// hooks. It does not run Raft or prove cancellation, determinism, absence of
// external effects, or safety of the application's implementation.
package lifecycletest

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
)

// ReadinessCase specifies an input and expected startup-gate decision or error
// code.
type ReadinessCase struct {
	Name      string
	Input     lifecycle.ReadinessInput
	Want      lifecycle.ReadinessDecision
	ErrorCode fault.Code
}

// RetentionCase specifies an input and expected quarantine-retention decision
// or error code.
type RetentionCase struct {
	Name      string
	Input     lifecycle.RetentionInput
	Want      lifecycle.RetentionDecision
	ErrorCode fault.Code
}

// RemovalCase specifies an input and expected fenced-removal decision or error
// code.
type RemovalCase struct {
	Name      string
	Input     lifecycle.RemovalInput
	Want      lifecycle.RemovalDecision
	ErrorCode fault.Code
}

// CheckHooks checks independent readiness, retention and removal cases.
func CheckHooks(t *testing.T, factory func() lifecycle.Hooks, cases HooksCases) {
	t.Helper()
	for _, tc := range cases.Readiness {
		t.Run("readiness/"+tc.Name, func(t *testing.T) {
			in := tc.Input
			before := slices.Clone(in.MissingFounders)
			in.MissingFounders = slices.Clone(in.MissingFounders)
			got, err := factory().Readiness(context.Background(), in)
			check(t, got, err, expectedResult{Value: tc.Want, ErrorCode: tc.ErrorCode})
			if !reflect.DeepEqual(tc.Input.MissingFounders, before) {
				t.Fatal("readiness callback modified the original founder list")
			}
		})
	}
	for _, tc := range cases.Retention {
		t.Run("retention/"+tc.Name, func(t *testing.T) {
			got, err := factory().Retention(context.Background(), tc.Input)
			check(t, got, err, expectedResult{Value: tc.Want, ErrorCode: tc.ErrorCode})
		})
	}
	for _, tc := range cases.Removal {
		t.Run("removal/"+tc.Name, func(t *testing.T) {
			got, err := factory().Removal(context.Background(), tc.Input)
			check(t, got, err, expectedResult{Value: tc.Want, ErrorCode: tc.ErrorCode})
		})
	}
}

type expectedResult struct {
	Value     any
	ErrorCode fault.Code
}

func check(t *testing.T, got any, err error, expected expectedResult) {
	want, code := expected.Value, expected.ErrorCode
	t.Helper()
	if code == "" && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != "" && !errors.Is(err, fault.New(code, "", nil)) {
		t.Fatalf("error = %v, want code %s", err, code)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decision = %+v, want %+v", got, want)
	}
}
