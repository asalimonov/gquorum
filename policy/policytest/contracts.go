// Package policytest provides result and input-ownership checks for application
// policies. It does not run Raft or prove cancellation, determinism, absence of
// external effects, or safety of the application's implementation.
package policytest

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/policy"
)

type SchedulerCase struct {
	Name      string
	Input     policy.ScheduleInput
	Want      policy.ScheduleDecision
	ErrorCode fault.Code
}
type ReadinessCase struct {
	Name      string
	Input     policy.ReadinessInput
	Want      policy.ReadinessDecision
	ErrorCode fault.Code
}
type RetentionCase struct {
	Name      string
	Input     policy.RetentionInput
	Want      policy.RetentionDecision
	ErrorCode fault.Code
}
type RemovalCase struct {
	Name      string
	Input     policy.RemovalInput
	Want      policy.RemovalDecision
	ErrorCode fault.Code
}
type LifecycleCases struct {
	Readiness []ReadinessCase
	Retention []RetentionCase
	Removal   []RemovalCase
}

// CheckScheduler creates an independent implementation for each case and gives
// it owned input. The supplied cases remain reusable even if a callback uses
// its input as scratch state. Factories must return nonnil implementations.
func CheckScheduler(t *testing.T, factory func() policy.Scheduler, cases []SchedulerCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			before := tc.Input.Clone()
			got, err := factory().Schedule(context.Background(), tc.Input.Clone())
			check(t, got, tc.Want, err, tc.ErrorCode)
			if !reflect.DeepEqual(tc.Input, before) {
				t.Fatal("scheduler modified the original case through its owned input")
			}
		})
	}
}

// CheckLifecycle checks independent readiness, retention and removal cases.
func CheckLifecycle(t *testing.T, factory func() policy.LifecyclePolicy, cases LifecycleCases) {
	t.Helper()
	for _, tc := range cases.Readiness {
		t.Run("readiness/"+tc.Name, func(t *testing.T) {
			in := tc.Input
			before := slices.Clone(in.MissingFounders)
			in.MissingFounders = slices.Clone(in.MissingFounders)
			got, err := factory().Readiness(context.Background(), in)
			check(t, got, tc.Want, err, tc.ErrorCode)
			if !reflect.DeepEqual(tc.Input.MissingFounders, before) {
				t.Fatal("readiness callback modified the original founder list")
			}
		})
	}
	for _, tc := range cases.Retention {
		t.Run("retention/"+tc.Name, func(t *testing.T) {
			got, err := factory().Retention(context.Background(), tc.Input)
			check(t, got, tc.Want, err, tc.ErrorCode)
		})
	}
	for _, tc := range cases.Removal {
		t.Run("removal/"+tc.Name, func(t *testing.T) {
			got, err := factory().Removal(context.Background(), tc.Input)
			check(t, got, tc.Want, err, tc.ErrorCode)
		})
	}
}

func check(t *testing.T, got, want any, err error, code fault.Code) {
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
