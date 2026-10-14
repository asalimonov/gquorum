// Package schedulingtest checks repair decisions and input ownership.
package schedulingtest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/scheduling"
)

// RepairPolicyCase specifies an input and expected repair decision or
// classified error.
type RepairPolicyCase struct {
	Name      string
	Input     scheduling.ScheduleInput
	Want      scheduling.ScheduleDecision
	ErrorCode fault.Code
}

// CheckRepairPolicy creates an independent implementation for each case and
// gives
// it owned input. The supplied cases remain reusable even if a callback uses
// its input as scratch state. Factories must return nonnil implementations.
func CheckRepairPolicy(t *testing.T, factory func() scheduling.RepairPolicy, cases []RepairPolicyCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			before := tc.Input.Clone()
			got, err := factory().Schedule(context.Background(), tc.Input.Clone())
			check(t, got, err, expectedResult{Value: tc.Want, ErrorCode: tc.ErrorCode})
			if !reflect.DeepEqual(tc.Input, before) {
				t.Fatal("scheduler modified the original case through its owned input")
			}
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
