//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestExecutionLimitsDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	defaults := (Limits{}).WithDefaults()
	if defaults.MaxNodes != 3 || defaults.MaxReplicas != 5 || defaults.MaxConcurrentGroups != 4 || defaults.MaxConcurrentCleanup != 2 {
		t.Fatalf("defaults: %+v", defaults)
	}
	for _, tc := range []struct {
		name            string
		groups, cleanup int
		invalid         bool
	}{
		{"default", 0, 0, false}, {"explicit", 1, 7, false},
		{"negative groups", -1, 2, true}, {"negative cleanup", 4, -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := Limits{MaxNodes: 3, MaxReplicas: 5, MaxConcurrentGroups: tc.groups, MaxConcurrentCleanup: tc.cleanup}
			err := limits.WithDefaults().Validate()
			if errors.Is(err, fault.ErrInvalid) != tc.invalid || (!tc.invalid && err != nil) {
				t.Fatalf("Validate: %v", err)
			}
			if tc.groups != 0 && limits.WithDefaults().MaxConcurrentGroups != tc.groups || tc.cleanup != 0 && limits.WithDefaults().MaxConcurrentCleanup != tc.cleanup {
				t.Fatal("normalization changed explicit execution limits")
			}
		})
	}
}

func TestExecutionLimitsJSON(t *testing.T) {
	t.Parallel()
	var limits Limits
	if err := json.Unmarshal([]byte(`{"max_nodes":3,"max_replicas":5,"max_concurrent_groups":9,"max_concurrent_cleanup":6}`), &limits); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(limits.WithDefaults())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]int
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["max_concurrent_groups"] != 9 || fields["max_concurrent_cleanup"] != 6 {
		t.Fatalf("execution limits: %s", data)
	}
}
