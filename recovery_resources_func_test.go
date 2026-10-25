//go:build functional

package gquorum

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
)

func TestRecoveryBoundsRefuseBeforeApprovalWithoutCallingApplication(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name   string
		bounds recovery.Resources
	}{
		{"metadata", recovery.Resources{MaxMetadataBytes: 1}},
		{"record", recovery.Resources{MaxRecordBytes: 1}},
		{"group", recovery.Resources{MaxGroupBytes: 1}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			opts := recoveryFixtureOptions(t, true)
			before := sourceFileHashes(t, opts.StorageDir)
			opts.Loads.Factory = nil // Offline bounds checks cannot use application callbacks.
			handle, err := NewRecovery(opts, recovery.Options{Resources: shape.bounds})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := handle.Inspect(t.Context(), recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "bounded source"})
			var refusal *recovery.Refusal
			if !errors.As(err, &refusal) || refusal.Issue.Code != "resource_bound" || !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("size budget was reported as damaged or missing data", plan, err)
			}
			if !reflect.DeepEqual(before, sourceFileHashes(t, opts.StorageDir)) {
				t.Fatal("size refusal changed source")
			}
			statuses, listErr := handle.List(t.Context())
			if listErr != nil || len(statuses) != 0 {
				t.Fatal("size refusal created an operation", statuses, listErr)
			}
		})
	}
}
