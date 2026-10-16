//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestOperationLookupOwnsFullExactAggregate(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	id := s.Operations[0].Status.ID
	failure := scopedTransitionFor(s.Operations[0], "shard0", 3)
	failure.State, failure.Error = topology.OperationFailed, "unreachable"
	applyScopedTransitionFixture(t, &s, failure)
	got, err := queryOperationStatus(&s, id)
	if err != nil || got.ID != id || got.State.Terminal() || got.BlocksTruncated || len(got.Blocks) != 2 || got.TotalBlocks != 2 {
		t.Fatal(got, err)
	}
	got.Blocks[1].Error = "caller"
	again, err := queryOperationStatus(&s, id)
	if err != nil || again.Blocks[1].Error != "unreachable" {
		t.Fatal("lookup borrowed state", again, err)
	}
	for _, tc := range []struct {
		id   topology.OperationID
		want error
	}{{"", fault.ErrInvalid}, {"unknown", fault.ErrHistoryUnavailable}} {
		if _, err := queryOperationStatus(&s, tc.id); !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
	}
	if _, err := NewMachine(testRules(), nil).Operation(id); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("unknown legacy operation", err)
	}
}

func TestLegacyOperationLookupUsesExactRetainedAttempt(t *testing.T) {
	t.Parallel()
	older := &operatorRecord{ID: "exec-v1-a1", Version: 1, Attempt: 1,
		State: topology.OperatorFailed, CurrentStep: 2, GroupID: "work", NodeName: "n1",
		WaitingReason: "", Error: "disk unavailable", Steps: []topology.Step{
			{Seq: 1, GroupID: "meta", NodeName: "n1"}, {Seq: 2, GroupID: "work", NodeName: "n1"}}}
	newer := &operatorRecord{ID: "exec-v2-a1", Version: 2, Attempt: 1,
		State: topology.OperatorRunning, CurrentStep: 1, GroupID: "other", Steps: []topology.Step{
			{Seq: 1, GroupID: "other", NodeName: "n2"}}}
	records := map[topology.OperatorID]*operatorRecord{older.ID: older, newer.ID: newer}
	got, err := legacyOperationStatus(records, topology.OperationID(older.ID))
	if err != nil || got.ID != topology.OperationID(older.ID) || got.State != topology.OperationFailed ||
		got.Error != "disk unavailable" || len(got.Blocks) != 1 || got.TotalBlocks != 1 ||
		got.Blocks[0].GroupID != "work" || got.Blocks[0].CurrentStep != 2 || got.Blocks[0].TotalSteps != 2 ||
		got.Blocks[0].Error != "disk unavailable" {
		t.Fatal("legacy operation did not preserve exact retained attempt", got, err)
	}
	if _, err := legacyOperationStatus(records, "exec-v99-a1"); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("unknown legacy operation", err)
	}
}
