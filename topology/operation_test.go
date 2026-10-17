//go:build !functional

package topology

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestOperationIDAndStates(t *testing.T) {
	t.Parallel()
	if got := MakeOperationID(math.MaxUint64, math.MaxUint32); got != "exec-v18446744073709551615-a4294967295" {
		t.Fatal("operation ID loses version or attempt", got)
	}
	for _, tc := range []struct {
		state    OperationState
		value    string
		terminal bool
		waiting  bool
	}{
		{OperationPending, "pending", false, false},
		{OperationRunning, "running", false, false},
		{OperationWaitingForReplica, "waiting_for_replica", false, true},
		{OperationWaitingForCatchUp, "waiting_for_catch_up", false, true},
		{OperationWaitingForLeader, "waiting_for_leader", false, true},
		{OperationSucceeded, "succeeded", true, false},
		{OperationFailed, "failed", true, false},
		{OperationSuperseded, "superseded", true, false},
	} {
		if string(tc.state) != tc.value || !tc.state.Valid() || tc.state.Terminal() != tc.terminal ||
			tc.state.Waiting() != tc.waiting {
			t.Errorf("unexpected execution state behavior: %+v", tc)
		}
	}
	for _, bad := range []OperationState{"", "complete", "Pending"} {
		if bad.Valid() || bad.Terminal() || bad.Waiting() {
			t.Errorf("unknown state accepted: %q", bad)
		}
	}
}

func TestOperationCloneOwnsPlanAndDiagnostics(t *testing.T) {
	t.Parallel()
	original := Operation{
		Status: OperationStatus{ID: MakeOperationID(2, 1), State: OperationFailed, TotalBlocks: 2,
			BlocksTruncated: true, Blocks: []BlockStatus{{GroupID: "a", State: OperationSucceeded,
				Blocked: &BlockedReason{Code: "dependency", NodeName: "n"}}}},
		Plan: Plan{Version: 2, Steps: []Step{{Seq: 1, GroupID: "a"}},
			VoterBases: []PlanVoterBase{{GroupID: "a", Voters: []uint64{1}}}},
		Previous: MakeOperationID(1, 1),
	}
	owned := original.Clone()
	if !reflect.DeepEqual(original, owned) {
		t.Fatal("clone changed the full aggregate or provenance")
	}
	owned.Status.Blocks[0].GroupID = "b"
	owned.Status.Blocks[0].Blocked.NodeName = "other"
	owned.Plan.Steps[0].GroupID = "b"
	owned.Plan.VoterBases[0].Voters[0]++
	if original.Status.Blocks[0].GroupID != "a" || original.Status.Blocks[0].Blocked.NodeName != "n" ||
		original.Plan.Steps[0].GroupID != "a" || original.Plan.VoterBases[0].Voters[0] != 1 {
		t.Fatal("clone shares mutable execution evidence")
	}
	transition := OperationTransition{OperationID: original.Status.ID, Blocked: original.Status.Blocks[0].Blocked}
	copied := transition.Clone()
	copied.Blocked.Code = "leadership"
	if transition.Blocked.Code != "dependency" || (OperationStatus{}).Clone().Blocks != nil ||
		(OperationTransition{}).Clone().Blocked != nil {
		t.Fatal("clone aliases a diagnostic or changes absent evidence")
	}
}

func TestOperationStatusProjectionKeepsAggregate(t *testing.T) {
	t.Parallel()
	status := OperationStatus{ID: MakeOperationID(2, 1), State: OperationFailed, TotalBlocks: 2,
		BlocksTruncated: true, Blocks: []BlockStatus{{GroupID: "b", State: OperationSucceeded}}}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var restored OperationStatus
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(status, restored) || restored.Clone().State != OperationFailed {
		t.Fatal("partial successful blocks changed the full failed aggregate", string(data))
	}
}
