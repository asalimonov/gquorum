//go:build !functional

package metaengine

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadyCleanupSelectsDetachedActionAndPreservesRequiredStop(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		state scopedState
		kind  topology.StepKind
	}{
		{"detached_stop", readyDetachedStopFixture(t), topology.StepStopReplica},
		{"detached_retirement", pendingScopedRetirement(t), topology.StepCleanupReplicaState},
		{"required_deletion_stop", pendingScopedDeletion(), ""},
		{"retried_required_stop", retryScopedDeletion(), ""},
		{"only_collection_remains", scopedCodecFixture(), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := test.state
			prepareScopedFixture(t, &s)
			before, _ := json.Marshal(s)
			work, err := queryReadyCleanup(&s, storage.Identity{}, math.MaxInt)
			if err != nil {
				t.Fatal(err)
			}
			if test.kind == "" {
				if len(work) != 0 {
					t.Fatal("required or collected debt dispatched as cleanup", work)
				}
				return
			}
			if len(work) != 1 || work[0].Action.Kind != test.kind || work[0].Identity != s.Cleanup[0].Identity || work[0].MemberID != s.Cleanup[0].MemberID {
				t.Fatal(work)
			}
			again, err := queryReadyCleanup(&s, work[0].Identity, 1)
			if err != nil || !reflect.DeepEqual(work, again) {
				t.Fatal("rotation failed", again, err)
			}
			work[0].Action.NodeName = "caller"
			after, _ := json.Marshal(s)
			if string(before) != string(after) {
				t.Fatal("selection mutated durable state")
			}
		})
	}
}

func TestReadyCleanupWholeGroupTargetsRotateIndependently(t *testing.T) {
	t.Parallel()
	s := pendingScopedRetirement(t)
	// Read-selection regression: the index is sufficient; issuance remains
	// independently checked against the full restore graph and exact target.
	original := s.Cleanup[0].clone()
	other := original.clone()
	other.Identity.ReplicaID, other.StopAction.ReplicaID, other.RetireAction.ReplicaID = 6, 6, 6
	s.Cleanup = append(s.Cleanup, other)
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	// Use the same whole-group deletion action for both exact identities.
	for _, row := range s.indexes.cleanupOrder {
		if row.Identity.GroupID == original.Identity.GroupID {
			row.RetireAction.Kind = topology.StepDeleteGroupState
			row.RetireAction.NodeName, row.RetireAction.ReplicaID = "", 0
		}
	}
	op := s.indexes.operations[topology.OperationID(original.RetireAction.OperationID)]
	step := &op.Plan.Steps[original.RetireAction.StepSeq-1]
	step.Kind, step.NodeName, step.ReplicaID = topology.StepDeleteGroupState, "", 0
	first, err := queryReadyCleanup(&s, storage.Identity{}, 1)
	if err != nil || len(first) != 1 {
		t.Fatal(first, err)
	}
	second, err := queryReadyCleanup(&s, first[0].Identity, 1)
	if err != nil || len(second) != 1 || second[0].Identity == first[0].Identity || second[0].Action != first[0].Action {
		t.Fatal(second, err)
	}
	wrapped, err := queryReadyCleanup(&s, second[0].Identity, 1)
	if err != nil || len(wrapped) != 1 || wrapped[0].Identity != first[0].Identity {
		t.Fatal(wrapped, err)
	}
}

func readyDetachedStopFixture(t *testing.T) scopedState {
	t.Helper()
	s := pendingReplacementCleanup()
	prepareScopedFixture(t, &s)
	origin := s.indexes.operations[s.Cleanup[0].OperationID]
	origin.Plan.Blocks[0].Steps, origin.Plan.Blocks[0].Cleanup = []int{1}, []int{2, 3}
	origin.Status.Blocks[0].CurrentStep, origin.Status.Blocks[0].TotalSteps = 1, 1
	for i := range s.AuthorityRecords {
		r := &s.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Class = "cleanup"
		}
	}
	prepareScopedFixture(t, &s)
	return s
}
