//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// These snapshots, commands and expected results were captured by the unchanged
// FSM at 8e57798, including the ambiguous zero-step readmission approval.
func TestDeletionHistoricalCommittedReplayAfterSnapshot(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"required-stop", "inherited-debt", "pending-readmission"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			read := func(file string) []byte {
				data, err := os.ReadFile(filepath.Join("testdata/deletion-replay", name, file))
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			before, command, want := read("before.json"), read("command.json"), read("after.json")
			if bytes.Contains(command, []byte("deletion_semantics")) {
				t.Fatal("historical command has new marker")
			}
			var state scopedState
			if err := json.Unmarshal(before, &state); err != nil {
				t.Fatal(err)
			}
			m := restoreMachineFixture(t, state)
			index, term := m.AppliedState()
			result, err := m.Apply(t.Context(), raftengine.Command{Index: index + 1, Term: max(term, 1), Data: command})
			if err != nil {
				t.Fatal(err)
			}
			var applied ApplyResult
			if err := json.Unmarshal(result.Data, &applied); err != nil || !applied.Accepted {
				t.Fatal(applied, err)
			}
			got, err := m.Snapshot(t.Context())
			if err != nil || !bytes.Equal(want, got) {
				t.Fatal("historical replay changed original full snapshot", err)
			}
			status, err := m.Operation(applied.OperationID)
			if err != nil || status.State == topology.OperationSucceeded {
				t.Fatal("legacy approval acquired logical completion", status, err)
			}
			// Replaying an already-applied index cannot reinterpret the command.
			if _, err = m.Apply(t.Context(), raftengine.Command{Index: index + 1, Term: max(term, 1), Data: command}); err != nil {
				t.Fatal(err)
			}
			again, _ := m.Snapshot(t.Context())
			if !bytes.Equal(want, again) {
				t.Fatal("duplicate committed index changed legacy state")
			}
			op := m.scoped.indexes.operations[applied.OperationID].Clone()
			block := op.Plan.Blocks[0]
			seq := 0
			if len(block.Steps) != 0 {
				seq = block.Steps[0]
			}
			failed := scopedTransitionFor(op, block.GroupID, seq)
			failed.State, failed.Error = topology.OperationFailed, "offline"
			applyScopedTransitionFixture(t, m.scoped, failed)
			prior := m.scoped.indexes.operations[applied.OperationID]
			retry := applyScopedRetryFixture(t, m.scoped, scopedRetryProposal(m.scoped, prior))
			if !m.scoped.indexes.operations[retry.result.OperationID].Plan.Equal(op.Plan) ||
				!m.scoped.indexes.groups[block.GroupID].Deleting {
				t.Fatal("legacy retry migrated required deletion proof")
			}
		})
	}
}

func TestDeletionMarkedReplayAfterSnapshot(t *testing.T) {
	t.Parallel()
	state, p := deletionApprovalFixture(t)
	p = planDetachedApproval(t, &state, p)
	m := restoreMachineFixture(t, state)
	result := applyMachine(t, m, p)
	if !result.Accepted {
		t.Fatal(result)
	}
	status, err := m.Operation(result.OperationID)
	if err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	snapshot, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Restore(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	work, err := m.ReadyCleanup(storage.Identity{}, 1)
	if err != nil || len(work) != 1 || work[0].Action.Kind != topology.StepStopReplica {
		t.Fatal(work, err)
	}
}
