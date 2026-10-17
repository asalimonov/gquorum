//go:build functional

package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaFullSnapshotMatrixPreservesHistoryOperationsAndRetirement(t *testing.T) {
	t.Parallel()
	for _, size := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			machine := snapshotMatrix(t, size)
			start := time.Now()
			snapshot, err := machine.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot) == 0 || len(snapshot) > 4<<20 {
				t.Fatal("full meta snapshot exceeds four MiB", len(snapshot))
			}
			var state scopedState
			if err := json.Unmarshal(snapshot, &state); err != nil {
				t.Fatal(err)
			}
			if len(state.History) < historyRetention || len(state.Latest.LoadGroups()) != 15 || len(state.Operations) < historyRetention || len(state.RetiredGroups) != 1 || len(state.Allocations) != 17 || state.FullyConverged.Version != historyRetention+2 || state.Target.Version != historyRetention+3 || state.Latest.Version != state.Target.Version {
				t.Fatal("matrix lost retained full-state workload", len(state.History), len(state.Operations), len(state.Allocations), state.FullyConverged.Version, state.Target.Version)
			}
			pending, lookupErr := machine.Operation(topology.MakeOperationID(state.Target.Version, 1))
			if lookupErr != nil || pending.State != topology.OperationWaitingForLeader {
				t.Fatal("pending execution missing", pending)
			}
			restored := mustMachine(t, state.Rules, nil)
			if err := restored.Restore(context.Background(), snapshot); err != nil {
				t.Fatal(err)
			}
			roundtrip, err := restored.Snapshot(context.Background())
			if err != nil || !bytes.Equal(snapshot, roundtrip) {
				t.Fatal("full snapshot restore changed canonical state", err)
			}
			if elapsed := time.Since(start); elapsed >= 2*time.Second {
				t.Fatal("snapshot serialization/validation/restore exceeded two seconds", elapsed)
			}
			next := restored.Latest()
			next.Groups = append(next.Groups, topology.Group{ID: "retire-on-approval", NextReplicaID: 4, Replicas: []topology.Replica{{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "node2", ReplicaID: 2, Role: topology.ReplicaVoter}, {NodeName: "node3", ReplicaID: 3, Role: topology.ReplicaVoter}}})
			encoded, err := Encode(ProposeDocument{Doc: next, ExpectedVersion: next.Version})
			if err != nil {
				t.Fatal(err)
			}
			result, err := restored.Apply(context.Background(), raftengine.Command{Index: state.AppliedIndex + 1, Term: 1, Data: encoded})
			var refusal ApplyResult
			if err != nil || json.Unmarshal(result.Data, &refusal) != nil || refusal.Accepted || refusal.Code != proposalErrorCode(fault.GroupRetired) {
				t.Fatal("restored retirement allowed ID reuse", refusal, err)
			}
			t.Logf("METRIC nodes=%d groups=15 history=%d snapshot_bytes=%d", size, len(state.History), len(snapshot))
		})
	}
}
