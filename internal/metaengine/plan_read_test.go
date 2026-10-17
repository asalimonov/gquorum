//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestPlanReadRetainsCleanedIdentityWithoutInventingConvergence(t *testing.T) {
	t.Parallel()
	s := retryScopedDeletion()
	m := restoreMachineFixture(t, s)
	original := m.scoped.indexes.operations["exec-v2-a2"]
	data, err := json.Marshal(QueryRequest{Kind: "operation_plan", OperationID: original.Status.ID, Key: "b"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	var read PlanRead
	if err := json.Unmarshal(result.Data, &read); err != nil {
		t.Fatal(err)
	}
	retained := read.Input.Retained["a"].Replicas
	if len(retained) != 1 || retained[0].ReplicaID != 7 || retained[0].Progress != topology.RetainedStopped ||
		!read.Operation.Plan.Equal(original.Plan) || read.Input.Groups["a"].ConvergedVersion != 1 ||
		read.Input.Groups["a"].Converged.Replicas[0].ReplicaID != 7 {
		t.Fatal("retry lost original identity or invented convergence", read)
	}
	read.Input.Retained["a"].Replicas[0].ReplicaID = 99
	read.Input.Groups["a"].Converged.Replicas[0].ReplicaID = 99
	if m.scoped.Cleanup[0].Identity.ReplicaID != 7 || m.scoped.indexes.groups["a"].Declaration.Replicas[0].ReplicaID != 7 {
		t.Fatal("query result aliases metadata")
	}
}
