//go:build !functional

package recoveryinspect

import (
	"testing"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestExactDonorClassificationRequiresApprovedOrProvenIdentityAndLedger(t *testing.T) {
	t.Parallel()
	id := storage.Identity{NodeName: "n1", ClusterID: "old", GroupID: "work", ReplicaID: 7}
	for _, class := range []string{"approved", "proven", "pending_predecessor", "retired", "approved_deleted", "staged_only", "missing_ledger", "different_member", "ambiguous"} {
		t.Run(class, func(t *testing.T) {
			t.Parallel()
			state := SourceState{identity: topology.Identity{NodeName: "n1", ClusterID: "old", MemberID: 1},
				ledger:    map[string]metastore.ReplicaRecord{"work": {NodeName: "n1", ClusterID: "old", ReplicaID: 7}},
				inventory: metaengine.RecoveryInventory{Replicas: []model.ReplicaEvidence{{Identity: id, MemberID: 1, SourceVersion: 2, Classification: class}}}}
			invalid := false
			switch class {
			case "staged_only":
				state.inventory.Replicas = nil
				invalid = true
			case "missing_ledger":
				state.ledger = nil
				state.inventory.Replicas[0].Classification = "proven"
				invalid = true
			case "different_member":
				state.inventory.Replicas[0].MemberID = 2
				invalid = true
			case "ambiguous":
				state.inventory.Replicas = append(state.inventory.Replicas, model.ReplicaEvidence{Identity: id, MemberID: 2, SourceVersion: 2, Classification: "proven"})
				invalid = true
			}
			qualified, err := classifyIdentity(state, id)
			if invalid {
				if err == nil {
					t.Fatal("unproven source became a donor", qualified.Classification)
				}
				return
			}
			if err != nil || qualified.Classification != class || qualified.SourceVersion != 2 {
				t.Fatal(qualified, err)
			}
		})
	}
}
