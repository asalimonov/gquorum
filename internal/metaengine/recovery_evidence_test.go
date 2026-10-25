//go:build !functional

package metaengine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryEvidenceSelectsOwnedCurrentSameKeyPair(t *testing.T) {
	t.Parallel()
	state, proposal := consumedRecoveryFixture(t)
	m := &Machine{scoped: &state}
	proof := model.CheckpointProof{Identity: proposal.Evidence.Identity, Position: proposal.Evidence.Checkpoint,
		Digest: proposal.Evidence.SnapshotDigest}
	evidence, err := m.RecoveryEvidence(t.Context(), proof)
	if err != nil || !reflect.DeepEqual(evidence, proposal.Evidence) {
		t.Fatal(evidence, err)
	}
	evidence.GroupReceipt.Action.GroupID = "changed"
	evidence.HostReceipt.Action.NodeName = "changed"
	again, err := m.RecoveryEvidence(t.Context(), proof)
	if err != nil || !reflect.DeepEqual(again, proposal.Evidence) {
		t.Fatal("selector aliased retained receipts", again, err)
	}
}

func TestRecoveryEvidenceRefusesIncompleteMixedAndUncoveredPairs(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"incomplete", "mixed", "uncovered", "wrong identity", "newer host floor"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			state, proposal := consumedRecoveryFixture(t)
			m := &Machine{scoped: &state}
			proof := model.CheckpointProof{Identity: proposal.Evidence.Identity, Position: proposal.Evidence.Checkpoint,
				Digest: proposal.Evidence.SnapshotDigest}
			record := state.indexes.grants[proposal.Evidence.GroupReceipt.Key]
			switch shape {
			case "incomplete":
				record.Effects = record.Effects[:1]
			case "mixed":
				for i := range record.Effects {
					if record.Effects[i].Resource.Kind == topology.AuthorityGroup {
						record.Effects[i].Key.Revision++
					}
				}
			case "uncovered":
				proof.Position.Index = 0
			case "wrong identity":
				proof.Identity.ReplicaID++
			case "newer host floor":
				for i := range state.AuthorityFloors {
					if state.AuthorityFloors[i].Resource.Kind == topology.AuthorityHost {
						state.AuthorityFloors[i].Key.Revision++
					}
				}
			}
			if _, err := m.RecoveryEvidence(t.Context(), proof); err == nil {
				t.Fatal("unsafe pair selected", shape)
			}
			if shape == "incomplete" {
				if _, err := m.RecoveryEvidence(t.Context(), proof); !errors.Is(err, fault.ErrAuthorityPending) {
					t.Fatal(err)
				}
			}
		})
	}
}
