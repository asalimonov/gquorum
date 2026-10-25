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
	group := state.Recovery.Groups[0]
	evidence, err := state.recoveryEvidence(group, proposal.Checkpoint)
	if err != nil || evidence.Identity != proposal.Checkpoint.Identity || evidence.Checkpoint != proposal.Checkpoint.Position ||
		evidence.HostReceipt.Key != evidence.GroupReceipt.Key || evidence.HostReceipt.Key.Revision == 0 {
		t.Fatal(evidence, err)
	}
	expected := evidence
	expected.HostReceipt = evidence.HostReceipt.Clone()
	expected.GroupReceipt = evidence.GroupReceipt.Clone()
	evidence.GroupReceipt.Action.GroupID = "changed"
	evidence.HostReceipt.Action.NodeName = "changed"
	again, err := state.recoveryEvidence(group, proposal.Checkpoint)
	if err != nil || !reflect.DeepEqual(again, expected) {
		t.Fatal("selector aliased retained receipts", again, err)
	}
}
func TestRecoveryEvidenceRefusesIncompleteMixedAndUncoveredPairs(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"incomplete", "mixed", "uncovered", "wrong identity", "missing floor"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			state, proposal := consumedRecoveryFixture(t)
			group := state.Recovery.Groups[0]
			proof := proposal.Checkpoint
			record, err := state.currentRecoveryCreation(group)
			if err != nil {
				t.Fatal(err)
			}
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
			case "missing floor":
				for i := range state.AuthorityFloors {
					if state.AuthorityFloors[i].Resource.Kind == topology.AuthorityHost {
						state.AuthorityFloors[i].Key.Revision = 0
					}
				}
			}
			if _, err := state.recoveryEvidence(group, proof); err == nil {
				t.Fatal("unsafe pair selected", shape)
			}
			if shape == "incomplete" {
				if _, err := state.recoveryEvidence(group, proof); !errors.Is(err, fault.ErrAuthorityPending) {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestRecoveryCreationSurvivesLaterNonCreationFloors(t *testing.T) {
	t.Parallel()
	state, p := consumedRecoveryFixture(t)
	group := state.Recovery.Groups[0]
	birth, err := state.currentRecoveryCreation(group)
	if err != nil {
		t.Fatal(err)
	}
	fence := birth.Grant.Clone()
	fence.Key.Revision++
	fence.Action = nil
	fence.OperationID = ""
	fence.Purpose = topology.AuthorityFence
	state.AuthorityRecords = append(state.AuthorityRecords, scopedAuthorityRecord{authorityRecord: authorityRecord{Grant: fence}, Class: "execution"})
	state.indexes.grants[fence.Key] = &state.AuthorityRecords[len(state.AuthorityRecords)-1]
	for i := range state.AuthorityFloors {
		if state.AuthorityFloors[i].Resource.GroupID == group.GroupID {
			state.AuthorityFloors[i].Key = fence.Key
		}
	}
	evidence, err := state.recoveryEvidence(group, p.Checkpoint)
	if err != nil || evidence.GroupReceipt.Key != birth.Grant.Key {
		t.Fatal("non-creation floor erased creation proof", evidence, err)
	}
}

func TestRecoveryReceiptRequiresSeedOrNewLineagePosition(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name                string
		receipt, checkpoint model.Position
		valid               bool
	}{
		{"seed", model.Position{Index: 1, Term: 1}, model.Position{Index: 2, Term: 1}, true},
		{"copied tail", model.Position{Index: 2, Term: 1}, model.Position{Index: 3, Term: 2}, false},
		{"old term", model.Position{Index: 3, Term: 1}, model.Position{Index: 3, Term: 2}, false},
		{"new lineage", model.Position{Index: 3, Term: 2}, model.Position{Index: 3, Term: 2}, true},
		{"beyond checkpoint", model.Position{Index: 4, Term: 2}, model.Position{Index: 3, Term: 2}, false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			state, p := consumedRecoveryFixture(t)
			group := state.Recovery.Groups[0]
			record, err := state.currentRecoveryCreation(group)
			if err != nil {
				t.Fatal(err)
			}
			for i := range record.Effects {
				if record.Effects[i].Resource.Kind == topology.AuthorityGroup {
					record.Effects[i].AppliedIndex, record.Effects[i].AppliedTerm = shape.receipt.Index, shape.receipt.Term
				}
			}
			p.Checkpoint.Position = shape.checkpoint
			_, err = state.recoveryEvidence(group, p.Checkpoint)
			if (err == nil) != shape.valid {
				t.Fatal(shape.name, err)
			}
		})
	}
}

func TestRecoveryCreationSelectsRetryPairWithoutMixingAttempts(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"incomplete successor", "complete successor", "fenced successor", "foreign operation"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			state, p := consumedRecoveryFixture(t)
			group := state.Recovery.Groups[0]
			birth, err := state.currentRecoveryCreation(group)
			if err != nil {
				t.Fatal(err)
			}
			previous := state.indexes.operations[state.indexes.latest[state.Recovery.Version]]
			retry := previous.Clone()
			retry.Previous = previous.Status.ID
			retry.Status.Attempt++
			retry.Status.ID = topology.MakeOperationID(retry.Status.Version, retry.Status.Attempt)
			state.indexes.operations[retry.Status.ID] = &retry
			state.indexes.latest[retry.Status.Version] = retry.Status.ID
			grant := birth.Grant.Clone()
			grant.Key.Revision++
			grant.OperationID = retry.Status.ID
			grant.Action.OperationID = retry.Status.ID
			grant.Action.Attempt = retry.Status.Attempt
			if shape == "foreign operation" {
				grant.OperationID = "foreign"
				grant.Action.OperationID = "foreign"
			}
			next := &scopedAuthorityRecord{authorityRecord: authorityRecord{Grant: grant}, Class: "execution"}
			for _, receipt := range fixtureEffectReceipts(grant) {
				if receipt.Resource.Kind == topology.AuthorityHost {
					receipt.LocalSequence = 2
					next.Effects = append(next.Effects, receipt)
				}
				if receipt.Resource.Kind == topology.AuthorityGroup && shape == "complete successor" {
					receipt.AppliedIndex, receipt.AppliedTerm = 3, 2
					next.Effects = append(next.Effects, receipt)
				}
			}
			state.indexes.grants[grant.Key] = next
			for i := range state.AuthorityFloors {
				if state.AuthorityFloors[i].Resource.GroupID == group.GroupID {
					state.AuthorityFloors[i].Key = grant.Key
				}
			}
			if shape == "fenced successor" {
				fence := grant.Clone()
				fence.Key.Revision++
				fence.Purpose = topology.AuthorityFence
				fence.Action = nil
				fence.OperationID = ""
				record := &scopedAuthorityRecord{authorityRecord: authorityRecord{Grant: fence}, Class: "execution"}
				for _, resource := range fence.Resources {
					proof := topology.FenceEvidence{Key: fence.Key, Resource: resource, Receiver: topology.Identity{
						NodeName: group.Destination.NodeName, ClusterID: group.Destination.ClusterID, MemberID: state.Recovery.Survivor.ReplicaID, InstanceID: "run"}, ReplicaID: group.Destination.ReplicaID}
					if resource.Kind == topology.AuthorityHost {
						proof.LocalSequence = 3
						proof.ReplicaID = 0
					} else {
						proof.AppliedIndex, proof.AppliedTerm = 4, 2
					}
					if err := proof.Validate(fence); err != nil {
						t.Fatal(err)
					}
					record.Fences = append(record.Fences, proof)
				}
				state.indexes.grants[fence.Key] = record
				for i := range state.AuthorityFloors {
					if state.AuthorityFloors[i].Resource.GroupID == group.GroupID {
						state.AuthorityFloors[i].Key = fence.Key
					}
				}
			}
			if shape == "complete successor" {
				p.Checkpoint.Position = model.Position{Index: 3, Term: 2}
			}
			evidence, err := state.recoveryEvidence(group, p.Checkpoint)
			if shape == "incomplete successor" {
				if !errors.Is(err, fault.ErrAuthorityPending) {
					t.Fatal("incomplete newer creation was bypassed", evidence, err)
				}
				return
			}
			want := birth.Grant.Key
			if shape == "complete successor" {
				want = grant.Key
			}
			if err != nil || evidence.HostReceipt.Key != want || evidence.GroupReceipt.Key != want {
				t.Fatal("wrong same-key creation pair", shape, evidence, err)
			}
		})
	}
}
