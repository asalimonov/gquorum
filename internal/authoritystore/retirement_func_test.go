//go:build functional

package authoritystore

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func finishRetirementFixture(t *testing.T, s *Store, grant topology.AuthorityGrant) topology.EffectReceipt {
	t.Helper()
	floor := completeLocal(t, s, grant)
	if _, err := s.BeginEffect(t.Context(), grant, grant.Targets[0]); err != nil {
		t.Fatal(err)
	}
	receipt := receiptFor(grant, floor.Sequence)
	receipt.AdmissionID = grant.AdmissionID
	if err := s.FinishEffect(t.Context(), receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestCompletedRetirementRetainsExactProofAcrossFencesAndRebind(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	input := localCollection(7)
	if value, err := s.CompletedRetirement(t.Context(), input.Retired.Identity); err != nil || value.Found {
		t.Fatal("absent retirement manufactured proof", value, err)
	}
	retired := finishRetirementFixture(t, s, input.Grant)
	other := localGrant(20)
	finishRetirementFixture(t, s, other) // A later prepare is not a retirement.
	value, err := s.CompletedRetirement(t.Context(), input.Retired.Identity)
	if err != nil || !value.Found || value.Record.Receipt.Key != retired.Key || !value.Record.Grant.Equal(input.Grant) {
		t.Fatal("later fence/effect replaced exact retirement", value, err)
	}
	readmit := input.Grant.Clone()
	readmit.Key.Revision, readmit.Purpose = 30, topology.AuthorityReadmit
	readmit.OperationID, readmit.Action, readmit.AdmissionID = "", nil, "replacement"
	readmissionReceipt := finishRetirementFixture(t, s, readmit)
	previous := s.Identity()
	next := previous
	next.MemberID++
	if err := s.Rebind(t.Context(), previous, next, "replacement"); err != nil {
		t.Fatal(err)
	}
	value, err = s.CompletedRetirement(t.Context(), input.Retired.Identity)
	if err != nil || !value.Found || value.Record.Receipt.Key != readmissionReceipt.Key || value.Record.Target.MemberID != 1 {
		t.Fatal("rebind relabeled historical retirement", value, err)
	}
	value.Record.Grant.Targets[0].ReplicaID++
	value.Record.Receipt.LocalSequence++
	value, err = s.CompletedRetirement(t.Context(), input.Retired.Identity)
	if err != nil || value.Record.Target.ReplicaID != 7 || value.Record.Receipt.LocalSequence != readmissionReceipt.LocalSequence {
		t.Fatal("lookup aliases journal evidence", value, err)
	}
	foreign := input.Retired.Identity
	foreign.ClusterID = "foreign"
	if _, err := s.CompletedRetirement(t.Context(), foreign); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign journal identity accepted", err)
	}
	otherID := input.Retired.Identity
	otherID.ReplicaID++
	if value, err := s.CompletedRetirement(t.Context(), otherID); err != nil || value.Found {
		t.Fatal("another replica borrowed retirement", value, err)
	}
}
