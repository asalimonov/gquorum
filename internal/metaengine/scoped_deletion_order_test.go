//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedDeletionHardRefusalOutranksPendingProof(t *testing.T) {
	t.Parallel()
	s, proposal := readmissionDeletionFixture(t)
	next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	collector := s.planningCollector()
	if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(s.planningDeclarations().Groups)
	// The first owner has a usable unconsumed batch. The second has no exact
	// coverage. Unavailable must never hide that hard error and accept approval.
	collector.owners[planningReplicaKey{"shard0", 9}] = &planningReplica{
		target: topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 9}, progress: topology.RetainedHosted}
	before, _ := json.Marshal(next)
	source, _ := json.Marshal(s)
	for range 200 {
		if err := prepareDeletionCompletions(&s, next, collector, DeletionSemanticsLegacy); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("pending proof hid a hard refusal", err)
		}
		after, _ := json.Marshal(next)
		if !bytes.Equal(before, after) {
			t.Fatal("refused completion changed the candidate")
		}
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(source, after) {
		t.Fatal("refused completion changed its source")
	}
}

func TestScopedDeletionDebtHardRefusalOutranksMissingIssuance(t *testing.T) {
	t.Parallel()
	s, proposal := readmissionDeletionFixture(t)
	collector := s.planningCollector()
	collector.evidence.authority = newAuthorityCatalog()
	if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(s.planningDeclarations().Groups)
	collector.owners[planningReplicaKey{"shard0", 9}] = &planningReplica{
		target: topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 9}, progress: topology.RetainedHosted}
	next := s.prepareApprovalOwners(s.Latest, proposal)
	before, _ := json.Marshal(next)
	for range 200 {
		if err := prepareDeletionDebts(&s, next, collector, DeletionSemanticsLegacy); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("missing issuance hid missing historical storage", err)
		}
		after, _ := json.Marshal(next)
		if !bytes.Equal(before, after) {
			t.Fatal("refused debt preparation published partial debt or retirement")
		}
	}
	delete(collector.owners, planningReplicaKey{"shard0", 9})
	for range 20 {
		if err := prepareDeletionDebts(&s, next, collector, DeletionSemanticsLegacy); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("pure missing issuance stopped being retryable", err)
		}
	}
}
