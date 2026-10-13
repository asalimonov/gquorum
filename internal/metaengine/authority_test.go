//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func authorityFixture(t *testing.T, limit int) (*Machine, topology.AuthorizedStep) {
	t.Helper()
	rules := testRules()
	rules.Limits.MaxPendingAuthorityRecords = limit
	m := NewMachine(rules, nil)
	apply(t, m.topologySM, 1, ProposeDocument{Doc: baseDoc()})
	at := time.Unix(100, 0).UTC()
	begin := apply(t, m.topologySM, 2, beginProposal(1, 2, "begin", at))
	if !begin.Accepted {
		t.Fatal(begin)
	}
	a := topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, Kind: topology.StepTransferLeader, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
	r := apply(t, m.topologySM, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: a.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning, GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second)}})
	if !r.Accepted {
		t.Fatal(r)
	}
	return m, a
}
func TestAuthorityIssuanceExactIdempotentAndOwned(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 3)
	r := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a, ProposalID: "grant"})
	if !r.Accepted || r.Grant == nil || r.Grant.Key.Revision != 4 || r.Grant.Validate() != nil || m.authority.pendingCost() != 3 {
		t.Fatal(r)
	}
	original := r.Grant.Clone()
	r.Grant.Action.NodeName = "changed"
	copy, ok := m.AuthorityGrant(original.Key)
	if !ok || !copy.Equal(original) {
		t.Fatal(copy, ok)
	}
	retry := apply(t, m.topologySM, 5, BeginEffectProposal{Action: a})
	if !retry.Accepted || !retry.Grant.Equal(original) {
		t.Fatal(retry)
	}
	if err := m.ValidateGrant(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	copy.Targets[0].MemberID++
	if err := m.ValidateGrant(t.Context(), copy); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	bad := a
	bad.StepSeq = 2
	if got := apply(t, m.topologySM, 6, BeginEffectProposal{Action: bad}); got.Accepted {
		t.Fatal("future step granted", got)
	}
	bad = a
	bad.OperatorID = ""
	bad.StepSeq = 0
	bad.Attempt = 0
	if got := apply(t, m.topologySM, 7, BeginEffectProposal{Action: bad}); got.Accepted {
		t.Fatal("direct transfer bypass granted", got)
	}
	encoded, err := json.Marshal(retry)
	if err != nil {
		t.Fatal(err)
	}
	var result ApplyResult
	if err = json.Unmarshal(encoded, &result); err != nil || result.Grant == nil || !result.Grant.Equal(original) {
		t.Fatal(result, err)
	}
}
func TestAuthorityCapacityReservesRevocation(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 2)
	r := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a})
	if r.Accepted || r.Code != proposalOverloaded || len(m.authority.records) != 0 || len(m.authority.floors) != 0 {
		t.Fatal(r)
	}
}
func TestAuthoritySnapshotRebuildsIndexesAndRejectsCorruption(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 3)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewMachine(m.rules, nil)
	if err := fresh.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if r := apply(t, fresh.topologySM, 5, BeginEffectProposal{Action: a}); !r.Accepted || !r.Grant.Equal(*grant) {
		t.Fatal(r)
	}
	for name, mutate := range map[string]func(*smState){
		"duplicate":       func(s *smState) { s.AuthorityRecords = append(s.AuthorityRecords, s.AuthorityRecords[0]) },
		"future":          func(s *smState) { s.AuthorityRecords[0].Grant.Key.Revision = 9 },
		"foreign":         func(s *smState) { s.AuthorityRecords[0].Grant.Key.ClusterID = "foreign" },
		"missing_floor":   func(s *smState) { s.AuthorityFloors = nil },
		"orphan_floor":    func(s *smState) { s.AuthorityFloors[0].Key.Revision++ },
		"duplicate_floor": func(s *smState) { s.AuthorityFloors = append(s.AuthorityFloors, s.AuthorityFloors[0]) },
		"invalid_action":  func(s *smState) { s.AuthorityRecords[0].Grant.Action.ReplicaID = 99 },
		"partial_receipt": func(s *smState) { s.AuthorityRecords[0].Effects = []topology.EffectReceipt{{Key: grant.Key}} },
		"orphan_revocation": func(s *smState) {
			s.Revocations = []revocationRecord{{OperatorID: a.OperatorID, Obligations: []topology.FenceObligation{{Key: topology.AuthorityKey{Revision: 99}}}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			var state smState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			mutate(&state)
			bad, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := fresh.Restore(t.Context(), bad); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(err)
			}
			if err := fresh.ValidateGrant(t.Context(), *grant); err != nil {
				t.Fatal("failed restore mutated live catalog", err)
			}
		})
	}
}
func TestAuthorityGroupTargetsIncludeReceiversButExcludeStagedIntent(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 1024)
	m.active = m.target.Clone()
	m.active.Groups[0].Replicas = append(m.active.Groups[0].Replicas, topology.Replica{NodeName: "old", ReplicaID: 2, Role: topology.ReplicaVoter})
	m.active.Groups[1].Replicas = append(m.active.Groups[1].Replicas, topology.Replica{NodeName: "old", ReplicaID: 9, Role: topology.ReplicaVoter})
	m.latest = m.target.Clone()
	m.latest.Version = 2
	m.latest.Groups[0].Replicas = append(m.latest.Groups[0].Replicas, topology.Replica{NodeName: "staged", ReplicaID: 99, Role: topology.ReplicaVoter})
	m.latest.Groups[1].Replicas = append(m.latest.Groups[1].Replicas, topology.Replica{NodeName: "staged", ReplicaID: 99, Role: topology.ReplicaVoter})
	r := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a})
	if !r.Accepted || len(r.Grant.Targets) != 2 || r.Grant.Targets[1].NodeName != "old" || r.Grant.Targets[1].MemberID != 2 || r.Grant.Targets[1].ReplicaID != 9 {
		t.Fatal(r)
	}
}

func supersedeAuthority(t *testing.T, m *Machine, index uint64) topology.OperatorID {
	t.Helper()
	doc := m.latest.Clone()
	version := doc.Version + 1
	doc.Version = version
	if r := apply(t, m.topologySM, index, ProposeDocument{Doc: doc, ExpectedVersion: version - 1}); !r.Accepted {
		t.Fatal(r)
	}
	r := apply(t, m.topologySM, index+1, beginProposal(version, 2, "replacement", time.Unix(200+int64(index), 0).UTC()))
	if !r.Accepted {
		t.Fatal(r)
	}
	return r.OperatorID
}
func groupFenceProof(grant topology.AuthorityGrant, index uint64) topology.FenceEvidence {
	return topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0], Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "process"}, ReplicaID: 1, AppliedIndex: index, AppliedTerm: 2}
}
func groupEffectProof(grant topology.AuthorityGrant, index uint64) topology.EffectReceipt {
	outcome := topology.EffectApplied
	if grant.Action.Kind == topology.StepTransferLeader {
		outcome = topology.EffectTransferStarted
	}
	return topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Action: grant.Action, Target: grant.Targets[0], Outcome: outcome, AppliedIndex: index, AppliedTerm: 2}
}
func TestSupersessionPersistsRevocationAtReservedCapacity(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 3)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	_ = supersedeAuthority(t, m, 5)
	status, err := m.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) != 1 {
		t.Fatal(status, err)
	}
	if !m.authority.records[grant.Key].Closed || m.authority.pendingCost() != 3 {
		t.Fatal("reserved revocation was not durable")
	}
	// Being closed does not make the immutable committed record unverifiable.
	if err := m.ValidateGrant(t.Context(), *grant); err != nil {
		t.Fatal(err)
	}
	pending := m.PendingFences()
	fence, ok := m.AuthorityGrant(pending[0].Key)
	if !ok || fence.Purpose != topology.AuthorityFence || fence.Key.Revision != 6 {
		t.Fatal(fence, ok)
	}
	r := apply(t, m.topologySM, 7, RecordFenceProposal{Evidence: groupFenceProof(fence, 20)})
	if !r.Accepted {
		t.Fatal(r)
	}
	status, err = m.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationComplete || status.Obligations[0].Evidence == nil || len(m.PendingFences()) != 0 {
		t.Fatal(status, err)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	status, err = restored.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationComplete {
		t.Fatal(status, err)
	}
}
func TestLateConsumptionCanSettleDestroyedGroupWithoutFence(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 3)
	a.Kind = topology.StepPromoteVoter
	m.operators[a.OperatorID].Steps[0].Kind = a.Kind
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	supersedeAuthority(t, m, 5)
	r := apply(t, m.topologySM, 7, RecordEffectProposal{Receipt: groupEffectProof(*grant, 10)})
	if !r.Accepted {
		t.Fatal(r)
	}
	status, err := m.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationComplete || len(status.Obligations) != 0 || len(m.PendingFences()) != 0 || m.authority.pendingCost() != 0 {
		t.Fatal(status, err, m.authority.pendingCost())
	}
	// Repeated delivery returns the same consumption instead of executing again.
	if r := apply(t, m.topologySM, 8, RecordEffectProposal{Receipt: groupEffectProof(*grant, 10)}); !r.Accepted {
		t.Fatal(r)
	}
	bad := groupEffectProof(*grant, 11)
	if r := apply(t, m.topologySM, 9, RecordEffectProposal{Receipt: bad}); r.Accepted {
		t.Fatal("contradictory receipt accepted")
	}
}
func TestRepeatedSupersessionCarriesPendingObligations(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 8)
	apply(t, m.topologySM, 4, BeginEffectProposal{Action: a})
	second := supersedeAuthority(t, m, 5)
	third := supersedeAuthority(t, m, 7)
	if third == second || len(m.PendingFences()) != 1 {
		t.Fatal(third, second, m.PendingFences())
	}
	status, err := m.AuthorityStatus(second)
	if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) != 1 {
		t.Fatal(status, err)
	}
	fence, _ := m.AuthorityGrant(m.PendingFences()[0].Key)
	if r := apply(t, m.topologySM, 9, RecordFenceProposal{Evidence: groupFenceProof(fence, 20)}); !r.Accepted {
		t.Fatal(r)
	}
	for _, id := range []topology.OperatorID{a.OperatorID, second} {
		status, err = m.AuthorityStatus(id)
		if err != nil || !status.RevocationComplete {
			t.Fatal(status, err)
		}
	}
}
func TestReceiptsRejectForeignOrRegressingBoundaries(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 8)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	proof := groupFenceProof(*grant, 10)
	if r := apply(t, m.topologySM, 5, RecordFenceProposal{Evidence: proof}); !r.Accepted {
		t.Fatal(r)
	}
	bad := groupEffectProof(*grant, 9)
	if r := apply(t, m.topologySM, 6, RecordEffectProposal{Receipt: bad}); r.Accepted {
		t.Fatal("pre-install receipt accepted")
	}
	proof.Receiver.MemberID = 99
	if r := apply(t, m.topologySM, 7, RecordFenceProposal{Evidence: proof}); r.Accepted {
		t.Fatal("foreign receiver accepted")
	}
	supersedeAuthority(t, m, 8)
	fence, _ := m.AuthorityGrant(m.PendingFences()[0].Key)
	if r := apply(t, m.topologySM, 10, RecordFenceProposal{Evidence: groupFenceProof(fence, 9)}); r.Accepted {
		t.Fatal("regressing fence accepted")
	}
	if r := apply(t, m.topologySM, 11, RecordFenceProposal{Evidence: groupFenceProof(fence, 20)}); !r.Accepted {
		t.Fatal(r)
	}
	if r := apply(t, m.topologySM, 12, RecordEffectProposal{Receipt: groupEffectProof(*grant, 21)}); r.Accepted {
		t.Fatal("post-fence effect accepted")
	}
}

func TestTransferConsumptionRetainsFenceObligation(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 3)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	if r := apply(t, m.topologySM, 5, RecordEffectProposal{Receipt: groupEffectProof(*grant, 10)}); !r.Accepted {
		t.Fatal(r)
	}
	if m.authority.pendingCost() != 3 {
		t.Fatal("resumable transfer lost its fence reservation")
	}
	supersedeAuthority(t, m, 6)
	status, err := m.AuthorityStatus(a.OperatorID)
	if err != nil || status.RevocationComplete || len(status.Obligations) != 1 {
		t.Fatal(status, err)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	fence, _ := restored.AuthorityGrant(restored.PendingFences()[0].Key)
	if r := apply(t, restored.topologySM, 8, RecordFenceProposal{Evidence: groupFenceProof(fence, 11)}); !r.Accepted {
		t.Fatal(r)
	}
	status, err = restored.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationComplete || restored.authority.pendingCost() != 0 {
		t.Fatal(status, err)
	}
}
