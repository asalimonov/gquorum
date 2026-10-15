//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedAuthorityFixture() scopedState {
	state := scopedGraphFixture()
	action := topology.AuthorizedStep{OperatorID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 1,
		GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 1}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}
	key := topology.AuthorityKey{ClusterID: state.Latest.ClusterID, GroupID: topology.MetaGroupID, Revision: 3}
	grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID,
		Action: &action, Resources: []topology.AuthorityResource{resource},
		Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
	receipt := fixtureEffectReceipts(grant)[0]
	receipt.AppliedIndex = 3
	state.AuthorityRecords = []scopedAuthorityRecord{{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: []topology.EffectReceipt{receipt}}}}
	key.Revision = 4
	fence := topology.FenceEvidence{Key: key, Resource: resource, ReplicaID: 1, AppliedIndex: 4, AppliedTerm: 1,
		Receiver: topology.Identity{ClusterID: key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "instance"}}
	grant.Key, grant.Purpose, grant.OperatorID, grant.Action = key, topology.AuthorityFence, "", nil
	state.AuthorityRecords = append(state.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Fences: []topology.FenceEvidence{fence}}})
	state.AuthorityFloors = []resourceFloor{{Resource: resource, Key: key}}
	state.Revocations = []scopedRevocationRecord{{Requested: true, OperationID: "exec-v2-a1",
		GroupID: topology.MetaGroupID, Obligations: []topology.FenceObligation{{Key: key, Resource: resource, Evidence: &fence}}}}
	return state
}

func TestScopedAuthorityExactCatalogAndRevocationProof(t *testing.T) {
	t.Parallel()
	good := scopedAuthorityFixture()
	if _, err := prepareScopedGraph(&good, testRules()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*scopedState){
		"forged class":             func(s *scopedState) { s.AuthorityRecords[0].Class = "cleanup" },
		"cleanup fence":            func(s *scopedState) { s.AuthorityRecords[1].Class = "cleanup" },
		"changed immutable action": func(s *scopedState) { s.AuthorityRecords[0].Grant.Action.StepSeq = 2 },
		"missing floor":            func(s *scopedState) { s.AuthorityFloors = nil },
		"decreased floor":          func(s *scopedState) { s.AuthorityFloors[0].Key.Revision = 3 },
		"future revision":          func(s *scopedState) { s.AppliedIndex = 3 },
		"duplicate receipt": func(s *scopedState) {
			s.AuthorityRecords[0].Effects = append(s.AuthorityRecords[0].Effects, s.AuthorityRecords[0].Effects[0])
		},
		"refused receipt":           func(s *scopedState) { s.AuthorityRecords[0].Effects[0].Outcome = "refused" },
		"receipt after newer fence": func(s *scopedState) { s.AuthorityRecords[0].Effects[0].AppliedIndex = 5 },
		"foreign receiver":          func(s *scopedState) { s.AuthorityRecords[1].Fences[0].Receiver.InstanceID = "" },
		"missing fence predecessor": func(s *scopedState) { s.AuthorityRecords = s.AuthorityRecords[1:]; s.AuthorityRecords[0].Fences = nil },
		"scope not in plan":         func(s *scopedState) { s.Revocations[0].GroupID = "elsewhere" },
		"wrong resource scope":      func(s *scopedState) { s.Revocations[0].GroupID = "shard0" },
		"duplicate obligation": func(s *scopedState) {
			s.Revocations[0].Obligations = append(s.Revocations[0].Obligations, s.Revocations[0].Obligations[0])
		},
		"unrecorded fence proof": func(s *scopedState) { s.Revocations[0].Obligations[0].Evidence.AppliedIndex = 5 },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := scopedAuthorityFixture()
			corrupt(&state)
			if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid authority proof accepted", err)
			}
		})
	}
}

func TestScopedAuthorityRetainsDetachedUnresolvedDebtBeyondOldCap(t *testing.T) {
	t.Parallel()
	state := scopedCodecFixture()
	state.Rules.Limits.MaxPendingAuthorityRecords = 1
	for i := range state.AuthorityRecords {
		state.AuthorityRecords[i].Effects = nil
		state.Cleanup[i].Retired, state.Cleanup[i].RetiredAt, state.Cleanup[i].Collection = nil, nil, nil
	}
	if _, err := prepareScopedGraph(&state, state.Rules); err != nil {
		t.Fatal("valid unresolved cleanup hit old cap", err)
	}
	state.Cleanup[0].RetireAction = nil
	if _, err := prepareScopedGraph(&state, state.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("archived unconsumed cleanup lost its exact action/debt binding", err)
	}
}
