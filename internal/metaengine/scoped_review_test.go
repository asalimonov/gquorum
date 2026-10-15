//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedObligationsMatchIssuerAndRevocationSettlement(t *testing.T) {
	t.Parallel()
	for _, kind := range []topology.StepKind{topology.StepCleanupReplicaState, topology.StepTransferLeader} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n"}
			oldKey := topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: 3}
			key := topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: 9}
			target := topology.AuthorityTarget{NodeName: "n", MemberID: 1, ReplicaID: 1}
			old := scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{
				Grant: topology.AuthorityGrant{Key: oldKey, Purpose: topology.AuthorityEffect,
					Action: &topology.AuthorizedStep{Kind: kind}, Resources: []topology.AuthorityResource{host},
					Targets: []topology.AuthorityTarget{target}},
				Effects: []topology.EffectReceipt{{Key: oldKey, Resource: host, Target: target}},
			}}
			fence := scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{
				Grant: topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityFence,
					Resources: []topology.AuthorityResource{host}},
			}}
			state := scopedState{AuthorityRecords: []scopedAuthorityRecord{old, fence},
				AuthorityFloors: []resourceFloor{{Resource: host, Key: key}},
				Revocations:     []scopedRevocationRecord{{Obligations: []topology.FenceObligation{{Key: key, Resource: host}}}}}
			// A transfer receipt consumes the command, but revocation must still fence resumable intent.
			want := 0
			if kind == topology.StepTransferLeader {
				want = 1
			}
			if got := scopedResourceObligations(&state); len(got[host]) != want {
				t.Fatal("late consumed effect/unnecessary fence disagrees with issuer", got)
			}
			state.AuthorityRecords[0].Effects = nil
			state.AuthorityRecords[1].Fences = []topology.FenceEvidence{{Key: key, Resource: host}}
			if got := scopedResourceObligations(&state); len(got) != 0 {
				t.Fatal("newer durable fence left older grant or revocation blocked", got)
			}
			// Only current floor plus revocations gate issuance, not all older archived grants.
			state.AuthorityFloors[0].Key = oldKey
			state.AuthorityRecords[0].Effects = old.Effects
			state.AuthorityRecords = state.AuthorityRecords[:1]
			state.Revocations = nil
			if got := scopedResourceObligations(&state); len(got) != 0 {
				t.Fatal("consumed current effect should allow the next ordinary action", got)
			}
		})
	}
}

func TestScopedDocumentIndexIsImmutableAndFirstMeansOriginal(t *testing.T) {
	t.Parallel()
	fixture := indexedCodecFixture()
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	restored.Target.Nodes[0].Type = "changed nested value"
	if restored.indexes.documents[restored.Target.Version].Nodes[0].Type != "worker" {
		t.Fatal("document index aliases nested mutable data")
	}
	restored.Target.Nodes[0].Type = "worker"
	version := restored.Latest.Version
	restored.Latest.Comment = "changed state slot"
	if restored.indexes.documents[version].Comment != fixture.Latest.Comment {
		t.Fatal("immutable document index aliases a mutable state slot")
	}
	restored.Latest.Comment = fixture.Latest.Comment
	restored.Operations = []topology.Operation{restored.Operations[2]}
	index, err := restored.buildIndexes()
	if err != nil || index.first[3] != "" || index.latest[3] != "exec-v3-a2" {
		t.Fatal("pruned original approval was replaced by a retry", err)
	}
	restored.Operations[0].Previous = ""
	if _, err := restored.buildIndexes(); err == nil {
		t.Fatal("retry without predecessor became an original approval")
	}
	restored.Operations[0].Status.ID = "different-id"
	if _, err := restored.buildIndexes(); err == nil {
		t.Fatal("lookup identity differs from attempt fields")
	}
}

func TestCleanupEffectPayloadRequiresCompleteAction(t *testing.T) {
	t.Parallel()
	var proposal BeginCleanupEffectProposal
	if err := json.Unmarshal([]byte(`{"identity":{},"step_seq":3,"proposal_id":"old"}`), &proposal); err == nil {
		t.Fatal("cleanup proposal accepted a sequence without its exact action")
	}
}
