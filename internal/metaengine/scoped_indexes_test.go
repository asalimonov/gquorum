//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func indexedCodecFixture() scopedState {
	s := scopedCodecFixture()
	retry := s.Operations[0].Clone()
	retry.Previous = retry.Status.ID
	retry.Status.ID, retry.Status.Attempt = "exec-v3-a2", 2
	s.Operations = append(s.Operations, retry)
	s.Operations[len(s.Operations)-1].Status.State = topology.OperationPending
	s.Operations[len(s.Operations)-1].Status.Blocks[0].State = topology.OperationPending
	s.Groups[2].Owner = retry.Status.ID
	s.History = append(s.History, s.Latest.Clone()) // Duplicate views of the same version are legitimate.
	return s
}

func TestScopedIndexesRebuildExactOwnedLookups(t *testing.T) {
	t.Parallel()
	s := indexedCodecFixture()
	before, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	index := restored.indexes
	if !reflect.DeepEqual(before, index) {
		t.Fatal("restored lookups differ")
	}
	if index.first[3] != "exec-v3-a1" || index.latest[3] != "exec-v3-a2" || index.retries["exec-v3-a1"] != "exec-v3-a2" || index.rebases[4] != 6 {
		t.Fatal("lost attempt/rebase provenance", index)
	}
	if index.groups["a"].Owner != "exec-v3-a2" || index.nodes["b"].MemberID != 0 || index.intents[intentKey{1, "set_group_placement", strings.Repeat("0", 64)}].Version != 2 {
		t.Fatal("lost owners or normalized intent lookup")
	}
	id := storage.Identity{ClusterID: "c", GroupID: "a", NodeName: "n", ReplicaID: 7}
	old := index.cleanup[id]
	// Index keys must distinguish successor identities independently of their
	// proof validation; a copied predecessor receipt is not restorable proof.
	successor := *old
	successor.Identity.ReplicaID++
	restored.Cleanup = append(restored.Cleanup, successor)
	identityIndex, err := restored.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	id.ReplicaID++
	if identityIndex.cleanup[id] == nil || identityIndex.cleanup[id].Identity == old.Identity || old.Identity.ReplicaID != 7 {
		t.Fatal("successor identity collapsed into predecessor")
	}
	index.groups["a"].Owner = "owned scratch"
	if s.Groups[2].Owner != "exec-v3-a2" || restored.Groups[1].Owner != "owned scratch" {
		t.Fatal("index aliases original input or an unrelated row copy")
	}
	if bytes.Contains(data, []byte(`"indexes"`)) || bytes.Contains(data, []byte(`"obligations_index"`)) {
		t.Fatal("derived state became independently durable")
	}
}

func TestScopedIndexesRejectAmbiguousKeysAtomically(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"operation ID": func(s *scopedState) { s.Operations[1].Status.ID = s.Operations[0].Status.ID },
		"intent key": func(s *scopedState) {
			s.Intents[1].ExpectedVersion, s.Intents[1].Kind, s.Intents[1].Digest = s.Intents[0].ExpectedVersion, s.Intents[0].Kind, s.Intents[0].Digest
		},
		"retry fork": func(s *scopedState) {
			retry := s.Operations[len(s.Operations)-1].Clone()
			retry.Status.ID, retry.Status.Attempt = "exec-v3-a3", 3
			s.Operations = append(s.Operations, retry)
		},
		"rebase fork": func(s *scopedState) { s.History = append(s.History, topology.Document{Version: 7, RebasedFrom: 4}) },
		"self rebase": func(s *scopedState) { s.Latest.RebasedFrom = s.Latest.Version; s.History = s.History[:2] },
		"document version": func(s *scopedState) {
			conflict := s.Target.Clone()
			conflict.Comment = "different retained declaration"
			s.History = append(s.History, conflict)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := indexedCodecFixture()
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var previous scopedState
			if err := json.Unmarshal(data, &previous); err != nil {
				t.Fatal(err)
			}
			index := previous.indexes
			corrupt(&s)
			type wire scopedState
			bad, err := json.Marshal(wire(s))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(bad, &previous); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("accepted ambiguous lookup", err)
			}
			after, err := json.Marshal(previous)
			if err != nil || !bytes.Equal(data, after) || previous.indexes != index {
				t.Fatal("failed rebuild replaced prior state or indexes", err)
			}
		})
	}
}

func TestScopedObligationsShareResourcesAndIgnoreDebtCapacity(t *testing.T) {
	t.Parallel()
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules()}
	s.Rules.Limits.MaxPendingAuthorityRecords = 1
	s.Rules.Limits.MaxConcurrentGroups, s.Rules.Limits.MaxConcurrentCleanup = 1, 1
	const debtCount = 2500
	for i := range debtCount {
		group := fmt.Sprintf("work-%04d", i)
		host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: "n"}
		oldKey := topology.AuthorityKey{ClusterID: "c", GroupID: group, Revision: 1}
		key := topology.AuthorityKey{ClusterID: "c", GroupID: group, Revision: 2}
		target := topology.AuthorityTarget{NodeName: "n", MemberID: 1, ReplicaID: 1}
		old := scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{
			Grant: topology.AuthorityGrant{Key: oldKey, Purpose: topology.AuthorityEffect,
				Resources: []topology.AuthorityResource{host}, Targets: []topology.AuthorityTarget{target}}, Closed: true,
		}}
		if i%2 == 0 {
			old.Class = "cleanup"
		}
		if i == debtCount-1 {
			old.Effects = []topology.EffectReceipt{{Key: oldKey, Resource: host, Target: target}}
		}
		s.AuthorityRecords = append(s.AuthorityRecords, old, scopedAuthorityRecord{Class: "execution",
			authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityFence,
				Resources: []topology.AuthorityResource{host}}}})
		s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: host, Key: key})
		s.Revocations = append(s.Revocations, scopedRevocationRecord{Requested: true,
			OperationID: topology.MakeOperationID(uint64(i+1), 1), GroupID: group,
			Obligations: []topology.FenceObligation{{Key: key, Resource: host}}})
	}
	index, err := s.buildIndexes()
	if err != nil || len(index.obligations) != debtCount-1 {
		t.Fatal("wrong shared-resource prerequisites or debt cap", err)
	}
	for resource, keys := range index.obligations {
		if len(keys) != 1 || keys[0].Revision != 2 || keys[0].GroupID != resource.GroupID {
			t.Fatal("lost or duplicated exact resource obligation", resource, keys)
		}
	}
}
