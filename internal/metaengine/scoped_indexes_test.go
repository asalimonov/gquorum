//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
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
	s.Groups[0].Owner, s.Nodes[0].Owner = retry.Status.ID, retry.Status.ID
	s.History = append(s.History, s.Latest.Clone()) // Duplicate views of the same version are legitimate.
	successor := s.Cleanup[0]
	successor.Identity.ReplicaID++
	s.Cleanup = append(s.Cleanup, successor)
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
	if index.first[3] != "exec-v3-a1" || index.latest[3] != "exec-v3-a2" || index.retries["exec-v3-a1"] != "exec-v3-a2" || index.rebases[2] != 4 {
		t.Fatal("lost attempt/rebase provenance", index)
	}
	if index.groups["live"].Owner != "exec-v3-a2" || index.nodes["b"].Owner != "exec-v3-a2" || index.intents[intentKey{3, "add_node", "digest"}].Version != 4 {
		t.Fatal("lost owners or normalized intent lookup")
	}
	id := storage.Identity{ClusterID: "c", GroupID: "a", NodeName: "n", ReplicaID: 7}
	old := index.cleanup[id]
	id.ReplicaID++
	if old == nil || index.cleanup[id] == nil || old == index.cleanup[id] || old.Identity.ReplicaID != 7 {
		t.Fatal("successor identity collapsed into predecessor")
	}
	index.groups["live"].Owner = "owned scratch"
	if s.Groups[0].Owner != "exec-v3-a2" || restored.Groups[0].Owner != "owned scratch" {
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
			retry := s.Operations[2].Clone()
			retry.Status.ID, retry.Status.Attempt = "exec-v3-a3", 3
			s.Operations = append(s.Operations, retry)
		},
		"rebase fork": func(s *scopedState) { s.History = append(s.History, topology.Document{Version: 5, RebasedFrom: 2}) },
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
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n"}
	native := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}
	const debtCount = 2500
	for i := range debtCount {
		key := topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: uint64(i + 1)}
		target := topology.AuthorityTarget{NodeName: "n", MemberID: 1, ReplicaID: uint64(i + 1)}
		r := scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{
			Grant:   topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityEffect, Resources: []topology.AuthorityResource{host, native}, Targets: []topology.AuthorityTarget{target}},
			Effects: []topology.EffectReceipt{{Key: key, Resource: native, Target: target}},
		}}
		if i%2 == 0 {
			r.Class = "cleanup"
		}
		s.AuthorityRecords = append(s.AuthorityRecords, r)
		s.Revocations = append(s.Revocations, scopedRevocationRecord{Requested: true, OperationID: topology.MakeOperationID(uint64(i+1), 1), GroupID: "work", Obligations: []topology.FenceObligation{{Key: key, Resource: host}}})
	}
	// A closed grant cannot erase its independently outstanding revocation.
	s.AuthorityRecords[0].Closed = true
	// Evidence for the final host satisfies both its grant and revocation.
	last := &s.AuthorityRecords[debtCount-1]
	last.Effects = append(last.Effects, topology.EffectReceipt{Key: last.Grant.Key, Resource: host, Target: last.Grant.Targets[0]})
	s.Revocations[debtCount-1].Obligations[0].Evidence = &topology.FenceEvidence{Key: last.Grant.Key, Resource: host}
	index, err := s.buildIndexes()
	if err != nil || len(index.obligations[host]) != debtCount-1 || len(index.obligations[native]) != 0 {
		t.Fatal("wrong shared-resource prerequisites or debt cap", err)
	}
	for i, key := range index.obligations[host] {
		if key.Revision != uint64(i+1) {
			t.Fatal("duplicate or unsorted obligation", key)
		}
	}
}
