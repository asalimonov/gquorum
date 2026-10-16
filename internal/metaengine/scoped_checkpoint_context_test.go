//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCheckpointContextKeepsNodeProgressAfterSourceCompaction(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations, s.History = nil, nil
	s.Groups[1].Owner = ""
	s.FullyConverged.Version = s.Target.Version
	prepareScopedFixture(t, &s)
	if !s.nodeGroupReady(s.Groups[1], "n1", s.indexes) {
		t.Fatal("identical approved context lost the checkpoint member")
	}
	data, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !restored.nodeGroupReady(restored.Groups[1], "n1", restored.indexes) {
		t.Fatal("restored compacted checkpoint lost node completion proof")
	}
}

func TestScopedCheckpointContextRetainsFormerMemberAfterMetadataAdvances(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations, s.AuthorityRecords, s.AuthorityFloors = nil, nil, nil
	s.Revocations = nil
	s.History = []topology.Document{s.Target.Clone()}
	s.Target = s.Target.Clone()
	s.Target.Version, s.Target.ParentVersion = 3, 2
	for i := range s.Target.Groups {
		s.Target.Groups[i].Replicas[0].ReplicaID, s.Target.Groups[i].NextReplicaID = 2, 3
		s.Allocations[i].NextReplicaID = 3
	}
	s.Latest = s.Target.Clone()
	s.FullyConverged.Version = 2
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 3, s.Target.MetaGroup()
	s.Groups[1].Owner = ""
	prepareScopedFixture(t, &s)
	admission := AdmissionRecord{NodeName: "n1", MemberID: 2, InstanceID: "replacement"}
	former := unretiredFormerMembers(&s, s.indexes, s.authorityCatalog(), admission)
	if len(former) != 1 || !former[1] {
		t.Fatal("metadata advancement hid the compacted load predecessor", former)
	}
	// The retained approved v2 supplies the same exact old load declaration;
	// the newer metadata checkpoint cannot relabel its member as 2.
	doc, found := checkpointDocument(&s, s.indexes, s.Groups[1])
	if !found || doc.Version != 2 || doc.MemberID("n1") != 1 {
		t.Fatal("wrong equivalent checkpoint context", doc.Version, found)
	}
}

func TestScopedCompactedCheckpointRefusesAmbiguousMemberContext(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations, s.AuthorityRecords, s.AuthorityFloors = nil, nil, nil
	s.Revocations = nil
	s.History = []topology.Document{s.Target.Clone()}
	s.Target = s.Target.Clone()
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Target.MetaGroup().Replicas[0].ReplicaID, s.Target.MetaGroup().NextReplicaID = 2, 3
	s.Latest = s.Target.Clone()
	s.Allocations[0].NextReplicaID = 3
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 3, s.Target.MetaGroup()
	s.Groups[1].Owner = ""
	s.FullyConverged.Version = 2
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	// Both approved contexts name identical load replica 1, but disagree about
	// its durable process member. Map order must never choose its identity.
	for range 20 {
		if _, found := checkpointDocument(&s, index, s.Groups[1]); found {
			t.Fatal("ambiguous member context resolved by map iteration")
		}
	}
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("restore accepted ambiguous compacted ownership", err)
	}
}

func TestScopedRetentionKeepsSourceWhenEquivalentDeclarationsChangeMemberBinding(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations, s.AuthorityRecords, s.AuthorityFloors, s.Revocations = nil, nil, nil, nil
	s.History = append(s.History, s.Target.Clone())
	s.Target = s.Target.Clone()
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Target.MetaGroup().Replicas[0].ReplicaID, s.Target.MetaGroup().NextReplicaID = 2, 3
	s.Latest = s.Target.Clone()
	s.Allocations[0].NextReplicaID = 3
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 3, s.Target.MetaGroup()
	s.Groups[1].Owner = ""
	s.Nodes[0].SourceVersion, s.Nodes[0].MemberID = 3, 2
	s.FullyConverged.Version = 2
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	if s.indexes.documents[1].Version == 0 || s.nodeGroupReady(*s.indexes.groups["shard0"], "n1", s.indexes) {
		t.Fatal("pruning relabeled the old load checkpoint with a new metadata member")
	}
}
