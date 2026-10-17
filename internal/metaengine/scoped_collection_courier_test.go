//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Later batches have metadata only: they rebind the journal without targeting
// the old load replica, which was already retired by the first batch.
func appendCollectionReadmission(s *scopedState, former, next uint64) topology.AuthorityKey {
	admission := AdmissionRecord{NodeName: "n", InstanceID: fmt.Sprintf("member-%d", next),
		MemberID: next, OperationID: topology.OperationID(fmt.Sprintf("exec-v%d-a1", next+10)), State: AdmissionSuperseded}
	s.Admissions = append(s.Admissions, admission)
	key := topology.AuthorityKey{ClusterID: "c", GroupID: topology.MetaGroupID, Revision: 20 + next}
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n"}
	grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityReadmit, AdmissionID: ReadmissionID(admission),
		Resources: []topology.AuthorityResource{resource},
		Targets:   []topology.AuthorityTarget{{NodeName: "n", MemberID: former, ReplicaID: former}}}
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: fixtureEffectReceipts(grant)}})
	for i := range s.AuthorityFloors {
		if s.AuthorityFloors[i].Resource == resource {
			s.AuthorityFloors[i].Key = key
		}
	}
	for i := range s.Allocations {
		if s.Allocations[i].GroupID == topology.MetaGroupID {
			s.Allocations[i].NextReplicaID = max(s.Allocations[i].NextReplicaID, next+1)
		}
	}
	s.Latest.MetaGroup().NextReplicaID = max(s.Latest.MetaGroup().NextReplicaID, next+1)
	return key
}

func collectionReadmissionFixture(t *testing.T) (scopedState, RecordCollectionProposal) {
	t.Helper()
	_, p := collectionProposalFixture(t)
	s := scopedReadmissionCleanupFixture()
	appendCollectionReadmission(&s, 2, 3)
	prepareScopedFixture(t, &s)
	p.Receipt.RetirementKey = s.Cleanup[0].Retired.Key
	p.Courier.MemberID = 3
	return s, p
}

func TestScopedCollectionFollowsWholeJournalReadmissions(t *testing.T) {
	t.Parallel()
	s, p := collectionReadmissionFixture(t)
	if s.Target.MemberID(p.Courier.NodeName) == p.Courier.MemberID {
		t.Fatal("fixture must not depend on current Target admission")
	}
	result, err := s.applyRecordCollection(31, p)
	if err != nil || !result.changed || !result.result.Collection.Receipt.Equal(p.Receipt) {
		t.Fatal("journal successor could not report old member collection", result, err)
	}
	// A lost accepted reply followed by another readmission still echoes the
	// exact old receipt. Neither the old row nor its timestamp is rewritten.
	last := appendCollectionReadmission(&s, 3, 4)
	prepareScopedFixture(t, &s)
	p.Courier.MemberID, p.Courier.InstanceID, p.ReadmissionKey = 4, "restarted-again", &last
	before, _ := json.Marshal(s)
	result, err = s.applyRecordCollection(32, p)
	after, _ := json.Marshal(s)
	if err != nil || result.changed || !bytes.Equal(before, after) || !result.result.Collection.Receipt.Equal(p.Receipt) {
		t.Fatal("readmitted duplicate lost historical evidence", result, err)
	}
}

func TestScopedCollectionCourierRequiresUniqueConsumedEndpoint(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stale original", "stale intermediate", "future", "partial", "ambiguous", "old hint", "missing hint"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := collectionReadmissionFixture(t)
			want := fault.ErrUnavailable
			switch mode {
			case "stale original", "stale intermediate":
				p.Courier.MemberID = 1
				if mode == "stale intermediate" {
					p.Courier.MemberID = 2
				}
				want = fault.ErrAuthorityFenced
			case "future":
				p.Courier.MemberID = 4
			case "partial":
				s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects = nil
			case "ambiguous":
				appendCollectionReadmission(&s, 1, 4)
			case "old hint":
				p.ReadmissionKey = s.Cleanup[0].ReadmissionKey
				want = fault.ErrOperationAuthorization
			case "missing hint":
				p.ReadmissionKey = &topology.AuthorityKey{ClusterID: "c", GroupID: topology.MetaGroupID, Revision: 29}
				want = fault.ErrOperationAuthorization
			}
			prepareScopedFixture(t, &s)
			before, _ := json.Marshal(s)
			result, err := s.applyRecordCollection(31, p)
			after, _ := json.Marshal(s)
			if !errors.Is(err, want) || result.changed || !bytes.Equal(before, after) {
				t.Fatal("invalid lineage changed collection", result, err, want)
			}
		})
	}
}

func TestScopedCollectionRetentionPreservesCurrentLineageForLostReplies(t *testing.T) {
	t.Parallel()
	for _, collected := range []bool{false, true} {
		t.Run(fmt.Sprint(collected), func(t *testing.T) {
			t.Parallel()
			s, p := collectionReadmissionFixture(t)
			if collected {
				if _, err := s.applyRecordCollection(31, p); err != nil {
					t.Fatal(err)
				}
			}
			appendCollectionReadmission(&s, 3, 4)
			p.Courier.MemberID = 4
			ageScopedHistory(t, &s)
			pruneScopedFixture(t, &s)
			if len(s.consumedJournalLinks("n")) != 3 || s.collectionCheckLocked(p.Receipt.Identity).Untracked {
				t.Fatal("selected debt lost journal lineage")
			}
			result, err := s.applyRecordCollection(s.AppliedIndex+1, p)
			if err != nil || result.changed == collected || !result.result.Collection.Receipt.Equal(p.Receipt) {
				t.Fatal("retained successor could not settle receipt", result, err)
			}
		})
	}
}

func TestScopedCollectionRetentionPreservesAmbiguousBranches(t *testing.T) {
	t.Parallel()
	s, p := collectionReadmissionFixture(t)
	appendCollectionReadmission(&s, 1, 4)
	for range 2 {
		slices.Reverse(s.AuthorityRecords)
		ageScopedHistory(t, &s)
		pruneScopedFixture(t, &s)
		if len(s.consumedJournalLinks("n")[1]) != 2 {
			t.Fatal("pruning erased ambiguity")
		}
		if _, err := s.applyRecordCollection(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("pruning or insertion order turned ambiguity into permission", err)
		}
	}
}
