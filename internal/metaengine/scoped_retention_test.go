//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

// Advance unrelated retained declarations beyond the ordinary history window;
// callers decide which checkpoints/debts still independently require old proof.
func ageScopedHistory(t *testing.T, s *scopedState) {
	t.Helper()
	s.History = append(s.History, s.Latest.Clone(), s.Target.Clone())
	slices.SortFunc(s.History, func(a, b topology.Document) int {
		if a.Version < b.Version {
			return -1
		}
		if a.Version > b.Version {
			return 1
		}
		return 0
	})
	s.History = slices.CompactFunc(s.History, func(a, b topology.Document) bool { return a.Version == b.Version })
	for range historyRetention + 8 {
		doc := s.Target.Clone()
		for _, floor := range s.Allocations {
			if group := doc.Group(floor.GroupID); group != nil {
				group.NextReplicaID = max(group.NextReplicaID, floor.NextReplicaID)
			}
		}
		doc.Version, doc.ParentVersion, doc.RebasedFrom = s.Latest.Version+1, s.Target.Version, 0
		s.History = append(s.History, doc)
		s.Latest, s.Target = doc, doc.Clone()
	}
	prepareScopedFixture(t, s)
}

func completedRetentionFixture(t *testing.T) scopedState {
	t.Helper()
	s := scopedCodecFixture()
	for i := range s.Groups {
		row := &s.Groups[i]
		if row.GroupID == "a" || row.GroupID == "b" {
			row.Declaration, row.SourceVersion, row.Deleting = s.Target.Group(row.GroupID), s.Target.Version, false
		}
	}
	s.Activated = append(s.Activated, "n")
	s.FullyConverged.Version = s.Target.Version
	ageScopedHistory(t, &s)
	s.FullyConverged.Version = s.Target.Version
	for i := range s.Groups {
		if s.Groups[i].GroupID == topology.MetaGroupID {
			s.Groups[i].SourceVersion, s.Groups[i].Declaration = s.Target.Version, s.Target.MetaGroup()
		}
	}
	prepareScopedFixture(t, &s)
	return s
}

func pruneScopedFixture(t *testing.T, s *scopedState) {
	t.Helper()
	if err := s.pruneRetainedProofs(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal("pruned snapshot:", err)
	}
	*s = restored
}

func TestScopedRetentionPrunesCompleteOrphansWithoutResurrection(t *testing.T) {
	t.Parallel()
	s := completedRetentionFixture(t)
	ids := append([]cleanupRecord(nil), s.Cleanup...)
	floors := append([]resourceFloor(nil), s.AuthorityFloors...)
	pruneScopedFixture(t, &s)
	if len(s.Cleanup) != 0 || len(s.Operations) != 0 || len(s.AuthorityRecords) != 0 || len(s.Admissions) != 0 {
		t.Fatalf("orphan proof retained: debt=%d operations=%d grants=%d admissions=%d", len(s.Cleanup), len(s.Operations), len(s.AuthorityRecords), len(s.Admissions))
	}
	if len(s.History) != historyRetention || !reflect.DeepEqual(floors, s.AuthorityFloors) {
		// Canonical restore sorts floors; compare by resource rather than insertion.
		for _, floor := range floors {
			if !slices.Contains(s.AuthorityFloors, floor) {
				t.Fatal("lost permanent floor")
			}
		}
		if len(s.History) != historyRetention {
			t.Fatal("unexpected retained history", len(s.History))
		}
	}
	for range 3 {
		collector := s.planningCollector()
		if _, err := collector.collect(s.planningDeclarations(), s.Target); err != nil {
			t.Fatal(err)
		}
		for _, debt := range ids {
			if s.identityReferencedLocked(debt.Identity) {
				t.Fatal("old identity resurrected", debt.Identity)
			}
			if collector.owners[planningReplicaKey{group: debt.Identity.GroupID, id: debt.Identity.ReplicaID}] != nil {
				t.Fatal("planner rediscovered collected owner", debt.Identity)
			}
		}
		pruneScopedFixture(t, &s)
	}
}

func TestScopedRetentionDebtPinsFailedAttemptsSiblingReceiptsAndDeclarations(t *testing.T) {
	t.Parallel()
	s := completedRetentionFixture(t)
	s.Cleanup[0].Collection = nil // One unresolved disk obligation pins the whole approval.
	prepareScopedFixture(t, &s)
	pruneScopedFixture(t, &s)
	if len(s.Cleanup) != 2 || s.indexes.operations["exec-v2-a1"] == nil || len(s.AuthorityRecords) != 5 {
		t.Fatal("debt lost its origin or unrelated required stop proof", len(s.Cleanup), len(s.AuthorityRecords))
	}
	if len(s.History) <= historyRetention || s.indexes.documents[1].Version != 1 || s.indexes.documents[2].Version != 2 {
		t.Fatal("soft cap erased required declaration evidence")
	}
	if !s.identityReferencedLocked(s.Cleanup[1].Identity) {
		t.Fatal("retained failed attempt or settled grant lost its identity reference")
	}
}

func TestScopedRetentionIsIndependentOfInsertionOrder(t *testing.T) {
	t.Parallel()
	var expected []byte
	for seed := uint64(1); seed <= 16; seed++ {
		s := completedRetentionFixture(t)
		s.Cleanup[0].Collection = nil
		rng := rand.New(rand.NewPCG(seed, seed+1))
		v := reflect.ValueOf(&s).Elem()
		for i := range v.NumField() {
			field := v.Field(i)
			if field.Kind() == reflect.Slice {
				rng.Shuffle(field.Len(), reflect.Swapper(field.Interface()))
			}
		}
		prepareScopedFixture(t, &s)
		pruneScopedFixture(t, &s)
		data, _ := json.Marshal(s)
		if expected != nil && !bytes.Equal(expected, data) {
			t.Fatal("pruning depended on insertion order", seed)
		}
		expected = data
	}
}

func TestScopedRetentionPinsRetrySuccessfulSiblingAndItsFence(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	if len(s.Operations) != 2 || len(s.AuthorityRecords) != 2 || len(s.Revocations) != 1 {
		t.Fatal("retry lost original successful sibling or transfer settlement")
	}
	catalog := s.authorityCatalog()
	for _, record := range s.AuthorityRecords {
		if !catalog.recordSettled(&record.authorityRecord) {
			t.Fatal("pruning unsettled retained authority")
		}
	}
}

func TestScopedRetentionSoftCapStillStagesAndApprovesUnrelatedWork(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	doc := s.Latest.Clone()
	doc.ParentVersion = s.Latest.Version
	doc.Node("n1").Labels = map[string]string{"owner": "jobs"}
	body, _ := json.Marshal(ProposeDocument{Doc: doc, ExpectedVersion: s.Latest.Version})
	result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindProposeDocument, body)
	if err != nil || !result.changed {
		t.Fatal("proof-pinned history rejected unrelated stage", err)
	}
	row := s.indexes.groups[topology.MetaGroupID]
	approval := BeginOperationProposal{Version: s.Latest.Version, At: s.Operations[0].UpdatedAt,
		Plan:   topology.Plan{Version: s.Latest.Version, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: row.SourceVersion}}},
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: row.SourceVersion, Owner: row.Owner}}}
	applyScopedApprovalFixture(t, &s, approval)
	if len(s.History) <= historyRetention || s.indexes.operations["exec-v2-a1"] == nil || s.indexes.operations["exec-v2-a2"] == nil {
		t.Fatal("unrelated approval discarded the live retry or imposed a hard history cap")
	}
	pruneScopedFixture(t, &s)
}
