//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func collectionProposalFixture(t *testing.T) (scopedState, RecordCollectionProposal) {
	t.Helper()
	s := scopedCodecFixture()
	receipt := *s.Cleanup[0].Collection
	s.Cleanup[0].Collection = nil
	prepareScopedFixture(t, &s)
	return s, RecordCollectionProposal{Receipt: receipt, Courier: topology.Identity{ClusterID: "c", NodeName: "n",
		MemberID: 1, InstanceID: "retained-restart"}, At: s.Cleanup[0].UpdatedAt.Add(time.Minute), ProposalID: "collected"}
}

func TestScopedCollectionAcceptsExactReceiptAndReplaysWithoutMutation(t *testing.T) {
	t.Parallel()
	s, p := collectionProposalFixture(t)
	before := s.Cleanup[0].clone()
	body, _ := json.Marshal(p)
	result, err := applyScopedProposal(&s, 31, "record_collection", body)
	if err != nil || !result.changed || !result.result.Accepted || result.result.Collection == nil ||
		result.result.Collection.Settled || result.result.Collection.AppliedIndex != 31 ||
		!result.result.Collection.Receipt.Equal(p.Receipt) {
		t.Fatal("collection was not accepted exactly", result, err)
	}
	debt := s.indexes.cleanup[p.Receipt.Identity]
	if !debt.UpdatedAt.Equal(p.At) || !debt.Collection.Equal(p.Receipt) ||
		!debt.RetiredAt.Equal(*before.RetiredAt) || debt.Stopped.Key != before.Stopped.Key {
		t.Fatal("collection changed earlier proof or timestamps")
	}
	result.result.Collection.Receipt.Token = "caller mutation"
	data, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	// The receipt's timestamp denotes an instant; preserve the first spelling
	// in the echo even when a duplicate arrived with another timezone offset.
	p.Receipt.CollectedAt = p.Receipt.CollectedAt.In(time.FixedZone("host", 3600))
	p.At = p.At.Add(-time.Hour)
	result, err = restored.applyRecordCollection(32, p)
	after, _ := json.Marshal(restored)
	if err != nil || result.changed || !result.result.Accepted || !bytes.Equal(data, after) ||
		result.result.Collection.AppliedIndex != 32 || !result.result.Collection.Receipt.Equal(p.Receipt) {
		t.Fatal("exact duplicate changed retained evidence", result, err)
	}
}

func TestScopedCollectionRejectsConflictsAndForgedProofAtomically(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*RecordCollectionProposal){
		"token":                   func(p *RecordCollectionProposal) { p.Receipt.Token = "conflicting" },
		"sequence":                func(p *RecordCollectionProposal) { p.Receipt.LocalSequence++ },
		"host time":               func(p *RecordCollectionProposal) { p.Receipt.CollectedAt = p.Receipt.CollectedAt.Add(time.Second) },
		"retirement":              func(p *RecordCollectionProposal) { p.Receipt.RetirementKey.Revision++ },
		"member":                  func(p *RecordCollectionProposal) { p.Receipt.MemberID++ },
		"courier node":            func(p *RecordCollectionProposal) { p.Courier.NodeName = "foreign" },
		"courier cluster":         func(p *RecordCollectionProposal) { p.Courier.ClusterID = "foreign" },
		"courier process":         func(p *RecordCollectionProposal) { p.Courier.InstanceID = "" },
		"successor without grant": func(p *RecordCollectionProposal) { p.Courier.MemberID++ },
		"empty token":             func(p *RecordCollectionProposal) { p.Receipt.Token = " " },
		"zero sequence":           func(p *RecordCollectionProposal) { p.Receipt.LocalSequence = 0 },
		"zero time":               func(p *RecordCollectionProposal) { p.At = time.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := collectionProposalFixture(t)
			if _, err := s.applyRecordCollection(31, p); err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(s)
			mutate(&p)
			result, err := s.applyRecordCollection(32, p)
			after, _ := json.Marshal(s)
			if err == nil || result.changed || result.result.Accepted || !bytes.Equal(before, after) {
				t.Fatal("conflicting receipt or courier changed evidence", result, err)
			}
		})
	}
}

func TestScopedCollectionRefusesMemoryAndMissingEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"memory", "unknown", "tracked without debt", "pruned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := collectionProposalFixture(t)
			switch mode {
			case "memory":
				debt := s.Cleanup[1]
				p.Receipt.Identity, p.Receipt.RetirementKey = debt.Identity, debt.Retired.Key
			case "unknown":
				p.Receipt.Identity.ReplicaID = 999
			case "tracked without debt":
				p.Receipt.Identity.ReplicaID = 8
			case "pruned":
				s = completedRetentionFixture(t)
				pruneScopedFixture(t, &s)
			}
			before, _ := json.Marshal(s)
			check := s.collectionCheckLocked(p.Receipt.Identity)
			if check.Untracked != (mode == "unknown" || mode == "pruned") || check.AppliedIndex != s.AppliedIndex {
				t.Fatal("absence check fabricated historical acceptance", check)
			}
			result, err := s.applyRecordCollection(s.AppliedIndex+1, p)
			after, _ := json.Marshal(s)
			if err == nil || result.changed || result.result.Collection != nil || !bytes.Equal(before, after) {
				t.Fatal("missing obligation manufactured a collection fact", result, err)
			}
			if mode != "memory" && !errors.Is(err, fault.ErrHistoryUnavailable) {
				t.Fatal(err)
			}
		})
	}
}

func TestScopedCollectionUsesOnlyProposalAndHostClocks(t *testing.T) {
	t.Parallel()
	s, p := collectionProposalFixture(t)
	before, _ := json.Marshal(s)
	p.At = s.Cleanup[0].UpdatedAt.Add(-24 * time.Hour)
	p.Receipt.CollectedAt = p.At.Add(-24 * time.Hour)
	p.Receipt.LocalSequence = 1 // Independent of retirement effect sequence.
	var expected []byte
	for range 2 {
		var restored scopedState
		if err := json.Unmarshal(before, &restored); err != nil {
			t.Fatal(err)
		}
		if _, err := restored.applyRecordCollection(31, p); err != nil {
			t.Fatal(err)
		}
		debt := restored.indexes.cleanup[p.Receipt.Identity]
		if !debt.UpdatedAt.Equal(s.Cleanup[0].UpdatedAt) || !debt.Collection.Equal(p.Receipt) {
			t.Fatal("host and metadata clocks were conflated")
		}
		data, _ := json.Marshal(restored)
		if expected != nil && !bytes.Equal(data, expected) {
			t.Fatal("replicas used different clocks")
		}
		expected = data
	}
}

func TestScopedCollectionQueryIsReadOnlyAndRejectsForeignIdentity(t *testing.T) {
	t.Parallel()
	s, _ := collectionProposalFixture(t)
	id := storage.Identity{ClusterID: "c", GroupID: "unknown", NodeName: "n", ReplicaID: 99}
	before, _ := json.Marshal(s)
	q := QueryRequest{Kind: "collection_check", CollectionIdentity: &id}
	value, err := queryScopedCollection(&s, q)
	after, _ := json.Marshal(s)
	if err != nil || !value.Untracked || value.AppliedIndex != s.AppliedIndex || !bytes.Equal(before, after) {
		t.Fatal("absence query wrote evidence", value, err)
	}
	id.ClusterID = "foreign"
	if _, err := queryScopedCollection(&s, q); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	id.ClusterID, id.NodeName = "c", "\x00"
	if _, err := queryScopedCollection(&s, q); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	id.NodeName = "n"
	data, _ := json.Marshal(q)
	if _, err := NewMachine(testRules(), nil).Query(t.Context(), data); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("scoped settlement exposed on schema3", err)
	}
}

func TestScopedCollectionSettlementIncludesPreviousAndReadmissionReferences(t *testing.T) {
	t.Parallel()
	id := storage.Identity{ClusterID: "c", GroupID: "g", NodeName: "n", ReplicaID: 7}
	for _, reference := range []string{"previous", "settled readmission"} {
		t.Run(reference, func(t *testing.T) {
			t.Parallel()
			// Isolate each reference so a retained document cannot mask a missing
			// attempt or grant check in this read-only predicate.
			s := scopedState{SchemaVersion: scopedStateSchemaVersion, Target: topology.Document{ClusterID: "c"},
				AppliedIndex: 99, indexes: &scopedIndexes{cleanup: map[storage.Identity]*cleanupRecord{}}}
			if reference == "previous" {
				s.Operations = []topology.Operation{
					{Status: topology.OperationStatus{ID: "exec-v2-a1", State: topology.OperationFailed},
						Plan: topology.Plan{Steps: []topology.Step{{GroupID: "g", Kind: topology.StepStopReplica, NodeName: "n", ReplicaID: 7}}}},
					{Previous: "exec-v2-a1", Status: topology.OperationStatus{ID: "exec-v2-a2"}},
				}
			} else {
				s.AuthorityRecords = []scopedAuthorityRecord{{Class: "execution", authorityRecord: authorityRecord{Closed: true,
					Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "g", Revision: 9},
						Purpose: topology.AuthorityReadmit, Targets: []topology.AuthorityTarget{{NodeName: "n", MemberID: 3, ReplicaID: 7}}}}}}
			}
			before, _ := json.Marshal(s)
			value, err := queryScopedCollection(&s, QueryRequest{Kind: "collection_check", CollectionIdentity: &id})
			after, _ := json.Marshal(s)
			if err != nil || value.Untracked || value.AppliedIndex != 99 || !bytes.Equal(before, after) {
				t.Fatal("absence ignored retained proof or mutated it", value, err)
			}
		})
	}
}

func TestScopedCollectionRequiresRecordedRetirementBeforeFirstAcceptance(t *testing.T) {
	t.Parallel()
	s, p := collectionProposalFixture(t)
	s.Cleanup[0].Retired, s.Cleanup[0].RetiredAt = nil, nil
	s.AuthorityRecords[0].Effects = nil
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	result, err := s.applyRecordCollection(31, p)
	after, _ := json.Marshal(s)
	if !errors.Is(err, fault.ErrUnavailable) || result.changed || !bytes.Equal(before, after) {
		t.Fatal("collection substituted for retirement proof", result, err)
	}
}
