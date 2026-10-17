//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCollectionAcceptsRetirementRevisionsAcrossFenceAndDeliveryOrder(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"old first", "new first"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			s := pendingScopedRetirement(t)
			debt := s.Cleanup[0].clone()
			issue := BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.RetireAction}
			old := *issueScopedCleanup(t, &s, issue).result.Grant
			fence := fenceCleanupFixture(t, &s, old)
			if _, err := s.applyRecordFence(s.AppliedIndex+1, RecordFenceProposal{Evidence: fence}); err != nil {
				t.Fatal(err)
			}
			newer := *issueScopedCleanup(t, &s, issue).result.Grant
			first, last := old, newer
			if order == "new first" {
				first, last = newer, old
			}
			at := debt.UpdatedAt.Add(time.Hour)
			recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(first)[0], At: at})
			// Select the other revision to exercise both valid alternatives, including
			// latest local selection when the delayed old receipt arrived first.
			p := RecordCollectionProposal{Receipt: authoritystore.CollectionReceipt{Identity: debt.Identity,
				MemberID: debt.MemberID, RetirementKey: last.Key, Token: "quarantine-token",
				LocalSequence: 1, CollectedAt: at}, Courier: fence.Receiver, At: at}
			before, _ := json.Marshal(s)
			if _, err := s.applyRecordCollection(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal("undelivered exact proof must remain retryable", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("pending retirement changed metadata")
			}
			recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(last)[0], At: at})
			status := RecordCleanupStatusProposal{Identity: debt.Identity, MemberID: debt.MemberID,
				RetirementKey: last.Key, Blocked: &topology.BlockedReason{Code: "retention"}, At: at}
			if _, err := s.applyRecordCleanupStatus(s.AppliedIndex+1, status); err != nil {
				t.Fatal("alternate exact revision could not report retention", err)
			}
			if _, err := s.applyRecordCollection(s.AppliedIndex+1, p); err != nil {
				t.Fatal("recorded alternate revision stranded collection", err)
			}
			pruneScopedFixture(t, &s) // Includes complete snapshot encode/restore validation.
			got := s.indexes.cleanup[debt.Identity]
			if got == nil || got.Retired.Key != first.Key || !got.Collection.Equal(p.Receipt) ||
				s.indexes.grants[last.Key] == nil {
				t.Fatal("pruning lost alternate retirement or rewrote the first milestone")
			}
			if result, err := s.applyRecordCollection(s.AppliedIndex+1, p); err != nil || result.changed {
				t.Fatal("lost reply could not retry after restore/pruning", result, err)
			}
			// Corrupt only the alternate proof: the original milestone remains valid.
			s.indexes.grants[last.Key].Effects = nil
			data, _ := json.Marshal(s)
			var restored scopedState
			if err := json.Unmarshal(data, &restored); err == nil {
				t.Fatal("snapshot restored collection without its cited effect")
			}
		})
	}
}

func TestScopedCollectionCannotSubstituteAnotherActionOrTarget(t *testing.T) {
	t.Parallel()
	s, p := collectionProposalFixture(t)
	for _, key := range []topology.AuthorityKey{s.Cleanup[0].Stopped.Key, s.Cleanup[1].Retired.Key} {
		p.Receipt.RetirementKey = key
		if _, err := s.applyRecordCollection(s.AppliedIndex+1, p); err == nil || errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("unrelated retained proof was not refused", err)
		}
	}
}

func TestScopedCollectionAcceptsReadmissionAfterCleanupRetirement(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"cleanup first", "readmission first"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			s := scopedReadmissionCleanupFixture()
			debt := &s.Cleanup[0]
			readmitKey := *debt.ReadmissionKey
			old := fixtureEffectReceipts(s.AuthorityRecords[0].Grant)[0]
			s.AuthorityRecords[0].Effects = []topology.EffectReceipt{old}
			if order == "cleanup first" {
				debt.Retired, debt.ReadmissionKey = &old, nil
			}
			prepareScopedFixture(t, &s)
			first := s.Cleanup[0].clone()
			at := first.UpdatedAt.Add(time.Hour)
			// A repeated consumed batch must preserve the first milestone, while
			// its own exact retirement receipt can bind the later host collection.
			if err := s.retireReadmissionTarget("exec-v3-a1", s.indexes.grants[readmitKey].Effects[0], at); err != nil {
				t.Fatal(err)
			}
			status := RecordCleanupStatusProposal{Identity: first.Identity, MemberID: first.MemberID,
				RetirementKey: readmitKey, At: at, Blocked: &topology.BlockedReason{Code: "retention"}}
			if _, err := s.applyRecordCleanupStatus(s.AppliedIndex+1, status); err != nil {
				t.Fatal("readmission-keyed retention report stranded", err)
			}
			p := RecordCollectionProposal{Receipt: authoritystore.CollectionReceipt{Identity: first.Identity,
				MemberID: first.MemberID, RetirementKey: readmitKey, Token: "retained-token", LocalSequence: 1, CollectedAt: at},
				Courier: topology.Identity{ClusterID: "c", NodeName: "n", MemberID: 2, InstanceID: "restart"}, At: at}
			if _, err := s.applyRecordCollection(s.AppliedIndex+1, p); err != nil {
				t.Fatal("completed exact readmission could not bind collection", err)
			}
			pruneScopedFixture(t, &s)
			got := s.indexes.cleanup[first.Identity]
			if got == nil || got.Retired.Key != first.Retired.Key || !got.Collection.Equal(p.Receipt) ||
				(first.ReadmissionKey == nil) != (got.ReadmissionKey == nil) || s.indexes.grants[readmitKey] == nil {
				t.Fatal("pruning lost collection proof or changed the first retirement")
			}
			if result, err := s.applyRecordCollection(s.AppliedIndex+1, p); err != nil || result.changed {
				t.Fatal("lost accepted reply failed after restore/pruning", result, err)
			}
		})
	}
}
