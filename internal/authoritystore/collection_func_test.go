//go:build functional

package authoritystore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func localCollection(replica uint64) CollectionRecord {
	grant := localGrant(10)
	grant.Action.Kind = topology.StepCleanupReplicaState
	grant.Action.ReplicaID, grant.Targets[0].ReplicaID = replica, replica
	id := storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
		NodeName: grant.Targets[0].NodeName, ReplicaID: replica}
	return CollectionRecord{Retired: storage.RetiredReplica{Identity: id, Token: fmt.Sprintf("token-%d", replica),
		QuarantinedAt: time.Unix(10, 0)}, MemberID: 1, Grant: grant,
		Retirement: receiptFor(grant, 9), State: collectionPrepared}
}

func prepareLocalCollection(t *testing.T, s *Store, replica uint64) CollectionRecord {
	t.Helper()
	record, err := s.PrepareCollection(t.Context(), localCollection(replica))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestCollectionJournalReopensBothStatesAndPreservesFirstReceipt(t *testing.T) {
	t.Parallel()
	for _, state := range []string{collectionPrepared, collectionCollected} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s, path := openLocal(t)
			input := localCollection(7)
			prepared := prepareLocalCollection(t, s, 7)
			if prepared.Sequence != 1 || input.Sequence != 0 {
				t.Fatal("sequence not assigned exactly once", prepared)
			}
			at := time.Unix(5, 0) // Host clock need not follow quarantine/effect time.
			if state == collectionCollected {
				if _, err := s.CompleteCollection(t.Context(), input.Retired.Identity, prepared.Sequence, at); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			identity := localIdentity()
			identity.InstanceID = "retained-restart"
			reopened, err := Open(path, identity, RetainedEvidence)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			row, err := reopened.PrepareCollection(t.Context(), input)
			if err != nil || row.Sequence != 1 || row.State != state || !sameCollectionProof(row, prepared) {
				t.Fatal("reopen/duplicate preparation lost evidence", row, err)
			}
			if state == collectionCollected {
				got, err := reopened.CompleteCollection(t.Context(), input.Retired.Identity, 1, at.Add(time.Hour))
				if err != nil || !got.Equal(collectionReceipt(row)) || !got.CollectedAt.Equal(at) {
					t.Fatal("duplicate completion replaced first time", got, err)
				}
			}
			row.Grant.Action.StepSeq++
			row.Retirement.Action.StepSeq++
			page, err := reopened.Collections(t.Context(), "", 0)
			if err != nil || len(page.Records) != 1 || !sameCollectionProof(page.Records[0], input) {
				t.Fatal("returned values alias journal", page, err)
			}
		})
	}
}

func TestCollectionJournalRefusesConflictingPreparation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"token", "quarantine", "grant", "retirement", "member", "sequence", "collected", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, _ := openLocal(t)
			original := prepareLocalCollection(t, s, 7)
			bad := original.Clone()
			switch mode {
			case "token":
				bad.Retired.Token += "-other"
			case "quarantine":
				bad.Retired.QuarantinedAt = bad.Retired.QuarantinedAt.Add(time.Second)
			case "grant":
				bad.Grant.Key.Revision++
				bad.Retirement.Key.Revision++
			case "retirement":
				bad.Retirement.LocalSequence++
			case "member":
				bad.MemberID++
			case "sequence":
				bad.Sequence++
			case "collected":
				bad.State = collectionCollected
				at := time.Unix(20, 0)
				bad.CollectedAt = &at
			case "foreign":
				bad.Retired.Identity.ClusterID = "foreign"
			}
			if _, err := s.PrepareCollection(t.Context(), bad); err == nil {
				t.Fatal("conflicting preparation accepted")
			}
			page, err := s.Collections(t.Context(), "", 1)
			if err != nil || !reflect.DeepEqual(page.Records, []CollectionRecord{original}) {
				t.Fatal("conflict mutated evidence", page, err)
			}
			if next := prepareLocalCollection(t, s, 8); next.Sequence != 2 {
				t.Fatal("conflict consumed a sequence", next.Sequence)
			}
		})
	}
}

func TestCollectionJournalRemovalRequiresExactCollectedRow(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	row := prepareLocalCollection(t, s, 7)
	at := time.Unix(20, 0)
	pretend := row.Clone()
	pretend.State, pretend.CollectedAt = collectionCollected, &at
	if err := s.RemoveCollection(t.Context(), collectionReceipt(pretend)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("prepared proof was released", err)
	}
	if _, err := s.CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence+1, at); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrong sequence completed collection", err)
	}
	receipt, err := s.CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"token", "sequence", "time", "grant", "member"} {
		bad := receipt
		switch field {
		case "token":
			bad.Token += "-other"
		case "sequence":
			bad.LocalSequence++
		case "time":
			bad.CollectedAt = bad.CollectedAt.Add(time.Second)
		case "grant":
			bad.RetirementKey.Revision++
		case "member":
			bad.MemberID++
		}
		if err := s.RemoveCollection(t.Context(), bad); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("inexact receipt released proof", field, err)
		}
	}
	for range 2 {
		if err := s.RemoveCollection(t.Context(), receipt); err != nil {
			t.Fatal("exact release not repeatable", err)
		}
	}
	if _, err := s.CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence, at); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("missing row manufactured completion", err)
	}
	if next := prepareLocalCollection(t, s, 8); next.Sequence != 2 {
		t.Fatal("removal reset monotonic sequence", next.Sequence)
	}
}

func TestCollectionJournalRebindPreservesHistoricalRowsAndSerializesAccess(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	prepareLocalCollection(t, s, 7)
	prepared := prepareLocalCollection(t, s, 8)
	if _, err := s.CompleteCollection(t.Context(), prepared.Retired.Identity, prepared.Sequence, time.Unix(20, 0)); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Collections(t.Context(), "", 0)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 20 {
				if _, err := s.Collections(t.Context(), "", 1); err != nil {
					t.Error(err)
				}
				if _, err := s.PrepareCollection(t.Context(), localCollection(7)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	previous := localIdentity()
	for member := uint64(2); member <= 3; member++ {
		next := previous
		next.MemberID, next.InstanceID = member, fmt.Sprint(member)
		if err := s.Rebind(t.Context(), previous, next, fmt.Sprint("admit-", member)); err != nil {
			t.Fatal(err)
		}
		previous = next
	}
	workers.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, previous, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Collections(t.Context(), "", 0)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rebind changed historical collection evidence", after, err)
	}
}

func TestCollectionJournalConcurrentPreparationAndCompletionShareOneFact(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Go(func() {
			row, err := s.PrepareCollection(t.Context(), localCollection(7))
			if err != nil || row.Sequence != 1 {
				t.Error(row, err)
				return
			}
			receipt, err := s.CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence, time.Unix(int64(worker+1), 0))
			if err != nil || receipt.LocalSequence != 1 {
				t.Error(receipt, err)
			}
		})
	}
	workers.Wait()
	page, err := s.Collections(t.Context(), "", 0)
	if err != nil || len(page.Records) != 1 || page.Records[0].State != collectionCollected {
		t.Fatal(page, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.PrepareCollection(ctx, localCollection(8)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collections(t.Context(), "", 0); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
