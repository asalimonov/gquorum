//go:build !functional

package metaengine

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestDeletionUnstoppedDebtSurvivesPruningAndRestore(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	p = planDetachedApproval(t, &s, p)
	applyScopedApprovalFixture(t, &s, p)
	debt := s.Cleanup[0].clone()
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	got := s.indexes.cleanup[debt.Identity]
	if got == nil || !reflect.DeepEqual(*got, debt) || s.indexes.groups["shard0"].Deleting {
		t.Fatal("logical completion discarded or rewrote pending cleanup", got)
	}
	if s.indexes.operations[debt.OperationID] == nil || s.indexes.documents[1].Version != 1 || s.indexes.documents[p.Version].Version != p.Version {
		t.Fatal("pruning lost immutable origin or former declaration")
	}
	if len(s.History) >= int(s.Target.Version) {
		t.Fatal("cleanup pinned every unrelated document")
	}
	m := restoreMachineFixture(t, s)
	status, err := m.Operation(debt.OperationID)
	if err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	work, err := m.ReadyCleanup(storage.Identity{}, 1)
	if err != nil || len(work) != 1 || work[0].Action != *debt.StopAction {
		t.Fatal(work, err)
	}
	page, err := m.Progress(topology.ProgressQuery{Limit: 1})
	if err != nil || page.Next == "" {
		t.Fatal("pending debt lost bounded progress", page, err)
	}
}

func TestDeletionReadmissionSettlesOnlyAfterWholeBatch(t *testing.T) {
	t.Parallel()
	s, p := readmissionDeletionFixture(t)
	p = planDetachedApproval(t, &s, p)
	applyScopedApprovalFixture(t, &s, p)
	original := s.Cleanup[0].clone()
	var receipts []topology.EffectReceipt
	for _, r := range s.AuthorityRecords {
		if r.Grant.Purpose == topology.AuthorityReadmit {
			receipts = append(receipts, fixtureEffectReceipts(r.Grant)...)
		}
	}
	slices.SortFunc(receipts, func(a, b topology.EffectReceipt) int {
		if a.Key.GroupID == "shard0" {
			return -1
		}
		if b.Key.GroupID == "shard0" {
			return 1
		}
		return 0
	})
	for i, receipt := range receipts {
		at := p.At.Add(time.Duration(i+1) * time.Second)
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: at})
		debt := s.indexes.cleanup[original.Identity]
		if i < len(receipts)-1 && debt.Stopped != nil {
			t.Fatal("partial batch fabricated stop", debt)
		}
	}
	debt := s.indexes.cleanup[original.Identity].clone()
	if debt.Stopped == nil || debt.Retired == nil || debt.Collection != nil || debt.ReadmissionKey == nil || debt.MemberID != original.MemberID {
		t.Fatal("whole batch lost exact old identity or manufactured collection", debt)
	}
	for _, receipt := range receipts {
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: p.At.Add(time.Hour)})
	}
	if !reflect.DeepEqual(debt, *s.indexes.cleanup[original.Identity]) {
		t.Fatal("late duplicate rewrote first retirement")
	}
	encoded, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
}

func TestDeletionWithInheritedDebtSurvivesMissingOrClosedReadmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "closed", "partial"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := existingDebtDeletionFixture(t)
			group := "a"
			if mode == "missing" {
				s, p = readmissionDeletionFixture(t)
				group = "shard0"
				s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool { return r.Grant.Purpose == topology.AuthorityReadmit })
				s.AuthorityFloors = slices.DeleteFunc(s.AuthorityFloors, func(f resourceFloor) bool { return f.Key.Revision == 5 })
			} else {
				for i := range s.AuthorityRecords {
					r := &s.AuthorityRecords[i]
					if r.Grant.Purpose == topology.AuthorityReadmit {
						r.Closed = mode == "closed"
						if mode == "partial" && r.Grant.Key.GroupID == "a" {
							r.Effects = fixtureEffectReceipts(r.Grant)
						}
					}
				}
			}
			prepareScopedFixture(t, &s)
			p = planDetachedApproval(t, &s, p)
			applyScopedApprovalFixture(t, &s, p)
			if s.indexes.groups[group].Deleting || s.Cleanup[0].Stopped != nil {
				t.Fatal("logical approval waited or invented stop", mode)
			}
		})
	}
}

func TestDeletionRetryPreservesSuccessfulSiblingAndOriginalCleanup(t *testing.T) {
	t.Parallel()
	s, p := mixedDeletionApprovalFixture(t)
	applyScopedApprovalFixture(t, &s, p)
	op := s.Operations[len(s.Operations)-1].Clone()
	debt := s.Cleanup[0].clone()
	transition := scopedTransitionFor(op, topology.MetaGroupID, 1)
	transition.State, transition.Error = topology.OperationFailed, "unreachable"
	applyScopedTransitionFixture(t, &s, transition)
	prior := s.indexes.operations[op.Status.ID]
	result := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, prior))
	retry := s.indexes.operations[result.result.OperationID]
	if retry.Status.Blocks[1].State != topology.OperationSucceeded || s.indexes.groups["shard0"].Owner != "" ||
		!reflect.DeepEqual(debt, *s.indexes.cleanup[debt.Identity]) || !retry.Plan.Equal(op.Plan) {
		t.Fatal("retry reclaimed successful deletion or its exact cleanup", retry)
	}
}

func TestDeletionTerminalEffectsRetainFirstProofAndRefuseConflict(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	p = planDetachedApproval(t, &s, p)
	applyScopedApprovalFixture(t, &s, p)
	id := s.Cleanup[0].Identity
	for _, kind := range []topology.StepKind{topology.StepStopReplica, topology.StepDeleteGroupState} {
		work, err := queryReadyCleanup(&s, storage.Identity{}, 1)
		if err != nil || len(work) != 1 || work[0].Action.Kind != kind {
			t.Fatal(work, err)
		}
		issued := issueScopedCleanup(t, &s, BeginCleanupEffectProposal{Identity: id, Action: work[0].Action})
		receipt := fixtureEffectReceipts(*issued.result.Grant)[0]
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: p.At})
		first := s.indexes.cleanup[id].clone()
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: p.At.Add(time.Hour)})
		if !reflect.DeepEqual(first, *s.indexes.cleanup[id]) {
			t.Fatal("repeated receipt replaced first timestamp")
		}
		before, _ := json.Marshal(s)
		receipt.Target.ReplicaID++
		body, _ := json.Marshal(RecordEffectProposal{Receipt: receipt, At: p.At.Add(time.Hour), ProposalID: "conflict"})
		result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindRecordEffect, body)
		if err == nil && result.result.Accepted {
			t.Fatal("conflicting receipt accepted")
		}
		after, _ := json.Marshal(s)
		if string(before) != string(after) {
			t.Fatal("conflicting receipt changed retained evidence")
		}
	}
	if s.indexes.cleanup[id].Collection != nil {
		t.Fatal("retirement invented collection")
	}
}
