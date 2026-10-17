//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func recordScopedEffectFixture(t *testing.T, s *scopedState, p RecordEffectProposal) proposalApplication {
	t.Helper()
	p.ProposalID = "record-fixture"
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyScopedProposal(s, s.AppliedIndex+1, KindRecordEffect, body)
	if err != nil || !result.result.Accepted {
		t.Fatal("receipt refused", result, err)
	}
	prepareScopedFixture(t, s)
	return result
}

func TestScopedReceiptCreatesRemovalDebtAtomicallyAndPreservesReplay(t *testing.T) {
	t.Parallel()
	s := pendingReplacementCleanup()
	debt := s.Cleanup[0].clone()
	s.Cleanup = s.Cleanup[1:]
	s.Operations[2].Status.Blocks[0].CurrentStep = 1
	for i := range s.AuthorityRecords {
		if s.AuthorityRecords[i].Grant.Key == debt.Removed.Key {
			s.AuthorityRecords[i].Effects = nil
		}
	}
	prepareScopedFixture(t, &s)
	p := RecordEffectProposal{Receipt: *debt.Removed, At: debt.CreatedAt.Add(-time.Hour)}
	result := recordScopedEffectFixture(t, &s, p)
	got := s.indexes.cleanup[debt.Identity]
	if !result.changed || got == nil || !reflect.DeepEqual(got.Removed, debt.Removed) ||
		got.MemberID != debt.MemberID || got.Storage != debt.Storage || got.OperationID != debt.OperationID ||
		got.Stopped != nil || got.Retired != nil || got.RemovedAt.Before(got.CreatedAt) {
		t.Fatal("native receipt did not atomically retain original exact cleanup", got)
	}
	before, _ := json.Marshal(s)
	p.At = p.At.Add(48 * time.Hour)
	result = recordScopedEffectFixture(t, &s, p)
	after, _ := json.Marshal(s)
	if result.changed || !bytes.Equal(before, after) {
		t.Fatal("duplicate removal changed state")
	}
	p.Receipt.AppliedIndex++
	result, err := s.applyRecordEffect(s.AppliedIndex+1, p)
	after, _ = json.Marshal(s)
	if err != nil || result.result.Accepted || !bytes.Equal(before, after) {
		t.Fatal("conflicting receipt changed authority/debt", result, err)
	}
}

func TestScopedReadmissionLastReceiptCompletesWaitingDeletion(t *testing.T) {
	t.Parallel()
	s, approval := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, approval)
	var receipts []topology.EffectReceipt
	for _, record := range s.AuthorityRecords {
		if record.Grant.Purpose == topology.AuthorityReadmit {
			receipts = append(receipts, fixtureEffectReceipts(record.Grant)...)
		}
	}
	if len(receipts) != 2 {
		t.Fatal("fixture must have independent meta/load targets")
	}
	for i, receipt := range receipts {
		p := RecordReadmissionReceiptsProposal{Admission: s.Admissions[0], Receipts: []topology.EffectReceipt{receipt},
			At: approval.At.Add(-time.Hour), ProposalID: "batch"}
		body, _ := json.Marshal(p)
		result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindRecordReadmissionReceipts, body)
		if err != nil || !result.result.Accepted || !result.changed {
			t.Fatal("partial/last receipt refused", i, result, err)
		}
		prepareScopedFixture(t, &s)
		if i == 0 && (!s.indexes.groups["shard0"].Deleting || len(s.Cleanup) != 0) {
			t.Fatal("partial batch manufactured cleanup or deletion")
		}
	}
	row := s.indexes.groups["shard0"]
	op := s.indexes.operations[topology.MakeOperationID(3, 1)]
	if row.Deleting || row.Owner != "" || row.SourceVersion != 3 || row.Declaration != nil ||
		row.CompletedAt.Before(approval.At) || op.Status.State != topology.OperationSucceeded || len(s.Cleanup) != 2 {
		t.Fatal("last receipt failed to atomically complete exact deletion", row, op, s.Cleanup)
	}
	for _, debt := range s.Cleanup {
		if debt.StopAction != nil || debt.RetireAction != nil || debt.Removed != nil ||
			debt.ReadmissionKey == nil || debt.Stopped == nil || debt.Retired == nil || debt.MemberID != 1 {
			t.Fatal("readmission debt invented ordinary actions/removal", debt)
		}
	}
	before, _ := json.Marshal(s)
	// Native removal after whole-batch retirement remains execution evidence;
	// its debt already belongs to the exact readmission origin.
	for _, debt := range s.Cleanup {
		action := topology.AuthorizedStep{OperationID: "exec-v2-a1", Version: 2, Attempt: 1,
			StepSeq: 6, Kind: topology.StepRemoveReplica, GroupID: debt.Identity.GroupID,
			NodeName: debt.Identity.NodeName, ReplicaID: debt.Identity.ReplicaID}
		receipt := *debt.Retired
		receipt.Action = &action
		receipt.Resource.Kind = topology.AuthorityGroup
		if err := s.recordCleanupEffect(receipt, approval.At.Add(time.Hour)); err != nil {
			t.Fatal("later native removal replaced actionless debt", err)
		}
	}
	for _, receipt := range receipts {
		if recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: approval.At.Add(time.Hour)}).changed {
			t.Fatal("duplicate batch effect changed state")
		}
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("duplicate batch moved completion or cleanup timestamps")
	}
}

func TestScopedLateStopCompletesReboundDeletionWithoutDeadHost(t *testing.T) {
	t.Parallel()
	s := retryScopedDeletion()
	old := scopedCodecFixture()
	debt := s.Cleanup[1]
	var grant topology.AuthorityGrant
	for _, record := range old.AuthorityRecords {
		if record.Grant.Key == old.Cleanup[1].Stopped.Key {
			grant = record.Grant.Clone()
		}
	}
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Closed: true}})
	later := grant.Clone()
	later.Key.Revision, later.Action, later.OperationID = 31, debt.StopAction, debt.StopAction.OperationID
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: later}})
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: later.Resources[0], Key: later.Key})
	s.AppliedIndex = 31
	prepareScopedFixture(t, &s)
	op := s.Operations[len(s.Operations)-1]
	running := scopedTransitionFor(op, "b", debt.StopAction.StepSeq)
	applyScopedTransitionFixture(t, &s, running)
	stamp := running.At.Add(time.Second)
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: *old.Cleanup[1].Stopped, At: stamp})
	transition := running
	transition.State, transition.At = topology.OperationSucceeded, stamp
	applyScopedTransitionFixture(t, &s, transition)
	got := s.indexes.cleanup[debt.Identity]
	if s.indexes.groups["b"].Deleting || s.indexes.groups["b"].Owner != "" ||
		got.Stopped.Action.OperationID != grant.OperationID || got.StopAction.OperationID != later.OperationID ||
		s.indexes.grants[later.Key].Effects != nil {
		t.Fatal("late exact stop was lost or relabeled", got)
	}
	// The sibling failed block is preserved; deletion succeeds independently.
	if !slices.ContainsFunc(s.Operations[len(s.Operations)-1].Status.Blocks, func(b topology.BlockStatus) bool {
		return b.GroupID == "a" && b.State == topology.OperationFailed
	}) {
		t.Fatal("completion changed the independent failed block")
	}
	// Memory retirement ends the debt without a durable collection token/time.
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(later)[0], At: stamp})
	retire := later.Clone()
	retire.Key.Revision, retire.Action, retire.OperationID = s.AppliedIndex+1, got.RetireAction, got.RetireAction.OperationID
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "cleanup",
		authorityRecord: authorityRecord{Grant: retire}})
	s.AuthorityFloors[len(s.AuthorityFloors)-1].Key = retire.Key
	s.AppliedIndex = retire.Key.Revision
	prepareScopedFixture(t, &s)
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(retire)[0], At: stamp})
	got = s.indexes.cleanup[debt.Identity]
	if got.Storage != topology.StorageMemory || got.Retired == nil || got.Collection != nil ||
		got.Stopped.Action.OperationID != grant.OperationID {
		t.Fatal("memory retirement invented collection or replaced its stop proof", got)
	}
}

func TestScopedReadmissionReceiptBatchRejectsPartialMutation(t *testing.T) {
	t.Parallel()
	s, _ := readmissionDeletionFixture(t)
	var receipts []topology.EffectReceipt
	for _, record := range s.AuthorityRecords {
		receipts = append(receipts, fixtureEffectReceipts(record.Grant)...)
	}
	receipts[len(receipts)-1].Target.MemberID++
	before, _ := json.Marshal(s)
	p := RecordReadmissionReceiptsProposal{Admission: s.Admissions[0], Receipts: receipts,
		At: s.Operations[0].UpdatedAt, ProposalID: "atomic-batch"}
	result, err := s.applyRecordReadmissionReceipts(s.AppliedIndex+1, p)
	after, _ := json.Marshal(s)
	if err != nil || result.result.Accepted || result.changed || !bytes.Equal(before, after) {
		t.Fatal("later invalid receipt left first target consumed", result, err)
	}
}

func TestScopedCompletionRecordsFinalPromotionAndAdmission(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	s.Latest = s.Target.Clone()
	op := &s.Operations[0]
	op.Plan.Steps = append(op.Plan.Steps,
		topology.Step{Seq: 3, GroupID: topology.MetaGroupID, Kind: topology.StepWaitCaughtUp, NodeName: "n2", ReplicaID: 2},
		topology.Step{Seq: 4, GroupID: topology.MetaGroupID, Kind: topology.StepPromoteVoter, NodeName: "n2", ReplicaID: 2,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})})
	op.Plan.Blocks[0].Steps = []int{1, 2, 3, 4}
	op.Status.State, op.Status.Blocks[0].State = topology.OperationRunning, topology.OperationRunning
	op.Status.Blocks[0].CurrentStep, op.Status.Blocks[0].TotalSteps = 4, 4
	s.Admissions[0].State = AdmissionLearner
	for _, seq := range []int{1, 2, 4} {
		admissionPrefixReceipt(&s, 0, seq)
	}
	prepareScopedFixture(t, &s)
	transition := scopedTransitionFor(s.Operations[0], topology.MetaGroupID, 4)
	transition.State = topology.OperationSucceeded
	applyScopedTransitionFixture(t, &s, transition)
	if s.Admissions[0].State != AdmissionCompleted || len(s.VoterSwitches) != 1 ||
		s.indexes.groups[topology.MetaGroupID].SourceVersion != 2 {
		t.Fatal("last promotion lost cooldown, admission or convergence", s.Admissions, s.VoterSwitches)
	}
}

func TestScopedReceiptDeletionScopeIncludesWholeExactBatch(t *testing.T) {
	t.Parallel()
	s, approval := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, approval)
	for _, record := range s.AuthorityRecords {
		if record.Grant.Purpose != topology.AuthorityReadmit {
			continue
		}
		groups := s.receiptDeletionGroups(fixtureEffectReceipts(record.Grant))
		if !slices.Equal(groups, []string{topology.MetaGroupID, "shard0"}) {
			t.Fatal("one batch receipt omitted its other deletion scope", groups)
		}
	}
	ordinary := topology.EffectReceipt{Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: "independent", Revision: 99}}
	if groups := s.receiptDeletionGroups([]topology.EffectReceipt{ordinary}); !slices.Equal(groups, []string{"independent"}) {
		t.Fatal("unrelated receipt selected global deletion work", groups)
	}
	before, _ := json.Marshal(s)
	s.completeWaitingDeletions(approval.At, s.receiptDeletionGroups([]topology.EffectReceipt{ordinary}))
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("unrelated receipt changed waiting deletion state")
	}
}

func TestScopedDeletionCompletionFailureIsStableDiagnostic(t *testing.T) {
	t.Parallel()
	s, approval := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, approval)
	op := s.indexes.operations["exec-v3-a1"]
	transition := scopedTransitionFor(*op, "shard0", 0)
	transition.At = op.UpdatedAt.Add(time.Second)
	beforeEffects := slices.Clone(s.AuthorityRecords)
	s.blockDeletionCompletion(transition, fault.ErrIdentityMismatch)
	if op.Status.State != topology.OperationWaitingForReplica || op.Status.Blocks[0].Blocked.Code != "cleanup_failed" ||
		!reflect.DeepEqual(beforeEffects, s.AuthorityRecords) {
		t.Fatal("completion failure discarded receipt facts or lost diagnostic")
	}
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	transition.At = transition.At.Add(time.Hour)
	s.blockDeletionCompletion(transition, fault.ErrIdentityMismatch)
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("same completion failure rewrote its durable clock")
	}
}
