//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCompletionPreparesPinnedDeclarationBesideUnrelatedFence(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, topology.MetaGroupID)
	s.Operations[1].Status.State, s.Operations[1].Status.Blocks[1].State = topology.OperationRunning, topology.OperationRunning
	key := scopedPendingTransfer(&s)
	s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects = fixtureEffectReceipts(s.AuthorityRecords[len(s.AuthorityRecords)-1].Grant)
	s.AuthorityRecords[1].Fences, s.Revocations[0].Obligations[0].Evidence = nil, nil
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, approval)
	transition := scopedTransitionFor(s.Operations[1], "shard0", 2)
	transition.State = topology.OperationSucceeded
	before, _ := json.Marshal(s)
	next, err := s.prepareGroupCompletion(transition)
	pinned := s.indexes.documents[2]
	if err != nil || next.Groups[1].SourceVersion != 2 || next.Groups[1].Owner != "" ||
		!reflect.DeepEqual(next.Groups[1].Declaration, pinned.Group("shard0")) {
		t.Fatal("completion used current Target or waited for another group", next, err)
	}
	next.Groups[1].Declaration.Labels = map[string]string{"owned": "copy"}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) || s.indexes.grants[key].Effects == nil {
		t.Fatal("preparation changed source progress or consumption")
	}
	retained := s.indexes.grants[key].Effects
	s.indexes.grants[key].Effects = nil
	if _, err := s.prepareGroupCompletion(transition); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("missing exact final receipt accepted", err)
	}
	s.indexes.grants[key].Effects = retained
	applyScopedTransitionFixture(t, &s, transition)
	if s.indexes.groups["shard0"].SourceVersion != 2 || s.indexes.groups["shard0"].Owner != "" {
		t.Fatal("complete installation lost pinned convergence")
	}
	before, _ = json.Marshal(s)
	result, err := s.applyTransitionOperation(s.AppliedIndex+1, TransitionOperationProposal{Transition: transition})
	after, _ = json.Marshal(s)
	if err != nil || result.changed || !bytes.Equal(before, after) {
		t.Fatal("duplicate completion changed proof or clocks", err)
	}
}

func TestScopedCompletionRechecksLoadAdmissionAfterConsumption(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[2])
	issued, err := s.applyBeginEffect(11, BeginEffectProposal{Action: action})
	if err != nil {
		t.Fatal(err)
	}
	r := s.indexes.grants[issued.result.Grant.Key]
	r.Effects = fixtureEffectReceipts(r.Grant)
	s.Admissions[0].State = AdmissionPending
	prepareScopedFixture(t, &s)
	transition := scopedTransitionFor(s.Operations[0], "shard0", 3)
	transition.State = topology.OperationSucceeded
	before, _ := json.Marshal(s)
	if _, err := s.prepareGroupCompletion(transition); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("consumed load effect bypassed missing current admission prerequisite", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("late prerequisite refusal published convergence")
	}
}

func TestScopedZeroStepDeletionRetainsReasonAndRechecksWholeBatch(t *testing.T) {
	t.Parallel()
	s, approval := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, approval)
	operation := s.Operations[len(s.Operations)-1]
	block := operation.Status.Blocks[0]
	if block.State != topology.OperationWaitingForReplica || block.Blocked == nil ||
		block.Blocked.Code != "readmission_retirement" {
		t.Fatal("pending zero-step deletion lacks an exact diagnostic", block)
	}
	transition := scopedTransitionFor(operation, "shard0", 0)
	transition.State = topology.OperationSucceeded
	for i := range s.AuthorityRecords {
		if s.AuthorityRecords[i].Grant.Key.GroupID == "shard0" {
			s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
		}
	}
	prepareScopedFixture(t, &s)
	if _, err := s.prepareGroupCompletion(transition); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("partial readmission batch proved whole deletion", err)
	}
	for i := range s.AuthorityRecords {
		s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
	}
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	next, err := s.prepareGroupCompletion(transition)
	if err != nil || next.Groups[1].SourceVersion != 3 || next.Groups[1].Declaration != nil ||
		next.Groups[1].Deleting || len(next.Cleanup) != 1 || next.Cleanup[0].ReadmissionKey == nil ||
		next.Cleanup[0].OperationID != "exec-v2-a1" {
		t.Fatal("complete readmission lost tombstone or original debt", next, err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("deletion preparation mutated live state")
	}
}

func TestScopedDeletionCompletionUsesBoundStopAndIgnoresDetachedRetirement(t *testing.T) {
	t.Parallel()
	s := retryScopedDeletion()
	op := &s.Operations[len(s.Operations)-1]
	debt := &s.Cleanup[1]
	action := *debt.StopAction
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "b", NodeName: "n"}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "b", Revision: 31},
		Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action,
		Resources: []topology.AuthorityResource{host},
		Targets:   []topology.AuthorityTarget{{NodeName: "n", MemberID: 1, ReplicaID: 8}}}
	receipt := fixtureEffectReceipts(grant)[0]
	debt.Stopped, debt.StoppedAt = &receipt, &op.UpdatedAt
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: []topology.EffectReceipt{receipt}}})
	retire := grant.Clone()
	retire.Key.Revision, retire.Action, retire.OperatorID = 32, debt.RetireAction, debt.RetireAction.OperatorID
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "cleanup",
		authorityRecord: authorityRecord{Grant: retire}})
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: host, Key: retire.Key})
	s.AppliedIndex = 32
	prepareScopedFixture(t, &s)
	transition := scopedTransitionFor(*op, "b", action.StepSeq)
	transition.State = topology.OperationSucceeded
	next, err := s.prepareGroupCompletion(transition)
	if err != nil || next.Cleanup[1].Stopped.Action.OperatorID != action.OperatorID ||
		next.Cleanup[1].OperationID != "exec-v2-a1" || next.Cleanup[1].Retired != nil {
		t.Fatal("detached retirement blocked exact required-stop completion", next, err)
	}
}

func TestScopedDeletionStopsDoNotRequireDeadNativeGroupFence(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, "shard0")
	scopedPendingTransfer(&s)
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	approval.Plan.Steps = []topology.Step{
		{Seq: 1, GroupID: "shard0", NodeName: "n1", ReplicaID: 1, Kind: topology.StepStopReplica},
		{Seq: 2, GroupID: "shard0", Kind: topology.StepDeleteGroupState},
	}
	approval.Plan.Blocks[0].Steps, approval.Plan.Blocks[0].Cleanup = []int{1}, []int{2}
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, approval)
	op := s.Operations[len(s.Operations)-1]
	running := scopedTransitionFor(op, "shard0", 1)
	applyScopedTransitionFixture(t, &s, running)
	action := scopedPlanAction(&op, op.Plan.Steps[0])
	issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action})
	if err != nil {
		t.Fatal(err)
	}
	r := s.indexes.grants[issued.result.Grant.Key]
	r.Effects = fixtureEffectReceipts(r.Grant)
	at := running.At
	s.Cleanup[0].Stopped, s.Cleanup[0].StoppedAt, s.Cleanup[0].UpdatedAt = &r.Effects[0], &at, at
	prepareScopedFixture(t, &s)
	native := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"}
	if len(s.indexes.obligations[native]) == 0 {
		t.Fatal("fixture lost its unreachable native fence")
	}
	completion := scopedTransitionFor(s.Operations[len(s.Operations)-1], "shard0", 1)
	completion.State = topology.OperationSucceeded
	if _, err := s.prepareGroupCompletion(completion); err != nil {
		t.Fatal("all exact hosts stopped but dead group's native fence blocked deletion", err)
	}
	applyScopedTransitionFixture(t, &s, completion)
	if len(s.indexes.obligations[native]) != 0 || !s.authorityCatalog().deletedResource(native) {
		t.Fatal("completed permanent deletion retained unreachable native obligation")
	}
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}
	if s.authorityCatalog().deletedResource(host) {
		t.Fatal("native tombstone settled a host floor")
	}
	for _, revocation := range s.Revocations {
		status, err := queryScopedAuthority(&s, QueryRequest{Kind: "authority", OperationID: revocation.OperationID})
		if err != nil || slices.ContainsFunc(status.Obligations, func(ob topology.FenceObligation) bool { return ob.Resource == native }) {
			t.Fatal("authority query retained impossible native fence", status, err)
		}
	}
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	if len(s.indexes.obligations[native]) != 0 || !s.authorityCatalog().deletedResource(native) {
		t.Fatal("pruning reopened native deletion fence")
	}
}
