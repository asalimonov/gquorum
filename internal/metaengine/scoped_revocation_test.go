//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedPendingTransfer(s *scopedState) topology.AuthorityKey {
	action := scopedPlanAction(&s.Operations[1], s.Operations[1].Plan.Steps[1])
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"}
	key := topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: "shard0", Revision: 9}
	grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID,
		Action: &action, Resources: []topology.AuthorityResource{resource},
		Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{Grant: grant}})
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: resource, Key: key})
	return key
}

func applyScopedApprovalFixture(t *testing.T, s *scopedState, proposal BeginOperationProposal) {
	t.Helper()
	prepareScopedFixture(t, s)
	proposal.ProposalID = "approval"
	body, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyScopedProposal(s, s.AppliedIndex+1, "begin_operation", body)
	if err != nil || !result.changed || !result.result.Accepted {
		t.Fatal("approval failed", result, err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal("installed state cannot round-trip", err)
	}
}

func TestScopedRevocationPreservesIndependentExecutionAndProgress(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "shard0"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			s, proposal := approvalFixture(t, group)
			key := scopedPendingTransfer(&s)
			var err error
			s.indexes, err = prepareScopedGraph(&s, s.Rules)
			if err != nil {
				t.Fatal(err)
			}
			old := s.AuthorityRecords[len(s.AuthorityRecords)-1]
			applyScopedApprovalFixture(t, &s, proposal)
			grant := s.indexes.grants[key]
			if group == topology.MetaGroupID {
				if !reflect.DeepEqual(*grant, old) || s.indexes.groups["shard0"].Owner != "exec-v2-a2" {
					t.Fatal("metadata approval revoked an independent load owner")
				}
				return
			}
			if !grant.Closed || len(s.indexes.obligations[grant.Grant.Resources[0]]) == 0 {
				t.Fatal("supersession lost the affected native fence")
			}
			obligations := slices.Clone(s.indexes.obligations[grant.Grant.Resources[0]])
			s.Latest = s.Target.Clone()
			s.Latest.Version, s.Latest.ParentVersion = 4, 3
			s.Latest.Node("n1").Labels = map[string]string{"owner": "independent"}
			proposal.Version, proposal.At = 4, proposal.At.Add(time.Second)
			proposal.Plan = topology.Plan{Version: 4, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 2}}}
			proposal.Scopes = []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 2}}
			applyScopedApprovalFixture(t, &s, proposal)
			if s.indexes.groups[topology.MetaGroupID].SourceVersion != 4 ||
				!slices.Equal(s.indexes.obligations[grant.Grant.Resources[0]], obligations) {
				t.Fatal("an unrelated pending fence blocked progress or was replaced")
			}
		})
	}
}

func TestScopedRevocationFencesReboundStopAndPreservesOtherCleanup(t *testing.T) {
	t.Parallel()
	s, proposal := cleanupApprovalFixture(t)
	stop := *s.Cleanup[0].StopAction
	var oldKey topology.AuthorityKey
	var unrelated []scopedAuthorityRecord
	for _, record := range s.AuthorityRecords {
		if record.Grant.Action != nil && *record.Grant.Action == stop {
			oldKey = record.Grant.Key
		}
		if record.Grant.Key.GroupID == "b" {
			unrelated = append(unrelated, record)
		}
	}
	applyScopedApprovalFixture(t, &s, proposal)
	old := s.indexes.grants[oldKey]
	resource := old.Grant.Resources[0]
	if !old.Closed || len(s.indexes.obligations[resource]) == 0 || s.Cleanup[0].StopAction.Version != 7 {
		t.Fatal("new stop binding bypassed its predecessor host grant")
	}
	for _, record := range unrelated {
		if !reflect.DeepEqual(*s.indexes.grants[record.Grant.Key], record) {
			t.Fatal("unrelated cleanup was fenced")
		}
	}
	before := s.AuthorityRecords
	// A second scoped revocation reuses the same unfinished resource fence.
	op := s.indexes.operations["exec-v7-a1"]
	if err := s.revokeAuthorityLocked(32, s.supersededBlocks(op), op.Status.ID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, s.AuthorityRecords) {
		t.Fatal("repeated supersession minted another pending fence")
	}
}

func TestScopedRevocationKeepsPendingDeletionReadmission(t *testing.T) {
	t.Parallel()
	s, proposal := existingDebtDeletionFixture(t)
	s.Rules.Limits.MaxPendingAuthorityRecords = 3 // Durable proof exceeds the obsolete global budget.
	var records []scopedAuthorityRecord
	for _, record := range s.AuthorityRecords {
		if record.Grant.Purpose == topology.AuthorityReadmit {
			records = append(records, record)
		}
	}
	applyScopedApprovalFixture(t, &s, proposal)
	for _, record := range records {
		if !reflect.DeepEqual(*s.indexes.grants[record.Grant.Key], record) {
			t.Fatal("deletion approval fenced its own readmission prerequisite")
		}
	}
}

func TestScopedAuthorityStatusAggregatesRequestedBlocks(t *testing.T) {
	t.Parallel()
	s, proposal := approvalFixture(t, "shard0")
	scopedPendingTransfer(&s)
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	query := QueryRequest{Kind: "authority", OperationID: "exec-v2-a2"}
	status, err := queryScopedAuthority(&s, query)
	if err != nil || status.RevocationRequested || status.RevocationComplete {
		t.Fatal("not requested became completed", status, err)
	}
	applyScopedApprovalFixture(t, &s, proposal)
	status, err = queryScopedAuthority(&s, query)
	if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) != 1 {
		t.Fatal("aggregate lost pending scoped revocation", status, err)
	}
	complete, err := queryScopedAuthority(&s, QueryRequest{Kind: "authority", OperationID: "exec-v2-a1"})
	if err != nil || !complete.RevocationRequested || !complete.RevocationComplete {
		t.Fatal("requested and settled blocks did not aggregate to completion", complete, err)
	}
	ob := status.Obligations[0]
	record := s.indexes.grants[ob.Key]
	record.Fences = []topology.FenceEvidence{groupFenceProof(record.Grant, 20)}
	status, err = queryScopedAuthority(&s, query)
	if err != nil || !status.RevocationComplete || status.Obligations[0].Evidence == nil {
		t.Fatal("durable fence did not settle aggregate", status, err)
	}
	status.Obligations[0].Evidence.Receiver.NodeName = "owned-copy"
	if record.Fences[0].Receiver.NodeName == "owned-copy" {
		t.Fatal("status aliases durable evidence")
	}
	query.OperationID = "exec-v99-a1"
	if _, err := queryScopedAuthority(&s, query); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("unknown operation reported completion", err)
	}
}

func TestScopedDeletionFencesIssuedPreparationAndFounding(t *testing.T) {
	t.Parallel()
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepCreateFoundingGroup} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			s, proposal := deletionApprovalFixture(t)
			s.History[0].Groups = s.History[0].Groups[:1]
			s.Groups[1].SourceVersion, s.Groups[1].Declaration, s.Groups[1].CompletedAt = 0, nil, time.Time{}
			proposal.Scopes[0].SourceVersion, proposal.Plan.Blocks[0].SourceVersion = 0, 0
			for i := range s.Operations {
				op := &s.Operations[i]
				op.Plan.Blocks[1].SourceVersion = 0
				op.Plan.Steps[1].Kind = kind
				if kind == topology.StepCreateFoundingGroup {
					op.Plan.Steps[1].NodeName, op.Plan.Steps[1].ReplicaID = "", 0
					op.Plan.Steps[1].ExpectedVotersDigest = topology.VoterSetDigest(nil)
					op.Plan.Steps[1].ResultVotersDigest = topology.VoterSetDigest([]uint64{1})
					op.Plan.VoterBases = []topology.PlanVoterBase{{GroupID: "shard0"}}
				}
			}
			action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[1])
			grant := topology.AuthorityGrant{Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID,
				Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: "shard0", Revision: 5}, Action: &action,
				Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}},
				Targets:   []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
			if kind == topology.StepCreateFoundingGroup {
				grant.Resources = append(grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"})
				slices.SortFunc(grant.Resources, topology.AuthorityResource.Compare)
			}
			s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
				authorityRecord: authorityRecord{Grant: grant}})
			for _, resource := range grant.Resources {
				s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: resource, Key: grant.Key})
			}
			var err error
			s.indexes, err = prepareScopedGraph(&s, s.Rules)
			if err != nil {
				t.Fatal(err)
			}
			applyScopedApprovalFixture(t, &s, proposal)
			for _, resource := range grant.Resources {
				if len(s.indexes.obligations[resource]) == 0 || !s.indexes.grants[grant.Key].Closed {
					t.Fatal("deletion omitted an issued creation resource", resource)
				}
			}
			if s.Cleanup[0].Stopped != nil || !s.Groups[1].Deleting {
				t.Fatal("revocation manufactured a stop or tombstone")
			}
		})
	}
}

func TestScopedReadmissionRevocationRetainsExactOriginOutsideExecutionBlock(t *testing.T) {
	t.Parallel()
	s, _ := existingDebtDeletionFixture(t)
	for _, record := range slices.Clone(s.AuthorityRecords) {
		if record.Grant.Purpose != topology.AuthorityReadmit || record.Grant.Key.GroupID != "a" {
			continue
		}
		grant := record.Grant.Clone()
		grant.Key.GroupID, grant.Resources[0].GroupID, grant.Targets[0].ReplicaID = "live", "live", 1
		s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{Grant: grant}})
		s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: grant.Resources[0], Key: grant.Key})
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal("retained batch fixture", err)
	}
	// Exercise cancellation preparation before the future admission handler
	// installs the replacement's declaration and new origin together.
	next := s
	next.Target = s.Target.Clone()
	next.Target.MetaGroup().Replicas[0].ReplicaID = 3
	if err := next.revokeAuthorityLocked(31, []blockRef{{"exec-v3-a1", topology.MetaGroupID}}, "exec-v3-a1"); err != nil {
		t.Fatal(err)
	}
	next.AppliedIndex = 31
	index, err := next.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restoreScopedAuthority(&next, index); err != nil {
		t.Fatal("batch's former group lost exact admission attribution", err)
	}
	found := false
	for _, record := range next.Revocations {
		if record.GroupID == "live" && record.OperationID == "exec-v3-a1" && len(record.Obligations) == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("readmission revocation omitted a former group outside its execution plan")
	}
}

func TestScopedRevocationFencesConflictingDetachedStop(t *testing.T) {
	t.Parallel()
	s, proposal := cleanupApprovalFixture(t)
	origin := s.indexes.operations[s.Cleanup[0].OperationID]
	origin.Plan.Blocks[0].Steps, origin.Plan.Blocks[0].Cleanup = []int{1}, []int{2, 3}
	origin.Status.Blocks[0].CurrentStep, origin.Status.Blocks[0].TotalSteps = 1, 1
	var key topology.AuthorityKey
	for i := range s.AuthorityRecords {
		r := &s.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Class, key = "cleanup", r.Grant.Key
		}
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	applyScopedApprovalFixture(t, &s, proposal)
	old := s.indexes.grants[key]
	if !old.Closed || len(s.indexes.obligations[old.Grant.Resources[0]]) == 0 ||
		s.Cleanup[0].Stopped != nil || s.Cleanup[0].StopAction.Version != 7 {
		t.Fatal("required stop bypassed detached predecessor or invented completion")
	}
}

func TestScopedApprovalLateRefusalsLeaveAllStateUnchanged(t *testing.T) {
	t.Parallel()
	for _, staleScope := range []bool{false, true} {
		s, proposal := cleanupApprovalFixture(t)
		proposal.ProposalID = "refused"
		index := s.AppliedIndex
		want := fault.ErrOperationAuthorization
		if staleScope {
			proposal.Scopes[0].Owner = "exec-v3-a1"
			index++
			want = fault.ErrVersionConflict
		}
		before, _ := json.Marshal(s)
		body, _ := json.Marshal(proposal)
		result, err := applyScopedProposal(&s, index, "begin_operation", body)
		if !errors.Is(err, want) || result.changed {
			t.Fatal("stale captured state/index accepted", result, err)
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("late refusal published ownership, debt or authority changes")
		}
	}
}
