//go:build !functional

package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func pendingScopedRetirement(t *testing.T) scopedState {
	t.Helper()
	s := scopedCodecFixture()
	debt := &s.Cleanup[0]
	key := debt.Retired.Key
	debt.Retired, debt.RetiredAt, debt.Collection = nil, nil, nil
	s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool { return r.Grant.Key == key })
	for i := range s.AuthorityFloors {
		if s.AuthorityFloors[i].Key == key {
			s.AuthorityFloors[i].Key = debt.Stopped.Key
		}
	}
	prepareScopedFixture(t, &s)
	return s
}

func issueScopedCleanup(t *testing.T, s *scopedState, p BeginCleanupEffectProposal) proposalApplication {
	t.Helper()
	p.ProposalID = "cleanup"
	body, _ := json.Marshal(p)
	result, err := applyScopedProposal(s, s.AppliedIndex+1, "begin_cleanup_effect", body)
	if err != nil || !result.result.Accepted || result.result.Grant == nil {
		t.Fatal("cleanup issuance refused", result, err)
	}
	prepareScopedFixture(t, s)
	return result
}

func TestScopedCleanupIssuesExactRetirementAfterFailedOperation(t *testing.T) {
	t.Parallel()
	s := pendingScopedRetirement(t)
	debt := s.Cleanup[0].clone()
	p := BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.RetireAction}
	before := s.Target.Clone()
	issued := issueScopedCleanup(t, &s, p)
	grant := *issued.result.Grant
	if !issued.changed || s.indexes.grants[grant.Key].Class != "cleanup" || len(grant.Resources) != 1 ||
		len(grant.Targets) != 1 || grant.Targets[0].MemberID != debt.MemberID ||
		grant.Targets[0].ReplicaID != debt.Identity.ReplicaID || !reflect.DeepEqual(s.Target, before) ||
		s.indexes.operations[debt.OperationID].Status.State != topology.OperationFailed {
		t.Fatal("cleanup lost original host or affected successor/failed operation", grant)
	}
	if err := s.authorizeLocked(context.Background(), p.Action); err != nil {
		t.Fatal("retained detached action failed receiver authorization", err)
	}
	if _, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: p.Action}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("ordinary issuance bypassed exact cleanup identity", err)
	}
	state, _ := json.Marshal(s)
	repeat := issueScopedCleanup(t, &s, p)
	after, _ := json.Marshal(s)
	if repeat.changed || !grant.Equal(*repeat.result.Grant) || !bytes.Equal(state, after) {
		t.Fatal("cleanup polling minted another grant or wrote state")
	}
	for _, mutate := range []func(*BeginCleanupEffectProposal){
		func(p *BeginCleanupEffectProposal) { p.Action.OperationID = "exec-v3-a1" },
		func(p *BeginCleanupEffectProposal) { p.Action.Kind = topology.StepPrepareReplica },
		func(p *BeginCleanupEffectProposal) { p.Identity.ReplicaID++ },
		func(p *BeginCleanupEffectProposal) { p.Action.StepSeq-- },
	} {
		wrong := p
		mutate(&wrong)
		if _, err := s.applyBeginCleanupEffect(s.AppliedIndex+1, wrong); err == nil {
			t.Fatal("wrong debt/action tuple accepted", wrong)
		}
	}
	after, _ = json.Marshal(s)
	if !bytes.Equal(state, after) {
		t.Fatal("refused cleanup tuple changed authority")
	}
}

func fenceCleanupFixture(t *testing.T, s *scopedState, old topology.AuthorityGrant) topology.FenceEvidence {
	t.Helper()
	s.indexes.grants[old.Key].Closed = true
	grant := old.Clone()
	grant.Key.Revision, grant.Purpose, grant.OperationID, grant.Action = s.AppliedIndex+1, topology.AuthorityFence, "", nil
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant}})
	for i := range s.AuthorityFloors {
		if s.AuthorityFloors[i].Resource == grant.Resources[0] {
			s.AuthorityFloors[i].Key = grant.Key
		}
	}
	s.AppliedIndex = grant.Key.Revision
	prepareScopedFixture(t, s)
	return topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0], LocalSequence: grant.Key.Revision * 10,
		Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n", MemberID: 1, InstanceID: "old"}}
}

func TestScopedCleanupReissuesOnlyAfterFenceAndKeepsFirstMilestone(t *testing.T) {
	t.Parallel()
	for _, oldFirst := range []bool{false, true} {
		s := pendingScopedRetirement(t)
		debt := s.Cleanup[0].clone()
		p := BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.RetireAction}
		old := *issueScopedCleanup(t, &s, p).result.Grant
		fence := fenceCleanupFixture(t, &s, old)
		before, _ := json.Marshal(s)
		if _, err := s.applyBeginCleanupEffect(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrAuthorityPending) {
			t.Fatal("unreconciled same-host floor was bypassed", err)
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("pending floor minted a revision")
		}
		body, _ := json.Marshal(RecordFenceProposal{Evidence: fence, ProposalID: "fence"})
		accepted, err := applyScopedProposal(&s, s.AppliedIndex+1, KindRecordFence, body)
		if err != nil || !accepted.result.Accepted || !accepted.changed {
			t.Fatal("fence installation failed", accepted, err)
		}
		newGrant := *issueScopedCleanup(t, &s, p).result.Grant
		if newGrant.Key == old.Key || *newGrant.Action != *old.Action {
			t.Fatal("reconciliation changed action or reused fenced grant")
		}
		first, last := newGrant, old
		if oldFirst {
			first, last = last, first
		}
		at := debt.UpdatedAt.Add(time.Second)
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(first)[0], At: at})
		retired := s.indexes.cleanup[debt.Identity].clone()
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(last)[0], At: at.Add(time.Hour)})
		if !reflect.DeepEqual(s.indexes.cleanup[debt.Identity], &retired) {
			t.Fatal("late compatible revision rewrote retirement or timestamps", oldFirst)
		}
		if s.authorityCatalog().actions[p.Action] != newGrant.Key {
			t.Fatal("action index depends on map/restore order")
		}
	}
}

func TestScopedCleanupRestoreRefusesConcurrentRevisions(t *testing.T) {
	t.Parallel()
	s := pendingScopedRetirement(t)
	debt := s.Cleanup[0]
	old := *issueScopedCleanup(t, &s, BeginCleanupEffectProposal{
		Identity: debt.Identity, Action: *debt.RetireAction}).result.Grant
	for _, class := range []string{"cleanup", "execution"} {
		newGrant := old.Clone()
		newGrant.Key.Revision++
		bad := s
		bad.AuthorityRecords = append(slices.Clone(s.AuthorityRecords), scopedAuthorityRecord{
			Class: class, authorityRecord: authorityRecord{Grant: newGrant}})
		bad.AuthorityFloors = slices.Clone(s.AuthorityFloors)
		for i := range bad.AuthorityFloors {
			if bad.AuthorityFloors[i].Key == old.Key {
				bad.AuthorityFloors[i].Key = newGrant.Key
			}
		}
		bad.AppliedIndex++
		if _, err := prepareScopedGraph(&bad, bad.Rules); err == nil {
			t.Fatal("unreconciled or misclassified duplicate cleanup restored", class)
		}
	}
}

func assertReboundCleanupAuthorization(t *testing.T, source *scopedState, original topology.AuthorizedStep) {
	t.Helper()
	body, _ := json.Marshal(source)
	var s scopedState
	if err := json.Unmarshal(body, &s); err != nil {
		t.Fatal(err)
	}
	prepareScopedFixture(t, &s)
	debt := s.Cleanup[0]
	if err := s.authorizeLocked(context.Background(), *debt.StopAction); err != nil {
		t.Fatal("detached successor binding lost approval", err)
	}
	if _, err := s.applyBeginCleanupEffect(s.AppliedIndex+1, BeginCleanupEffectProposal{
		Identity: debt.Identity, Action: original}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("original operation authorized a rebound stop", err)
	}
	old := s.authorityCatalog().records[s.authorityCatalog().actions[original]].Grant
	fence := fenceCleanupFixture(t, &s, old)
	if _, err := s.applyRecordFence(s.AppliedIndex+1, RecordFenceProposal{Evidence: fence}); err != nil {
		t.Fatal(err)
	}
	grant := issueScopedCleanup(t, &s, BeginCleanupEffectProposal{
		Identity: debt.Identity, Action: *debt.StopAction}).result.Grant
	if grant.OperationID == topology.OperationID(debt.OperationID) || *grant.Action != *debt.StopAction {
		t.Fatal("rebound stop was resolved against debt origin", grant)
	}
}
