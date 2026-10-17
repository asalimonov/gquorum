//go:build !functional

package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func prepareScopedFixture(t *testing.T, s *scopedState) {
	t.Helper()
	var err error
	s.indexes, err = prepareScopedGraph(s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
}

func TestScopedEffectSurvivesNewTargetAndIndependentPendingFence(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, topology.MetaGroupID)
	s.Operations[1].Status.State = topology.OperationRunning
	s.Operations[1].Status.Blocks[1].State = topology.OperationRunning
	s.AuthorityRecords[1].Fences = nil
	s.Revocations[0].Obligations[0].Evidence = nil
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, approval)
	action := scopedPlanAction(&s.Operations[1], s.Operations[1].Plan.Steps[1])
	if action.Version == s.Target.Version || len(s.indexes.obligations) == 0 {
		t.Fatal("fixture lacks independently advanced Target or unfinished fence")
	}
	body, _ := json.Marshal(BeginEffectProposal{Action: action, ProposalID: "effect"})
	result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindBeginEffect, body)
	if err != nil || !result.changed || !result.result.Accepted || *result.result.Grant.Action != action {
		t.Fatal("unrelated fence blocked old operation", result, err)
	}
	grant := result.result.Grant.Clone()
	result.result.Grant.Targets[0].NodeName = "caller-mutated"
	result, err = applyScopedProposal(&s, s.AppliedIndex+1, KindBeginEffect, body)
	if err != nil || result.changed || !reflect.DeepEqual(grant, *result.result.Grant) {
		t.Fatal("exact retry minted a grant or aliased the caller", result, err)
	}
	encoded, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.authorizeLocked(context.Background(), action); err != nil {
		t.Fatal("restored exact older operation lost authorization", err)
	}
}

func TestScopedAuthorizationChecksFullTupleBeforeRetryableLag(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(&s.Operations[1], s.Operations[1].Plan.Steps[1])
	if err := s.authorizeLocked(context.Background(), action); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("exact pending block is not retryable", err)
	}
	for name, mutate := range map[string]func(*topology.AuthorizedStep){
		"operation": func(a *topology.AuthorizedStep) { a.OperationID = "exec-v2-a1" },
		"version":   func(a *topology.AuthorizedStep) { a.Version++ },
		"attempt":   func(a *topology.AuthorizedStep) { a.Attempt++ },
		"sequence":  func(a *topology.AuthorizedStep) { a.StepSeq++ },
		"kind":      func(a *topology.AuthorizedStep) { a.Kind = topology.StepStopReplica },
		"group":     func(a *topology.AuthorizedStep) { a.GroupID = topology.MetaGroupID },
		"node":      func(a *topology.AuthorizedStep) { a.NodeName = "foreign" },
		"replica":   func(a *topology.AuthorizedStep) { a.ReplicaID++ },
		"voters":    func(a *topology.AuthorizedStep) { a.ExpectedVotersDigest = "foreign" },
		"transfer":  func(a *topology.AuthorizedStep) { a.TransferReplicaID = 9 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			forged := action
			mutate(&forged)
			if err := s.authorizeLocked(context.Background(), forged); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("forged tuple treated as apply lag", err)
			}
		})
	}
}

func TestScopedEffectRefusesFencedGrantWithoutChangingState(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations[1].Status.State, s.Operations[1].Status.Blocks[1].State = topology.OperationRunning, topology.OperationRunning
	key := scopedPendingTransfer(&s)
	prepareScopedFixture(t, &s)
	if err := s.revokeAuthorityLocked(11, []blockRef{{"exec-v2-a2", "shard0"}}, ""); err != nil {
		t.Fatal(err)
	}
	s.AppliedIndex = 11
	prepareScopedFixture(t, &s)
	action := *s.indexes.grants[key].Grant.Action
	before, _ := json.Marshal(s)
	if _, err := s.applyBeginEffect(12, BeginEffectProposal{Action: action}); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("fenced exact action was reissued", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("refused issuance changed authority")
	}
}

func TestScopedDirectTransferUsesGroupSourceAndRelevantFence(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, "shard0")
	scopedPendingTransfer(&s)
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, approval)
	p := GrantTransferProposal{ExpectedSourceVersion: 2, GroupID: topology.MetaGroupID,
		NodeName: "n1", ReplicaID: 1, RequestID: "transfer", ProposalID: "transfer"}
	result, err := s.applyGrantTransfer(s.AppliedIndex+1, p)
	if err != nil || !result.changed || result.result.Grant.Action.Version != 2 {
		t.Fatal("independent native transfer blocked", result, err)
	}
	p.RequestID = "coalesced"
	retry, err := s.applyGrantTransfer(s.AppliedIndex+1, p)
	if err != nil || retry.changed || !reflect.DeepEqual(retry.result.Grant, result.result.Grant) {
		t.Fatal("same unfinished transfer was not reused", retry, err)
	}
	p.ExpectedSourceVersion = s.Target.Version
	if _, err := s.applyGrantTransfer(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("global Target substituted for exact group source", err)
	}
}
