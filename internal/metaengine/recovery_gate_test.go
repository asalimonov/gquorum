//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRecoveryPendingGatePermitsOnlyExactInternalProgress(t *testing.T) {
	t.Parallel()
	s, _ := importedRecoveryWithPolicy(t, false)
	for _, p := range []any{
		ProposeDocument{Doc: s.Latest.Clone()}, BeginOperationProposal{Version: 2}, GrantTransferProposal{},
		CommitReadmissionProposal{}, ReadyAdmissionProposal{}, CommitVoterChangeProposal{},
		AcquireLock{Key: "app/lock"}, GrantSession{NodeName: "foreign"},
		BindAdmissionProposal{Founding: true, Admission: AdmissionRecord{NodeName: "n1", MemberID: 1}},
		RetryOperationProposal{Previous: topology.MakeOperationID(1, 1)},
	} {
		kind, _ := proposalKind(p)
		body, _ := json.Marshal(p)
		if err := s.recoveryCommandAllowed(kind, body); err == nil {
			t.Fatalf("pending import permitted %T", p)
		}
	}
	for _, p := range []any{
		BeginEffectProposal{ProposalID: "progress"}, RecordEffectProposal{ProposalID: "progress"},
		RecordFenceProposal{ProposalID: "progress"}, TransitionOperationProposal{ProposalID: "progress"},
		GrantSession{NodeName: "n1"}, RevokeSession{}, AcquireLock{Key: "node/n1"},
		BindAdmissionProposal{Founding: true, Admission: AdmissionRecord{NodeName: "n1", MemberID: 5}},
		RetryOperationProposal{Previous: topology.MakeOperationID(2, 1), Scopes: []ScopeExpectation{}, ProposalID: "retry"},
	} {
		kind, _ := proposalKind(p)
		body, _ := json.Marshal(p)
		if err := s.recoveryCommandAllowed(kind, body); err != nil {
			t.Fatalf("internal %T refused: %v", p, err)
		}
	}
}

func TestRecoveryMetadataOnlyCompletesThroughOrdinaryOperation(t *testing.T) {
	t.Parallel()
	s := recoveryFoundingFixture(t)
	p := recoveryImportFixture()
	p.ApprovedSource.Groups = p.ApprovedSource.Groups[:1]
	p.Sources = nil
	result, err := foundingCommand(t, &s, p, raftpb.ConfState{Voters: []uint64{5}})
	if err != nil || !result.result.Accepted {
		t.Fatal(result, err)
	}
	op := s.indexes.operations[s.indexes.first[2]]
	if len(s.Recovery.Groups) != 0 || op.Status.State != topology.OperationSucceeded {
		t.Fatal(s.Recovery, op)
	}
	completion := model.CompleteImportProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest,
		ProposalID: "complete", At: time.Unix(120, 0).UTC()}
	if _, err := s.applyCompleteRecoveryImport(s.AppliedIndex+1, completion); err != nil {
		t.Fatal(err)
	}
	if s.Recovery.Phase != "complete" {
		t.Fatal(s.Recovery)
	}
	if err := s.pruneRetainedProofs(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.recoveryCommandAllowed(KindProposeDocument, nil); err != nil {
		t.Fatal("completed metadata-only recovery retained public mutation gate", err)
	}
}

func TestRecoveryPendingRefusalIsACommittedResultRatherThanApplyFailure(t *testing.T) {
	t.Parallel()
	state, _ := importedRecoveryWithPolicy(t, false)
	machine := restoreMachineFixture(t, state)
	before, _ := machine.AppliedState()
	result := applyMachine(t, machine, ProposeDocument{Doc: state.Latest.Clone(),
		ExpectedVersion: state.Latest.Version, ProposalID: "blocked-user-stage"})
	after, _ := machine.AppliedState()
	if result.Accepted || result.Code != proposalErrorCode(fault.OperationAuthorization) || after != before+1 ||
		machine.Target().Version != 2 {
		t.Fatal("pending gate halted apply or changed topology", result, before, after)
	}
}
