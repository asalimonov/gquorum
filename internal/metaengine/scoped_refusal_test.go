//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedRefusalPreservesProofAndAdvancesAppliedBoundary(t *testing.T) {
	t.Parallel()
	s, p := scopedReadmissionCommitFixture(t)
	accepted := applyAdmissionFixture(t, &s, KindCommitReadmission, p)
	m := mustMachine(t, s.Rules, nil)
	m.scoped = &s
	stale := s.Latest.Clone()
	stale.Comment = "different request at an old CAS"
	for _, tc := range []struct {
		proposal any
		code     fault.Code
	}{
		{ReadyAdmissionProposal{NodeName: p.Admission.NodeName, InstanceID: p.Admission.InstanceID,
			MemberID: p.Admission.MemberID, OperationID: accepted.result.Admission.OperationID,
			ProposalID: "pending"}, fault.AuthorityPending},
		{BindAdmissionProposal{At: p.At, ProposalID: "invalid"}, fault.Invalid},
		{ProposeDocument{Doc: stale, ExpectedVersion: s.Latest.Version - 1, ProposalID: "stale"}, fault.VersionConflict},
	} {
		before, _ := json.Marshal(s)
		var expected scopedState
		if err := json.Unmarshal(before, &expected); err != nil {
			t.Fatal(err)
		}
		command := raftengine.Command{Index: s.AppliedIndex + 1, Term: s.AppliedTerm + 1,
			Data: mustEncode(t, tc.proposal)}
		result, err := m.Apply(t.Context(), command)
		var refusal ApplyResult
		if err != nil || json.Unmarshal(result.Data, &refusal) != nil || refusal.Accepted ||
			refusal.ProposalID == "" || result.Code != string(tc.code) || refusal.CurrentVersion != s.Latest.Version {
			t.Fatal("normal refusal escaped as a replica-fatal error", result, refusal, err)
		}
		expected.AppliedIndex, expected.AppliedTerm = command.Index, command.Term
		want, _ := json.Marshal(expected)
		got, err := m.Snapshot(t.Context())
		if err != nil || !bytes.Equal(want, got) {
			t.Fatal("refusal lost retained proof or its applied boundary", err)
		}
		if err := m.Restore(t.Context(), got); err != nil {
			t.Fatal("refused entry could not restore", err)
		}
		s = *m.scoped
		m.scoped = &s
	}
	result, err := m.Apply(t.Context(), raftengine.Command{Index: s.AppliedIndex + 1, Term: s.AppliedTerm,
		Data: mustEncode(t, GrantSession{SessionID: "after-refusals", NodeName: "n1", InstanceID: "alive",
			TTLSeconds: 30, ProposalID: "grant"})})
	if err != nil || result.Code != "" {
		t.Fatal("valid mutation failed after ordinary refusals", result, err)
	}
}

func TestScopedRefusalCannotHidePersistenceFailure(t *testing.T) {
	t.Parallel()
	s, p := scopedReadmissionCommitFixture(t)
	accepted := applyAdmissionFixture(t, &s, KindCommitReadmission, p)
	boom := errors.New("disk failed")
	m := mustMachine(t, s.Rules, failingPersister{boom})
	m.scoped = &s
	_, err := m.Apply(t.Context(), raftengine.Command{Index: s.AppliedIndex + 1, Term: s.AppliedTerm,
		Data: mustEncode(t, ReadyAdmissionProposal{NodeName: p.Admission.NodeName, InstanceID: p.Admission.InstanceID,
			MemberID: p.Admission.MemberID, OperationID: accepted.result.Admission.OperationID, ProposalID: "pending"})})
	if !errors.Is(err, errDurablePersist) || !errors.Is(err, boom) {
		t.Fatal("refusal acknowledged an unpersisted applied boundary", err)
	}
}

func TestScopedUnknownCommandCannotAdvanceAppliedState(t *testing.T) {
	t.Parallel()
	for _, kind := range []ProposalKind{"future_operation", "begin_execution"} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			s, _ := scopedReadmissionCommitFixture(t)
			m := mustMachine(t, s.Rules, nil)
			m.scoped = &s
			before, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(envelope{SchemaVersion: metaEnvelopeSchemaVersion, Kind: kind,
				Body: json.RawMessage(`{"proposal_id":"unknown"}`)})
			if err != nil {
				t.Fatal(err)
			}
			result, err := m.Apply(t.Context(), raftengine.Command{Index: s.AppliedIndex + 1,
				Term: s.AppliedTerm + 1, Data: data})
			if err == nil || len(result.Data) != 0 {
				t.Fatal("uninterpretable command was acknowledged", result, err)
			}
			after, err := m.Snapshot(t.Context())
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("fatal command changed proof or the applied boundary", err)
			}
		})
	}
}

func TestScopedMalformedPlansRefuseWithoutHaltingReplay(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"orphan stop", "unrelated group", "disallowed action", "voter surge", "unknown approval action", "unknown voter action"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var s scopedState
			var proposal any
			switch mode {
			case "orphan stop", "unknown approval action":
				var p BeginOperationProposal
				s, p = approvalFixture(t, "shard0")
				kind := topology.StepStopReplica
				if mode == "unknown approval action" {
					kind = "future_action"
				}
				p.Plan.Steps = []topology.Step{{Seq: 1, Kind: kind, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}}
				p.Plan.Blocks[0].Steps = []int{1}
				proposal = p
			default:
				var p CommitVoterChangeProposal
				s, p = scopedVoterChangeFixture(t, "work", 1)
				switch mode {
				case "unrelated group":
					p.Plan.Steps[0].GroupID = topology.MetaGroupID
				case "disallowed action":
					p.Plan.Steps[0].Kind = topology.StepStopReplica
				case "voter surge":
					s.Target = s.Target.Clone()
					s.Target.Group("work").VoterConfig.MaxSurgeVoters = 0
					s.Latest.Group("work").VoterConfig.MaxSurgeVoters = 0
					for i := range s.Groups {
						if s.Groups[i].GroupID == "work" {
							owned := *s.Groups[i].Declaration
							config := *owned.VoterConfig
							config.MaxSurgeVoters = 0
							owned.VoterConfig = &config
							s.Groups[i].Declaration = &owned
						}
					}
					p.Doc.Group("work").VoterConfig.MaxSurgeVoters = 0
				case "unknown voter action":
					p.Plan.Steps[0].Kind = "future_action"
				}
				proposal = p
			}
			m := restoreMachineFixture(t, s)
			before, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			command := raftengine.Command{Index: s.AppliedIndex + 1, Term: s.AppliedTerm + 1, Data: mustEncode(t, proposal)}
			result, err := m.Apply(t.Context(), command)
			unknown := mode == "unknown approval action" || mode == "unknown voter action"
			var expected scopedState
			if decodeErr := json.Unmarshal(before, &expected); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if unknown {
				if err == nil || len(result.Data) != 0 {
					t.Fatal("unknown action was acknowledged", result, err)
				}
			} else {
				var refusal ApplyResult
				if err != nil || json.Unmarshal(result.Data, &refusal) != nil || refusal.Accepted ||
					result.Code != string(fault.Invalid) || refusal.ProposalID == "" {
					t.Fatal("known malformed plan halted replay or was accepted", result, refusal, err)
				}
				expected.AppliedIndex, expected.AppliedTerm = command.Index, command.Term
			}
			want, _ := json.Marshal(expected)
			got, err := m.Snapshot(t.Context())
			if err != nil || !bytes.Equal(want, got) {
				t.Fatal("malformed plan changed proof or lost its applied boundary", err)
			}
			if err := m.Restore(t.Context(), got); err != nil {
				t.Fatal(err)
			}
		})
	}
}
