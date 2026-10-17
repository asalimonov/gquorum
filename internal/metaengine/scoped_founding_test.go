//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func foundingApprovalFixture(t *testing.T) (scopedState, BeginOperationProposal) {
	t.Helper()
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules()}
	prepareScopedFixture(t, &s)
	doc := baseDoc()
	doc.Origin, doc.CreatedAt = topology.OriginFounding, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	if _, err := foundingCommand(t, &s, ProposeDocument{Doc: doc}, raftpb.ConfState{}); err != nil {
		t.Fatal(err)
	}
	input := topology.PlanInput{Native: map[string]topology.NativeState{
		topology.MetaGroupID: {GroupID: topology.MetaGroupID, ReplicaID: 1, Applied: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
	}}
	plan, err := topology.BuildPlan(input, s.Latest, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	return s, BeginOperationProposal{Version: 1, Plan: plan, ProposalID: "found", Origin: topology.OriginFounding,
		At: doc.CreatedAt, Actor: "n1", Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID}, {GroupID: "shard0"}}}
}

func foundingCommand(t *testing.T, s *scopedState, p any, seed raftpb.ConfState) (proposalApplication, error) {
	t.Helper()
	var data []byte
	if _, ok := p.(BeginOperationProposal); ok {
		body, _ := json.Marshal(p)
		data, _ = json.Marshal(envelope{SchemaVersion: metaEnvelopeSchemaVersion, Kind: "begin_operation", Body: body})
	} else {
		data = mustEncode(t, p)
	}
	return applyScopedCommand(s, raftengine.Command{Index: s.AppliedIndex + 1, Term: 1,
		Data: data, ConfState: seed})
}

func TestScopedFoundingConvergesMetadataDespiteFailedLoadAndRestoresAdmission(t *testing.T) {
	t.Parallel()
	s, p := foundingApprovalFixture(t)
	seed := raftpb.ConfState{Voters: []uint64{1}}
	result, err := foundingCommand(t, &s, p, seed)
	if err != nil || !result.changed {
		t.Fatal(result, err)
	}
	if s.nativeConf != nil || s.indexes.groups[topology.MetaGroupID].SourceVersion != 1 ||
		s.indexes.groups["shard0"].SourceVersion != 0 || s.FullyConverged.Version != 0 {
		t.Fatal("seed fabricated whole-document convergence or leaked context", s.Groups, s.FullyConverged)
	}
	failed := scopedTransitionFor(s.Operations[0], "shard0", 1)
	failed.State, failed.ErrorCode, failed.Error = topology.OperationFailed, "unavailable", "load factory failed"
	applyScopedTransitionFixture(t, &s, failed)
	// Unrelated approval changes the current operation and Target version, but
	// neither can replace the original founding admission proof.
	doc := s.Target.Clone()
	doc.Comment, doc.ParentVersion = "unrelated approval", 1
	if _, err := foundingCommand(t, &s, ProposeDocument{Doc: doc, ExpectedVersion: 1}, seed); err != nil {
		t.Fatal(err)
	}
	unrelated := BeginOperationProposal{Version: 2, Plan: topology.Plan{Version: 2},
		At: failed.At, Actor: "admin", Scopes: []ScopeExpectation{},
		Origin: topology.OriginOperator, ProposalID: "unrelated"}
	if _, err := foundingCommand(t, &s, unrelated, seed); err != nil {
		t.Fatal(err)
	}
	// A snapshot-installed follower skips the founding entry entirely. Its
	// retained checkpoint must suffice for a new exact admission binding.
	installed, _ := json.Marshal(s)
	var follower scopedState
	if err := json.Unmarshal(installed, &follower); err != nil {
		t.Fatal(err)
	}
	s = follower
	pbind := BindAdmissionProposal{Founding: true, At: failed.At, Admission: AdmissionRecord{
		NodeName: "n1", MemberID: 1, InstanceID: "first", OperationID: topology.OperationID(s.Operations[0].Status.ID)}}
	result, err = foundingCommand(t, &s, pbind, raftpb.ConfState{})
	if err != nil || !result.changed || result.result.Admission.State != AdmissionCompleted ||
		s.Operations[0].Status.State != topology.OperationFailed {
		t.Fatal("failed load prevented exact founding admission", result, err)
	}
	before, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	// No native seed is needed to replay an already accepted admission/approval.
	for _, proposal := range []any{p, pbind} {
		result, err = foundingCommand(t, &restored, proposal, raftpb.ConfState{})
		if err != nil || result.changed || !result.result.Accepted {
			t.Fatal("retained replay refounded seed", result, err)
		}
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(before, after) {
		t.Fatal("retained replay rewrote proof")
	}
	identity := topology.Identity{ClusterID: s.Target.ClusterID, NodeName: "n1", MemberID: 1}
	if id, ready := restored.admissionBasis(identity); !ready || id != s.Operations[0].Status.ID {
		t.Fatal("restored metadata required aggregate success", id, ready)
	}
	pbind.Admission.InstanceID = "replacement"
	if _, err := foundingCommand(t, &restored, pbind, seed); err == nil {
		t.Fatal("fresh process reused admitted seed identity")
	}
}

func TestScopedFoundingRefusesMissingForeignJointAndRoleChangedSeed(t *testing.T) {
	t.Parallel()
	for name, seed := range map[string]raftpb.ConfState{
		"missing": {}, "foreign": {Voters: []uint64{9}}, "extra": {Voters: []uint64{1, 2}},
		"joint":     {Voters: []uint64{1}, VotersOutgoing: []uint64{1}},
		"autoleave": {Voters: []uint64{1}, AutoLeave: true}, "learner": {Learners: []uint64{1}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := foundingApprovalFixture(t)
			before, _ := json.Marshal(s)
			result, err := foundingCommand(t, &s, p, seed)
			after, _ := json.Marshal(s)
			if !errors.Is(err, errFoundingSeedMismatch) || result.changed || !bytes.Equal(before, after) || s.nativeConf != nil {
				t.Fatal("invalid native seed changed state", result, err)
			}
		})
	}
}

func TestScopedFoundingBindRechecksMemberOperationAndNativeProof(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"member", "node", "instance", "operation", "time", "origin", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, approval := foundingApprovalFixture(t)
			seed := raftpb.ConfState{Voters: []uint64{1}}
			if _, err := foundingCommand(t, &s, approval, seed); err != nil {
				t.Fatal(err)
			}
			p := BindAdmissionProposal{Founding: true, At: approval.At, Admission: AdmissionRecord{
				NodeName: "n1", MemberID: 1, InstanceID: "first", OperationID: "exec-v1-a1"}}
			switch mode {
			case "replaced":
				s.Target = s.Target.Clone()
				s.Target.MetaGroup().Replicas[0].ReplicaID = 9
				s.Target.MetaGroup().NextReplicaID = 10
			case "member":
				p.Admission.MemberID++
			case "node":
				p.Admission.NodeName = "other"
			case "instance":
				p.Admission.InstanceID = ""
			case "operation":
				p.Admission.OperationID = "exec-v1-a2"
			case "time":
				p.At = time.Time{}
			case "origin":
				p.Admission.ReadmissionInstanceID = "forged"
			}
			before, _ := json.Marshal(s)
			if _, err := foundingCommand(t, &s, p, seed); err == nil {
				t.Fatal("forged founding admission accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused admission changed state")
			}
		})
	}
}

// This isolated command fixture mirrors Machine.Apply's committed context.
// Context is installed only on an owned candidate and cannot leak to a later
// command, a snapshot, or a read. AfterApply is too late to supply this evidence.
func applyScopedCommand(state *scopedState, c raftengine.Command) (proposalApplication, error) {
	decoded, err := decodeEnvelope(c.Data)
	if err != nil {
		return proposalApplication{}, err
	}
	next := *state
	next.AppliedTerm = c.Term
	next.nativeConf = &c.ConfState
	result, err := applyScopedProposal(&next, c.Index, decoded.Kind, decoded.Body)
	if err == nil && result.changed {
		next.nativeConf = nil
		*state = next
	}
	return result, err
}

func TestScopedFoundingReplayBeforeAfterApplyHasIdenticalCheckpoint(t *testing.T) {
	t.Parallel()
	before, p := foundingApprovalFixture(t)
	seed := raftpb.ConfState{Voters: []uint64{1}}
	persisted, _ := json.Marshal(before)
	var restarted scopedState
	if err := json.Unmarshal(persisted, &restarted); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*scopedState{&before, &restarted} {
		if _, err := foundingCommand(t, s, p, seed); err != nil {
			t.Fatal(err)
		}
	}
	original, _ := json.Marshal(before)
	replayed, _ := json.Marshal(restarted)
	if !bytes.Equal(original, replayed) {
		t.Fatal("replay needed AfterApply membership cache")
	}
}

func TestMachineAppliesScopedFoundingApprovalThroughEngineEntryPoint(t *testing.T) {
	t.Parallel()
	machine := mustMachine(t, testRules(), nil)
	doc := baseDoc()
	doc.Origin = topology.OriginFounding
	if _, err := machine.Apply(t.Context(), raftengine.Command{Index: 1, Term: 1,
		Data: mustEncode(t, ProposeDocument{Doc: doc})}); err != nil {
		t.Fatal("stage founding topology", err)
	}
	var read PlanRead
	query, _ := json.Marshal(QueryRequest{Kind: "approval_plan", Version: 1})
	result, err := machine.Query(t.Context(), query)
	if err != nil {
		t.Fatal("capture founding approval", err)
	}
	if err := json.Unmarshal(result.Data, &read); err != nil {
		t.Fatal("decode founding plan capture", err)
	}
	read.Input.Native = map[string]topology.NativeState{topology.MetaGroupID: {
		GroupID: topology.MetaGroupID, ReplicaID: 1, Applied: 1,
		ConfState: raftpb.ConfState{Voters: []uint64{1}},
	}}
	plan, err := topology.BuildPlanPreferred(read.Input, read.Target, topology.PlanOptions{Rules: testRules()})
	if err != nil {
		t.Fatal("build exact seed plan", err)
	}
	proposal := BeginOperationProposal{Version: 1, Plan: plan, Scopes: read.Scopes,
		Actor: "n1", Origin: topology.OriginFounding, At: time.Now().UTC(), ProposalID: "found"}
	if _, err := machine.Apply(t.Context(), raftengine.Command{Index: 2, Term: 1,
		Data: mustEncode(t, proposal), ConfState: raftpb.ConfState{Voters: []uint64{1}}}); err != nil {
		t.Fatal("apply founding operation", err)
	}
	basis, ok := machine.AdmissionBasis(topology.Identity{ClusterID: doc.ClusterID,
		NodeName: "n1", MemberID: 1, InstanceID: "instance"})
	if machine.Target().Version != 1 || !ok || basis == "" {
		t.Fatal("founding approval did not retain admission basis")
	}
	commands := []any{
		BindAdmissionProposal{Admission: AdmissionRecord{NodeName: "n1", InstanceID: "instance",
			MemberID: 1, OperationID: topology.OperationID(basis)}, Founding: true,
			At: time.Now().UTC(), ProposalID: "bind"},
		GrantSession{SessionID: "session", NodeName: "n1", InstanceID: "instance", TTLSeconds: 30, ProposalID: "session"},
		AcquireLock{Key: "node/n1", SessionID: "session", ProposalID: "lock"},
	}
	for i, command := range commands {
		if _, err := machine.Apply(t.Context(), raftengine.Command{Index: uint64(i + 3), Term: 1,
			Data: mustEncode(t, command), ConfState: raftpb.ConfState{Voters: []uint64{1}}}); err != nil {
			t.Fatalf("apply startup command %T: %v", command, err)
		}
	}
}

func TestMachineApplyRejectsFoundingSeedMismatchWithoutStateChange(t *testing.T) {
	t.Parallel()
	state, proposal := foundingApprovalFixture(t)
	machine := mustMachine(t, testRules(), nil)
	machine.scoped = &state
	before, err := machine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = machine.Apply(t.Context(), raftengine.Command{Index: state.AppliedIndex + 1, Term: 1,
		Data: mustEncode(t, proposal), ConfState: raftpb.ConfState{Voters: []uint64{9}}})
	if !errors.Is(err, errFoundingSeedMismatch) {
		t.Fatalf("founding seed mismatch must be an Apply error, got %v", err)
	}
	after, err := machine.Snapshot(t.Context())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed founding entry changed durable state: snapshot err=%v", err)
	}
}
