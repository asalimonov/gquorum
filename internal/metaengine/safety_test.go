package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func activatedSM(t *testing.T) (*topologySM, uint64) {
	t.Helper()
	sm := newTopologySM(nil, testRules())
	if r := apply(t, sm, 1, ProposeDocument{Doc: baseDoc()}); !r.Accepted {
		t.Fatal(r.Reason)
	}
	at := time.Unix(100, 0).UTC()
	b := apply(t, sm, 2, beginProposal(1, 0, "begin", at))
	if !b.Accepted {
		t.Fatal(b.Reason)
	}
	r := apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: b.OperatorID, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(time.Second)}})
	if !r.Accepted {
		t.Fatal(r.Reason)
	}
	if r = apply(t, sm, 4, ActivateTargetProposal{OperatorID: b.OperatorID, Version: 1, Attempt: 1}); !r.Accepted {
		t.Fatal(r.Reason)
	}
	return sm, 4
}

func TestRetirementFencesAtApprovalAndSurvivesRestart(t *testing.T) {
	t.Parallel()
	sm, index := activatedSM(t)
	original := sm.Active()
	d := original.Clone()
	d.Groups = d.Groups[:1]
	index++
	if r := apply(t, sm, index, ProposeDocument{Doc: d, ExpectedVersion: 1}); !r.Accepted {
		t.Fatal(r.Reason)
	}
	if sm.retired["shard0"] != 0 {
		t.Fatal("staging retired group before approval")
	}
	plan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, Kind: topology.StepStopReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: topology.StepDeleteGroupState, GroupID: "shard0"},
	}}
	index++
	if r := apply(t, sm, index, BeginOperatorProposal{Version: 2, Plan: plan, At: time.Unix(110, 0).UTC()}); !r.Accepted {
		t.Fatal(r.Reason)
	}
	if sm.Active().Version != 1 || sm.retired["shard0"] != 2 {
		t.Fatal("approval did not atomically retire ID")
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	index++
	r := apply(t, restored, index, ProposeDocument{Doc: original, ExpectedVersion: 2})
	if r.Accepted || r.Code != proposalGuardrail {
		t.Fatal("retired ID admitted", r)
	}
}

func TestAuthorizeEveryIdentityAndProgressBoundary(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc()})
	at := time.Unix(100, 0).UTC()
	b := apply(t, sm, 2, beginProposal(1, 1, "begin", at))
	a := topology.AuthorizedStep{OperatorID: b.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, Kind: topology.StepTransferLeader, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
	if err := sm.Authorize(context.Background(), a); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("pending effect authorized", err)
	}
	badPending := a
	badPending.ReplicaID++
	if err := sm.Authorize(context.Background(), badPending); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("invalid pending identity treated as lag", err)
	}
	apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: b.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning, GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second)}})
	if err := sm.Authorize(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*topology.AuthorizedStep){
		func(a *topology.AuthorizedStep) { a.OperatorID = "wrong" }, func(a *topology.AuthorizedStep) { a.Version = 0 },
		func(a *topology.AuthorizedStep) { a.Attempt++ }, func(a *topology.AuthorizedStep) { a.StepSeq++ },
		func(a *topology.AuthorizedStep) { a.Kind = topology.StepStopReplica }, func(a *topology.AuthorizedStep) { a.GroupID = "other" },
		func(a *topology.AuthorizedStep) { a.NodeName = "other" }, func(a *topology.AuthorizedStep) { a.ReplicaID++ },
	}
	for i, mutate := range mutations {
		bad := a
		mutate(&bad)
		if err := sm.Authorize(context.Background(), bad); !errors.Is(err, fault.ErrOperatorAuthorization) {
			t.Fatalf("identity field %d accepted: %v", i, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sm.Authorize(ctx, a); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	apply(t, sm, 4, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: b.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorSucceeded, GroupID: "shard0", NodeName: "n1", At: at.Add(2 * time.Second)}})
	if err := sm.Authorize(context.Background(), a); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("completed effect authorized", err)
	}
}

func TestRestoreRejectsCorruptionWithoutChangingLiveState(t *testing.T) {
	t.Parallel()
	sm, _ := activatedSM(t)
	before, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	cases := []func(*smState){
		func(s *smState) { s.SchemaVersion = 99 }, func(s *smState) { s.Rules.Limits.MaxNodes++ },
		func(s *smState) { s.Active.Version = s.Latest.Version + 1 }, func(s *smState) { s.Allocations = nil },
		func(s *smState) { s.Operators[0].Status.State = "unknown" }, func(s *smState) { s.Operators[0].Status.TotalSteps++ },
		func(s *smState) { s.Operators[0].Status.Attempt++ }, func(s *smState) { s.AppliedTerm = 0 },
		func(s *smState) { s.Sessions = []sessionState{{SessionID: "s", NodeName: "n", InstanceID: "i"}} },
		func(s *smState) { s.Locks = []lockEntry{{Key: "key", SessionID: "absent"}} },
		func(s *smState) { s.RetiredGroups = []retiredGroup{{ID: "shard0", Version: 1}} },
	}
	for i, mutate := range cases {
		var st smState
		if err := json.Unmarshal(before, &st); err != nil {
			t.Fatal(err)
		}
		mutate(&st)
		data, err := json.Marshal(st)
		if err != nil {
			t.Fatal(err)
		}
		if err := sm.Restore(data); err == nil {
			t.Fatalf("corruption %d accepted", i)
		}
		after, _ := sm.Snapshot()
		if !bytes.Equal(before, after) {
			t.Fatalf("corruption %d mutated live state", i)
		}
	}
}

func TestSessionIdentityCannotBeRebound(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	p := GrantSession{SessionID: "s", NodeName: "n", InstanceID: "i", TTLSeconds: 10}
	apply(t, sm, 1, p)
	for i, mutate := range []func(*GrantSession){func(p *GrantSession) { p.NodeName = "other" }, func(p *GrantSession) { p.InstanceID = "other" }, func(p *GrantSession) { p.TTLSeconds++ }} {
		bad := p
		mutate(&bad)
		if r := apply(t, sm, uint64(i+2), bad); r.Accepted {
			t.Fatal("session rebound", r)
		}
	}
}

func TestReadmissionAllocatorOverflow(t *testing.T) {
	t.Parallel()
	d := baseDoc()
	d.MetaGroup().NextReplicaID = math.MaxUint64
	if _, err := buildReadmissionDocument(d, "n1", time.Unix(1, 0)); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflow accepted", err)
	}
}

type failingPersister struct{ err error }

func (p failingPersister) SaveState([]byte) error { return p.err }

func TestDurabilityFailureIsFatal(t *testing.T) {
	t.Parallel()
	boom := errors.New("disk failed")
	sm := newTopologySM(failingPersister{boom}, testRules())
	_, err := sm.Apply(1, 1, mustEncode(t, ProposeDocument{Doc: baseDoc()}))
	if !errors.Is(err, errDurablePersist) || !errors.Is(err, boom) {
		t.Fatal("durability failure was hidden", err)
	}
}
