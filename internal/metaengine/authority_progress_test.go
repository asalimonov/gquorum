//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestOperatorProgressRequiresEveryResourceReceipt(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	sm := m.topologySM
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc()})
	at := time.Unix(100, 0).UTC()
	step := topology.Step{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "shard0"}
	plan := withTestVoterProof(sm.Active(), sm.Latest(), topology.Plan{Version: 1, Steps: []topology.Step{step}})
	step = plan.Steps[0]
	begin := apply(t, sm, 2, BeginOperatorProposal{Version: 1, Plan: plan, At: at})
	if !begin.Accepted {
		t.Fatal(begin)
	}
	index := uint64(3)
	command := func(p any) ApplyResult { t.Helper(); r := apply(t, sm, index, p); index++; return r }
	transition := topology.OperatorTransition{OperatorID: begin.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, State: topology.OperatorSucceeded, At: at}
	pending := func() {
		t.Helper()
		r := command(TransitionOperatorProposal{Transition: transition})
		if r.Accepted || r.Code != proposalAuthorityPending {
			t.Fatal("advanced without consumption", r)
		}
	}
	pending() // Pending -> succeeded must not skip the only effect.
	transition.State = topology.OperatorRunning
	if r := command(TransitionOperatorProposal{Transition: transition}); !r.Accepted {
		t.Fatal(r)
	}
	transition.State = topology.OperatorSucceeded
	pending()
	a := topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, Kind: step.Kind, GroupID: step.GroupID, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	granted := command(BeginEffectProposal{Action: a})
	if !granted.Accepted || granted.Grant == nil {
		t.Fatal(granted)
	}
	grant := *granted.Grant
	receipts := fixtureEffectReceipts(grant)
	if len(receipts) != 2 {
		t.Fatal("founding fixture must require group and host receipts", receipts)
	}
	pending()
	wrong := receipts[0].Clone()
	wrong.Key.Revision++
	if r := command(RecordEffectProposal{Receipt: wrong}); r.Accepted {
		t.Fatal("wrong grant accepted", r)
	}
	for i, r := range receipts {
		if result := command(RecordEffectProposal{Receipt: r}); !result.Accepted {
			t.Fatal(result)
		}
		if i < len(receipts)-1 {
			pending()
		}
	}
	query, err := json.Marshal(QueryRequest{Kind: "plan_validation", OperatorID: begin.OperatorID})
	if err != nil {
		t.Fatal(err)
	}
	read, err := m.Query(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	var view PlanRead
	if err := json.Unmarshal(read.Data, &view); err != nil || len(view.Grants) != 1 || !view.Grants[0].Equal(grant) || len(view.Receipts) != len(receipts) {
		t.Fatalf("plan validation lost exact receipt evidence: %+v %v", view, err)
	}
	if r := command(TransitionOperatorProposal{Transition: transition}); !r.Accepted {
		t.Fatal(r)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(testRules(), nil)
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	got, err := restored.EffectReceipts(t.Context(), grant)
	if err != nil || len(got) != len(receipts) {
		t.Fatal("lost consumed evidence", got, err)
	}
	for _, removeAll := range []bool{false, true} {
		var state smState
		if err = json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		if removeAll {
			state.AuthorityRecords = nil
			state.AuthorityFloors = nil
		} else {
			state.AuthorityRecords[0].Effects = state.AuthorityRecords[0].Effects[:1]
		}
		bad, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err = restored.Restore(t.Context(), bad); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal("success restored without complete receipts", err)
		}
		if rs, err := restored.EffectReceipts(t.Context(), grant); err != nil || len(rs) != len(receipts) {
			t.Fatal("failed restore mutated live proof", rs, err)
		}
	}
}

func TestNoopSuccessWaitsForPredecessorRevocation(t *testing.T) {
	t.Parallel()
	m, a := authorityFixture(t, 1024)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: a}).Grant
	doc := m.Latest()
	doc.Comment = "new intent"
	if r := apply(t, m.topologySM, 5, ProposeDocument{Doc: doc, ExpectedVersion: doc.Version}); !r.Accepted {
		t.Fatal(r)
	}
	at := time.Unix(300, 0).UTC()
	begin := apply(t, m.topologySM, 6, BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2}, At: at})
	if !begin.Accepted {
		t.Fatal(begin)
	}
	transition := TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: begin.OperatorID, Version: 2, Attempt: 1, State: topology.OperatorSucceeded, At: at}}
	if r := apply(t, m.topologySM, 7, transition); r.Accepted || r.Code != proposalAuthorityPending {
		t.Fatal("noop bypassed revocation", r)
	}
	obligations := m.PendingFences()
	if len(obligations) != 1 || obligations[0].Key.Revision <= grant.Key.Revision {
		t.Fatal(obligations)
	}
	fence, _ := m.AuthorityGrant(obligations[0].Key)
	if r := apply(t, m.topologySM, 8, RecordFenceProposal{Evidence: groupFenceProof(fence, 100)}); !r.Accepted {
		t.Fatal(r)
	}
	if r := apply(t, m.topologySM, 9, transition); !r.Accepted {
		t.Fatal("completed fence did not unblock noop", r)
	}
}
