//go:build functional

package metaservice

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRollbackUnfinishedReadmissionRefusesRetiredIdentity(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive,
		"n3": topology.HealthDead}
	active := f.g.machine.Active()
	result, err := f.g.ReadmitNode(t.Context(), "n3", preview())
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, result.Document, false)
	observed := make(map[string]topology.GroupStatus)
	for _, group := range active.Groups {
		observed[group.ID] = topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: 1,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true,
			LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	}
	desired := f.g.machine.Latest()
	plan, err := topology.BuildPlan(active, desired, observed, f.g.rules)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	op := f.command(t, metaengine.BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at}).OperatorID
	first := plan.Steps[0]
	f.command(t, metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: op, Version: desired.Version, Attempt: 1, StepSeq: first.Seq,
		GroupID: first.GroupID, NodeName: first.NodeName, State: topology.OperatorFailed,
		Error: "readmission did not start", At: at.Add(time.Second)}})
	_, err = f.g.RollbackTo(t.Context(), active.Version, preview())
	if !errors.Is(err, fault.ErrInvalid) || !strings.Contains(err.Error(), "retired replica identity") {
		t.Fatal("rollback reused an identity retired by approved readmission", err)
	}
	// Raw staging has the same allocation guard, even before any readmission
	// effect has been consumed. Native presence does not undo approved retirement.
	candidate := active.Clone()
	for i := range candidate.Groups {
		candidate.Groups[i].NextReplicaID = desired.Group(candidate.Groups[i].ID).NextReplicaID
	}
	data, err := metaengine.Encode(metaengine.ProposeDocument{Doc: candidate, ExpectedVersion: desired.Version})
	if err != nil {
		t.Fatal(err)
	}
	f.index++
	applied, err := f.g.machine.Apply(t.Context(), raftengine.Command{Index: f.index, Term: 1, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	var rejected metaengine.ApplyResult
	if err := json.Unmarshal(applied.Data, &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Accepted || !strings.Contains(rejected.Reason, "retired replica identity") {
		t.Fatal("raw staging bypassed the approved identity floor", rejected)
	}
	target, current := f.g.machine.Target(), f.g.machine.Active()
	if f.g.machine.Latest().Version != desired.Version || target.MemberID("n3") != 4 || current.MemberID("n3") != 3 {
		t.Fatal("refused rollback mutated approved or converged identities")
	}
}
