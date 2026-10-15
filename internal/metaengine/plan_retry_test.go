//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestFailedAttemptCannotReplaceItsApprovedPlan(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	at := time.Now().UTC()
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0})
	first := apply(t, sm, 2, beginProposal(1, 1, "first", at))
	if !first.Accepted {
		t.Fatal(first)
	}
	failed := apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: first.OperatorID, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorFailed, GroupID: "shard0", NodeName: "n1", Error: "replan_required", At: at}})
	if !failed.Accepted {
		t.Fatal(failed)
	}
	changed := apply(t, sm, 4, beginProposal(1, 0, "changed", at))
	if changed.Accepted || changed.Code != proposalAuthorization {
		t.Fatal("failed retry rewrote plan", changed)
	}
	retry := apply(t, sm, 5, beginProposal(1, 1, "retry", at))
	if !retry.Accepted || retry.OperatorID != topology.MakeOperatorID(1, 2) {
		t.Fatal(retry)
	}
	if !reflect.DeepEqual(sm.operators[first.OperatorID].Steps, sm.operators[retry.OperatorID].Steps) {
		t.Fatal("retry changed immutable actions")
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	var snapshot smState
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.Operators {
		if snapshot.Operators[i].Status.Attempt == 2 {
			snapshot.Operators[i].Plan.Steps = nil
			snapshot.Operators[i].Status.TotalSteps = 0
		}
	}
	corrupt, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(corrupt); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("restored divergent retry plan", err)
	}
}

func TestImmutableRetryRetainsAlreadyCleanedPlanIdentities(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	base := baseDoc()
	base.Version = 1
	sm.active = base.Clone()
	sm.target = base.Clone()
	sm.latest = base.Clone()
	sm.target.Version = 2
	sm.latest.Version = 2
	sm.rememberAllocations(sm.latest)
	steps := []topology.Step{
		{Seq: 1, GroupID: "shard0", NodeName: "former", ReplicaID: 2, Kind: topology.StepRemoveReplica},
		{Seq: 2, GroupID: "shard0", NodeName: "former", ReplicaID: 2, Kind: topology.StepStopReplica},
		{Seq: 3, GroupID: "shard0", NodeName: "former", ReplicaID: 2, Kind: topology.StepCleanupReplicaState},
	}
	formerBase := base.Clone()
	formerBase.Group("shard0").Replicas = append(formerBase.Group("shard0").Replicas,
		topology.Replica{NodeName: "former", ReplicaID: 2, Role: topology.ReplicaLearner})
	guarded := withTestVoterProof(formerBase, sm.latest, topology.Plan{Version: 2, Steps: steps})
	at := time.Now().UTC()
	cleanedPlan := guarded.Clone()
	cleanedPlan.Version = 1
	cleaned := newOperatorRecordWithPlan(cleanedPlan, 1, operatorAudit{"actor", topology.OriginOperator, at})
	cleaned.State, cleaned.CurrentStep = topology.OperatorSucceeded, 3
	old := newOperatorRecordWithPlan(guarded, 1, operatorAudit{"actor", topology.OriginOperator, at})
	old.State, old.CurrentStep = topology.OperatorFailed, 1
	sm.operators[cleaned.ID], sm.operators[old.ID] = &cleaned, &old
	planning, err := sm.DeletionBase(t.Context(), sm.latest)
	if err != nil || len(planning.Group("shard0").Replicas) != 1 {
		t.Fatal(planning, err)
	}
	retry := apply(t, sm, 1, BeginOperatorProposal{Version: 2, Plan: guarded, At: at})
	if !retry.Accepted {
		t.Fatal("evolving cleanup catalog invalidated immutable retry", retry)
	}
	execution, err := sm.OperatorBase(t.Context(), retry.OperatorID)
	if err != nil || len(execution.Group("shard0").Replicas) != 2 {
		t.Fatal("retry lost exact retained identities", execution, err)
	}
}

func TestOperatorPlanProofSurvivesSnapshotAndRetry(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	at := time.Now().UTC()
	base := baseDoc()
	base.Version = 1
	sm.active, sm.target, sm.latest = base.Clone(), base.Clone(), base.Clone()
	sm.rememberAllocations(base)
	next := base.Clone()
	next.Nodes = append(next.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	next.MetaGroup().Replicas = append(next.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	next.MetaGroup().NextReplicaID = 3
	if result := apply(t, sm, 1, ProposeDocument{Doc: next, ExpectedVersion: 1}); !result.Accepted {
		t.Fatal(result)
	}
	plan, err := topology.BuildPlan(testPlanningInput(t, sm, sm.Latest(), assumedReadmissionStatuses(base, "")), sm.Latest(), testRules())
	if err != nil || len(plan.VoterBases) != 1 || len(plan.VoterBases[0].Voters) != 1 {
		t.Fatal("failed to build guarded learner plan", plan, err)
	}
	result := apply(t, sm, 2, BeginOperatorProposal{Version: 2, Plan: plan, Actor: "n1", At: at})
	if !result.Accepted {
		t.Fatal(result)
	}
	plan.VoterBases[0].Voters[0] = 99
	if got := operatorRecordView(*sm.operators[result.OperatorID]).Plan; got.VoterBases[0].Voters[0] != 1 {
		t.Fatalf("proposal mutated retained plan: %+v", got)
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var damaged smState
	if err := json.Unmarshal(data, &damaged); err != nil {
		t.Fatal(err)
	}
	for i := range damaged.Operators {
		for j := range damaged.Operators[i].Plan.Steps {
			step := &damaged.Operators[i].Plan.Steps[j]
			if step.ResultVotersDigest != "" {
				step.ResultVotersDigest = topology.VoterSetDigest([]uint64{99})
				break
			}
		}
	}
	corruptProof, err := json.Marshal(damaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := newTopologySM(nil, testRules()).Restore(corruptProof); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatalf("restored altered voter proof: %v", err)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	want := operatorRecordView(*sm.operators[result.OperatorID]).Plan
	got := operatorRecordView(*restored.operators[result.OperatorID]).Plan
	if !got.Equal(want) {
		t.Fatalf("snapshot lost full plan: got %+v, want %+v", got, want)
	}
	got.VoterBases[0].Voters[0] = 88
	if restored.operators[result.OperatorID].VoterBases[0].Voters[0] != 1 {
		t.Fatal("public operator view aliases retained plan")
	}
	failed := apply(t, restored, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: result.OperatorID, Version: 2, Attempt: 1, StepSeq: 1, State: topology.OperatorFailed, GroupID: topology.MetaGroupID, NodeName: "n2", Error: "retry", At: at}})
	if !failed.Accepted {
		t.Fatal(failed)
	}
	altered := want.Clone()
	altered.VoterBases[0].Voters[0] = 2
	if retry := apply(t, restored, 4, BeginOperatorProposal{Version: 2, Plan: altered, Actor: "n1", At: at}); retry.Accepted {
		t.Fatal("retry accepted changed voter base")
	}
	if retry := apply(t, restored, 5, BeginOperatorProposal{Version: 2, Plan: want, Actor: "n1", At: at}); !retry.Accepted {
		t.Fatal("retry rejected immutable plan", retry)
	}
}
