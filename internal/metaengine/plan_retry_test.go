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
	at := time.Now().UTC()
	cleaned := newOperatorRecord(1, 1, steps, "actor", topology.OriginOperator, at)
	cleaned.State, cleaned.CurrentStep = topology.OperatorSucceeded, 3
	old := newOperatorRecord(2, 1, steps, "actor", topology.OriginOperator, at)
	old.State, old.CurrentStep = topology.OperatorFailed, 1
	sm.operators[cleaned.ID], sm.operators[old.ID] = &cleaned, &old
	planning, err := sm.DeletionBase(t.Context(), sm.latest)
	if err != nil || len(planning.Group("shard0").Replicas) != 1 {
		t.Fatal(planning, err)
	}
	retry := apply(t, sm, 1, BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2, Steps: steps}, At: at})
	if !retry.Accepted {
		t.Fatal("evolving cleanup catalog invalidated immutable retry", retry)
	}
	execution, err := sm.OperatorBase(t.Context(), retry.OperatorID)
	if err != nil || len(execution.Group("shard0").Replicas) != 2 {
		t.Fatal("retry lost exact retained identities", execution, err)
	}
}
