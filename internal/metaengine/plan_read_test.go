//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestPlanReadRetainsCleanedIdentityWithoutInventingConvergence(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	m.topologySM = planningInputFixture()
	plan := topology.Plan{Version: m.target.Version, Steps: []topology.Step{
		{Seq: 1, Kind: topology.StepRemoveReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 2},
		{Seq: 2, Kind: topology.StepStopReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 2},
		{Seq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "shard0", NodeName: "n1", ReplicaID: 2},
	}}
	old := newOperatorRecordWithPlan(plan, 1, operatorAudit{"n1", topology.OriginOperator, time.Now()})
	old.State = topology.OperatorFailed
	retry := newOperatorRecordWithPlan(plan, 2, operatorAudit{"n1", topology.OriginOperator, time.Now()})
	m.operators[old.ID], m.operators[retry.ID] = &old, &retry
	record := planningGrant(m.topologySM, 2, topology.StepCleanupReplicaState)
	record.Grant.Purpose, record.Grant.OperatorID = topology.AuthorityEffect, old.ID
	record.Grant.Action.OperatorID, record.Grant.Action.Version = old.ID, plan.Version
	record.Grant.Action.Attempt, record.Grant.Action.StepSeq = 1, 3
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}
	record.Grant.Resources = []topology.AuthorityResource{resource}
	record.Effects = []topology.EffectReceipt{{Key: record.Grant.Key, Resource: resource,
		Action: record.Grant.Action, Target: record.Grant.Targets[0], Outcome: topology.EffectApplied}}
	data, err := json.Marshal(QueryRequest{Kind: "plan_validation", OperatorID: retry.ID})
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	var read PlanRead
	if err := json.Unmarshal(result.Data, &read); err != nil {
		t.Fatal(err)
	}
	retained := read.Input.Retained["shard0"].Replicas
	if len(retained) != 1 || retained[0].ReplicaID != 2 || retained[0].Progress != topology.RetainedStopped ||
		!read.Operator.Plan.Equal(plan) || read.Input.Groups["shard0"].ConvergedVersion != 1 ||
		read.Input.Groups["shard0"].Converged.Replicas[0].ReplicaID != 1 {
		t.Fatal("retry lost original identity or invented convergence", read)
	}
	read.Input.Retained["shard0"].Replicas[0].ReplicaID = 99
	read.Input.Groups["shard0"].Converged.Replicas[0].ReplicaID = 99
	if record.Grant.Targets[0].ReplicaID != 2 || m.active.Group("shard0").Replicas[0].ReplicaID != 1 {
		t.Fatal("query result aliases metadata")
	}
}
