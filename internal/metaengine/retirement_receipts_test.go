//go:build !functional

package metaengine

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRetirementReceiptsPreventResurrectionAfterOperatorPruning(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	index := uint64(6)
	at := time.Unix(300, 0).UTC()
	command := func(p any) ApplyResult {
		t.Helper()
		r := apply(t, m.topologySM, index, p)
		index++
		if !r.Accepted {
			t.Fatalf("%T: %+v", p, r)
		}
		return r
	}
	desired := m.Active()
	desired.Group("shard0").Replicas = slices.DeleteFunc(desired.Group("shard0").Replicas, func(r topology.Replica) bool { return r.NodeName == "n3" })
	command(ProposeDocument{Doc: desired, ExpectedVersion: 1})
	desired = m.Latest()
	plan := topology.Plan{Version: desired.Version, Steps: []topology.Step{
		{Seq: 1, GroupID: "shard0", NodeName: "n3", ReplicaID: 3, Kind: topology.StepRemoveReplica},
		{Seq: 2, GroupID: "shard0", NodeName: "n3", ReplicaID: 3, Kind: topology.StepStopReplica},
		{Seq: 3, GroupID: "shard0", NodeName: "n3", ReplicaID: 3, Kind: topology.StepCleanupReplicaState},
	}}
	plan = withTestVoterProof(m.Active(), desired, plan)
	old := command(BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at}).OperatorID
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, old, plan, step.Seq, topology.OperatorRunning, at)
		index++
		index = consumeTestStep(t, m.topologySM, old, index)
	}
	applyAdmissionTransition(t, m.topologySM, index, old, plan, len(plan.Steps), topology.OperatorSucceeded, at)
	index++
	command(ActivateTargetProposal{OperatorID: old, Version: desired.Version, Attempt: 1})
	for range historyRetention + 3 {
		command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
		desired = m.Latest()
	}
	next := command(BeginOperatorProposal{Version: desired.Version, Plan: topology.Plan{Version: desired.Version}, At: at}).OperatorID
	command(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: next, Version: desired.Version, Attempt: 1, State: topology.OperatorSucceeded, At: at}})
	command(ActivateTargetProposal{OperatorID: next, Version: desired.Version, Attempt: 1})
	command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
	if m.operators[old] != nil {
		t.Fatal("settled operator cannot leave history")
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	base, err := m.DeletionBase(t.Context(), m.Latest())
	if err != nil || documentReplica(base, "shard0", 3) != nil {
		t.Fatal("retained grant resurrected collected replica after operator pruning", base, err)
	}
	statuses := map[string]topology.GroupStatus{
		topology.MetaGroupID: {GroupID: topology.MetaGroupID, Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}},
		"shard0":             {GroupID: "shard0", Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
	}
	plan, err = topology.BuildPlan(base, m.Latest(), statuses, m.rules)
	if err != nil || !plan.IsEmpty() {
		t.Fatal("settled topology requires repeated historical cleanup", plan, err)
	}
}
