//go:build !functional

package metaengine

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestIntermediateMetaOwnerSurvivesSupersessionPruningAndRetiresExactly(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	m.rules.Limits.MaxNodes, m.rules.Limits.MaxReplicas = 4, 4
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
	original := m.Active()
	joining := original.Clone()
	joining.Nodes = append(joining.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"route": "approved-four"}})
	joining.MetaGroup().Replicas = append(joining.MetaGroup().Replicas, topology.Replica{NodeName: "n4", ReplicaID: 4, Role: topology.ReplicaLearner})
	joining.MetaGroup().NextReplicaID = 5
	command(ProposeDocument{Doc: joining, ExpectedVersion: 1})
	joining = m.Latest()
	steps := []topology.Step{
		{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, Kind: topology.StepPrepareReplica},
		{Seq: 2, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, Kind: topology.StepAddLearner},
	}
	op := command(BeginOperatorProposal{Version: joining.Version, Plan: topology.Plan{Version: joining.Version, Steps: steps}, At: at}).OperatorID
	applyAdmissionTransition(t, m.topologySM, index, op, topology.Plan{Version: joining.Version, Steps: steps}, 1, topology.OperatorRunning, at)
	index++
	index = consumeTestStep(t, m.topologySM, op, index)
	// Model a durably prepared host that may have entered native membership
	// before its AddLearner acknowledgement reached the metadata controller.
	desired := original.Clone()
	desired.MetaGroup().NextReplicaID = 5
	command(ProposeDocument{Doc: desired, ExpectedVersion: joining.Version})
	desired = m.Latest()
	for range historyRetention + 3 {
		command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
		desired = m.Latest()
	}
	base, err := m.DeletionBase(t.Context(), desired)
	if err != nil || documentReplica(base, topology.MetaGroupID, 4) == nil || base.Node("n4") == nil || base.Node("n4").Labels["route"] != "approved-four" {
		t.Fatal("lost interrupted metadata owner or approved routing attributes", base, err)
	}
	base.Node("n4").Labels["route"] = "mutated"
	base, err = m.DeletionBase(t.Context(), desired)
	if err != nil || base.Node("n4").Labels["route"] != "approved-four" {
		t.Fatal("planning input aliases retained metadata", base, err)
	}
	statuses := map[string]topology.GroupStatus{}
	for _, group := range original.Groups {
		statuses[group.ID] = topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	}
	meta := statuses[topology.MetaGroupID]
	meta.ConfState.Learners = []uint64{4}
	statuses[topology.MetaGroupID] = meta
	plan, err := topology.BuildPlan(base, desired, statuses, m.rules)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepRemoveReplica || plan.Steps[1].Kind != topology.StepStopReplica || plan.Steps[0].NodeName != "n4" || plan.Steps[0].ReplicaID != 4 {
		t.Fatal("cannot reconcile historical native metadata learner", plan, err)
	}
	cleanup := command(BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at}).OperatorID
	if owner, established := m.MemberOwner(4); owner != "n4" || !established {
		t.Fatal("supersession hid an approved native participant", owner, established)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	m = restored
	if owner, _ := m.MemberOwner(4); owner != "n4" {
		t.Fatal("restore lost intermediate inbound routing", owner)
	}
	owners, _ := m.MemberOwners()
	delete(owners, 4)
	if owner, _ := m.MemberOwner(4); owner != "n4" {
		t.Fatal("routing map aliases FSM state")
	}
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, cleanup, plan, step.Seq, topology.OperatorRunning, at)
		index++
		index = consumeTestStep(t, m.topologySM, cleanup, index)
		if owner, _ := m.MemberOwner(4); owner != "" {
			t.Fatal("consumed native removal still authorizes inbound traffic", owner)
		}
		base, err = m.DeletionBase(t.Context(), desired)
		if err != nil || (documentReplica(base, topology.MetaGroupID, 4) != nil) != (step.Kind == topology.StepRemoveReplica) {
			t.Fatal("native absence discarded unfinished host retirement, or completed stop resurrected it", base, err)
		}
	}
	applyAdmissionTransition(t, m.topologySM, index, cleanup, plan, len(plan.Steps), topology.OperatorSucceeded, at)
	index++
	command(ActivateTargetProposal{OperatorID: cleanup, Version: desired.Version, Attempt: 1})
	for range historyRetention + 3 {
		command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
		desired = m.Latest()
	}
	base, err = m.DeletionBase(t.Context(), desired)
	if err != nil || len(base.MetaGroup().Replicas) != 3 || slices.ContainsFunc(base.Nodes, func(n topology.Node) bool { return n.Name == "n4" }) {
		t.Fatal("completed intermediate metadata retirement returned to planning", base, err)
	}
	data, err = m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if owner, _ := m.MemberOwner(4); owner != "" {
		t.Fatal("pruning/restore resurrected retired inbound identity", owner)
	}
}
