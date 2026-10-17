//go:build !functional

package operator

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRemainingMetaReplacementAllowsOnlyExactDecommissionSurge(t *testing.T) {
	t.Parallel()
	base := Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding, CreatedAt: time.Now().UTC(),
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Groups: []Group{{ID: MetaGroupID, NextReplicaID: 4, Replicas: []Replica{
			{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter},
			{NodeName: "b", ReplicaID: 2, Role: topology.ReplicaVoter},
			{NodeName: "c", ReplicaID: 3, Role: topology.ReplicaVoter}}}}}
	for _, name := range []string{"a", "b", "c"} {
		base.Nodes = append(base.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
	}
	target := base.Clone()
	target.Version = 2
	target.MetaGroup().NextReplicaID = 5
	target.Node("c").State = topology.NodeExcluded
	target.Nodes = append(target.Nodes, topology.Node{Name: "d", Type: "worker", State: topology.NodeActive})
	target.MetaGroup().Replicas[2] = Replica{NodeName: "d", ReplicaID: 4, Role: topology.ReplicaVoter}
	view := RemainingPlanView{Input: remainingInput(base), Target: target}
	rules := Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}.WithDefaults()}
	native := map[string]topology.NativeState{MetaGroupID: {GroupID: MetaGroupID, ReplicaID: 1,
		Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	view.Input.Native = native
	row := view.Input.Groups[MetaGroupID]
	row.Observation = &topology.GroupStatus{GroupID: MetaGroupID, Found: true, LeaderID: 1,
		ConfState: native[MetaGroupID].ConfState, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	view.Input.Groups[MetaGroupID] = row
	plan, err := topology.BuildPlan(view.Input, target, rules)
	if err != nil {
		t.Fatal(err)
	}
	ex := Record{Version: 2, Steps: plan.Steps, VoterBases: plan.VoterBases}
	if err := ex.validateScope(view, native, "_meta", rules); err != nil {
		t.Fatal("approved learner-first replacement refused", err)
	}
	state := native[MetaGroupID]
	state.ConfState.Learners = []uint64{4}
	native[MetaGroupID], ex.CurrentStep = state, 3
	if err := ex.validateScope(view, native, "_meta", rules); err != nil {
		t.Fatal("resumed native surge refused", err)
	}
	state.ConfState.Voters = []uint64{1, 2}
	native[MetaGroupID], ex.CurrentStep = state, 5
	if err := ex.validateScope(view, native, "_meta", rules); err != nil {
		t.Fatal("resumed removal did not contract the allowance", err)
	}
	v := remainingPlanValidation{view: view, ex: ex, rules: rules}
	s := remainingGroupSimulation{group: *target.MetaGroup(), voters: remainingMembers([]uint64{1, 2, 3}),
		learners: remainingMembers([]uint64{4, 5})}
	if err := v.validateNativeCapacity(&s, ex.Steps[1]); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("one decommission allowed two extra members", err)
	}
	delete(s.voters, 3)
	if err := v.validateNativeCapacity(&s, ex.Steps[1]); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("removed member kept its surge allowance", err)
	}
	s.voters[3], s.learners = true, remainingMembers([]uint64{4})
	v.ex = Record{}
	if err := v.validateNativeCapacity(&s, Step{}); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("unplanned removal supplied capacity", err)
	}
	v.ex = ex
	v.view.Target.MetaGroup().Replicas[2].NodeName = "c"
	if err := v.validateNativeCapacity(&s, ex.Steps[1]); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("same-node readmission obtained decommission allowance", err)
	}
}
