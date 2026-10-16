//go:build !functional

package operator

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRemainingScopeUsesOriginalProofAndGlobalCursor(t *testing.T) {
	t.Parallel()
	old := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1, State: topology.OperationFailed,
		Blocks: []topology.BlockStatus{{GroupID: "other", State: topology.OperationSucceeded, CurrentStep: 1}, {GroupID: "work", State: topology.OperationFailed, CurrentStep: 3}}},
		Plan: topology.Plan{Version: 2, Steps: []Step{
			{Seq: 1, GroupID: "other", Kind: StepTransferLeader, NodeName: "c", ReplicaID: 1},
			{Seq: 2, GroupID: "work", Kind: StepWaitCaughtUp, NodeName: "b", ReplicaID: 2},
			{Seq: 3, GroupID: "work", Kind: StepPromoteVoter, NodeName: "b", ReplicaID: 2, ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})},
			{Seq: 4, GroupID: "work", Kind: StepStopReplica, NodeName: "retired", ReplicaID: 9}},
			Blocks: []topology.PlanBlock{{GroupID: "other", Steps: []int{1}}, {GroupID: "work", Steps: []int{2, 3}, Cleanup: []int{4}}}}}
	op := old.Clone()
	op.Previous = old.Status.ID
	op.Status.ID = "exec-v2-a2"
	op.Status.Attempt = 2
	op.Status.State = topology.OperationPending
	op.Status.Blocks[1].State = topology.OperationPending
	step := old.Plan.Steps[2]
	action := topology.AuthorizedStep{OperatorID: topology.OperatorID(old.Status.ID), Version: 2, Attempt: 1, StepSeq: 3, Kind: step.Kind, GroupID: "work", NodeName: "b", ReplicaID: 2, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}
	member := topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action, Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{member}}
	receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Target: member, Action: &action, Outcome: topology.EffectApplied, AppliedIndex: 11, AppliedTerm: 1}
	target := Document{Version: 2, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "b", ReplicaID: 2, Role: topology.ReplicaVoter}}}, {ID: "other"}}}
	native := topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 11, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}, Grant: &grant, Receipt: &receipt}
	view := RemainingPlanView{Target: target, Input: topology.PlanInput{Native: map[string]topology.NativeState{"work": native}}, Previous: []topology.Operation{old}, Grants: []topology.AuthorityGrant{grant}, Consumed: []int{1, 2}}
	rules := Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}
	if err := ValidateRemainingScope(view, op, "work", rules); err != nil {
		t.Fatal("satisfied original receipt did not permit new-grant execution", err)
	}
	if len(view.Consumed) != 2 || view.Grants[0].Action.Attempt != 1 {
		t.Fatal("simulation fabricated consumption")
	}
	view.Input.Native["work"] = topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 11, ConfState: native.ConfState}
	if err := ValidateRemainingScope(view, op, "work", rules); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("native state alone authorized changed predecessor", err)
	}
	view.Receipts = []topology.EffectReceipt{receipt}
	if err := ValidateRemainingScope(view, op, "work", rules); err != nil {
		t.Fatal(err)
	}
	view.Consumed = append(view.Consumed, 3)
	if err := ValidateRemainingScope(view, op, "work", rules); err != nil {
		t.Fatal("consumed current step was replayed", err)
	}
	view.Previous = nil
	if err := ValidateRemainingScope(view, op, "work", rules); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("missing ancestry accepted", err)
	}
}
