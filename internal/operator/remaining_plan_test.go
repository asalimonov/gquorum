//go:build !functional

package operator

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRemainingPlanAgainstPostFenceMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		voters, learners []uint64
		steps            []Step
		want             []Replica
		cursor           int
		unavailable      bool
		expected         error
	}{
		{name: "late promotion missing demotion", voters: []uint64{1, 2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaLearner}}, expected: fault.ErrReplanRequired},
		{name: "late removal needs unplanned add", voters: []uint64{1}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepWaitCaughtUp, ReplicaID: 2}, {Kind: StepPromoteVoter, ReplicaID: 2}}, expected: fault.ErrReplanRequired},
		{name: "late learner can be reconciled", voters: []uint64{1}, learners: []uint64{2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepRemoveReplica, ReplicaID: 2}, {Kind: StepStopReplica, ReplicaID: 2}, {Kind: StepCleanupReplicaState, ReplicaID: 2}}},
		{name: "already promoted remains reachable", voters: []uint64{1, 2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepPrepareReplica, ReplicaID: 2}, {Kind: StepAddLearner, ReplicaID: 2}, {Kind: StepWaitCaughtUp, ReplicaID: 2}, {Kind: StepPromoteVoter, ReplicaID: 2}}},
		{name: "completed prefix cannot add again", voters: []uint64{1}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepAddLearner, ReplicaID: 2}, {Kind: StepWaitCaughtUp, ReplicaID: 2}, {Kind: StepPromoteVoter, ReplicaID: 2}}, cursor: 2, expected: fault.ErrReplanRequired},
		{name: "late removal invalidates destructive order", voters: []uint64{1, 2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepRemoveReplica, ReplicaID: 2}}, expected: fault.ErrReplanRequired},
		{name: "late promotion exhausts planned headroom", voters: []uint64{1, 2, 4}, learners: []uint64{3}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaVoter}, {ReplicaID: 3, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepPromoteVoter, ReplicaID: 3}, {Kind: StepRemoveReplica, ReplicaID: 4}}, expected: fault.ErrReplanRequired},
		{name: "demotion lost transfer prerequisite", voters: []uint64{1, 2, 3}, learners: []uint64{4}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}, {ReplicaID: 2, Role: topology.ReplicaVoter}, {ReplicaID: 3, Role: topology.ReplicaLearner}, {ReplicaID: 4, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepDemoteVoter, ReplicaID: 3, TransferReplicaID: 4}, {Kind: StepPromoteVoter, ReplicaID: 4}}, expected: fault.ErrReplanRequired},
		{name: "no-op plan still checks native state", voters: []uint64{1, 2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, expected: fault.ErrReplanRequired},
		{name: "unavailable is not contradiction", want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, unavailable: true, expected: fault.ErrUnavailable},
		{name: "unstarted founding remains possible", want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, unavailable: true, steps: []Step{{Kind: StepCreateFoundingGroup}}},
		{name: "changed founding must replan", voters: []uint64{1, 2}, want: []Replica{{ReplicaID: 1, Role: topology.ReplicaVoter}}, steps: []Step{{Kind: StepCreateFoundingGroup}}, expected: fault.ErrReplanRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := Document{Version: 2, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Groups: []Group{{ID: "work", Replicas: tc.want}}}
			ex := Record{Version: 2, CurrentStep: tc.cursor, Steps: tc.steps}
			for i := range ex.Steps {
				ex.Steps[i].Seq = i + 1
				ex.Steps[i].GroupID = "work"
			}
			native := map[string]topology.NativeState{}
			if !tc.unavailable {
				native["work"] = topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 10, ConfState: raftpb.ConfState{Voters: tc.voters, Learners: tc.learners}}
			}
			if err := ValidateRemainingPlan(RemainingPlanView{Target: target}, ex, native, Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}); !errors.Is(err, tc.expected) {
				t.Fatalf("got %v, want %v", err, tc.expected)
			}
		})
	}
}

func TestRemainingPlanRetainsHostOnlyCleanupObligations(t *testing.T) {
	t.Parallel()
	target := Document{Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	base := target.Clone()
	base.Groups[0].Replicas = append(base.Groups[0].Replicas, Replica{NodeName: "b", ReplicaID: 2, Role: topology.ReplicaLearner})
	native := map[string]topology.NativeState{"work": {GroupID: "work", ReplicaID: 1, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	ex := Record{}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal(err)
	}
	ex.Steps = []Step{{Seq: 1, GroupID: "work", Kind: StepRemoveReplica, NodeName: "b", ReplicaID: 2}, {Seq: 2, GroupID: "work", Kind: StepStopReplica, NodeName: "b", ReplicaID: 2}, {Seq: 3, GroupID: "work", Kind: StepCleanupReplicaState, NodeName: "b", ReplicaID: 2}}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}); err != nil {
		t.Fatal(err)
	}
	// Proven progress in this immutable plan can satisfy its earlier host steps.
	ex.CurrentStep = 3
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}); err != nil {
		t.Fatal(err)
	}
	ex.Steps[2].NodeName = "different-owner"
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal(err)
	}
}

func TestRemainingPlanRequiresCompleteReadmissionBoundary(t *testing.T) {
	t.Parallel()
	base := Document{Version: 1, Groups: []Group{
		{ID: MetaGroupID, Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "b", ReplicaID: 2, Role: topology.ReplicaVoter}, {NodeName: "c", ReplicaID: 3, Role: topology.ReplicaVoter}}},
		{ID: "deleted", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}}},
	}}
	target := base.Clone()
	target.Version = 2
	target.Groups = target.Groups[:1]
	target.MetaGroup().Replicas[0].ReplicaID = 4
	ex := Record{Version: 2, Steps: []Step{
		{Seq: 1, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 1, Kind: StepRemoveReplica},
		{Seq: 2, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepPrepareReplica},
		{Seq: 3, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepAddLearner},
		{Seq: 4, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepWaitCaughtUp},
		{Seq: 5, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepPromoteVoter},
	}}
	native := map[string]topology.NativeState{MetaGroupID: {GroupID: MetaGroupID, ReplicaID: 2, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	rules := Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); err != nil {
		t.Fatal(err)
	}
	ex.Steps[1].Kind = StepWaitCaughtUp
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatal("incomplete preparation hid retained hosts", err)
	}
}

func TestRemainingPlanRequiresExactReceiptForAppliedVoterChange(t *testing.T) {
	t.Parallel()
	base := Document{ClusterID: "cluster", Version: 1, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "b", ReplicaID: 2, Role: topology.ReplicaLearner}}}}}
	target := base.Clone()
	target.Version = 2
	target.Group("work").Replicas[1].Role = topology.ReplicaVoter
	id := topology.MakeOperatorID(2, 1)
	step := Step{Seq: 2, Kind: StepPromoteVoter, GroupID: "work", NodeName: "b", ReplicaID: 2,
		ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})}
	ex := Record{ID: id, Version: 2, Attempt: 1, CurrentStep: 2,
		Steps: []Step{{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "work", NodeName: "b", ReplicaID: 2}, step}}
	rules := Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}
	native := map[string]topology.NativeState{"work": {GroupID: "work", ReplicaID: 1, Applied: 10,
		ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); err != nil {
		t.Fatalf("matching predecessor cannot reach target: %v", err)
	}
	native["work"] = topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 11,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatalf("unexplained native promotion was reused: %v", err)
	}
	action := topology.AuthorizedStep{OperatorID: id, Version: 2, Attempt: 1, StepSeq: 2,
		Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID,
		ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}
	member := topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 10},
		Purpose: topology.AuthorityEffect, OperatorID: id, Action: &action,
		Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{member}}
	receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: &action,
		Target: member, Outcome: topology.EffectApplied, AppliedIndex: 11, AppliedTerm: 1}
	ex.Grants = []topology.AuthorityGrant{grant}
	nativeState := native["work"]
	nativeState.Grant, nativeState.Receipt = &grant, &receipt
	native["work"] = nativeState
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); err != nil {
		t.Fatalf("exact native receipt did not bridge metadata recording: %v", err)
	}
	stale := grant.Clone()
	stale.Key.Revision--
	nativeState.Grant = &stale
	native["work"] = nativeState
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatalf("stale native grant supplied completion proof: %v", err)
	}
	nativeState.Grant, nativeState.Receipt = nil, nil
	native["work"] = nativeState
	ex.Receipts = []topology.EffectReceipt{receipt}
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); err != nil {
		t.Fatalf("recorded metadata receipt was ignored: %v", err)
	}
	ex.Receipts[0].Action = nil
	if err := ValidateRemainingPlan(RemainingPlanView{Input: remainingInput(base), Target: target}, ex, native, rules); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatalf("stale or malformed receipt was accepted: %v", err)
	}
}

func TestRemainingPlanAcceptsExactApprovedFoundingSeedBeforeReceipt(t *testing.T) {
	t.Parallel()
	target := Document{ClusterID: "cluster", Version: 2, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	id := topology.MakeOperatorID(2, 1)
	step := Step{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "work",
		ExpectedVotersDigest: topology.VoterSetDigest(nil), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})}
	action := topology.AuthorizedStep{OperatorID: id, Version: 2, Attempt: 1, StepSeq: 1,
		Kind: step.Kind, GroupID: step.GroupID, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 10},
		Purpose: topology.AuthorityEffect, OperatorID: id, Action: &action,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "work"}, {Kind: topology.AuthorityHost, GroupID: "work", NodeName: "a"}},
		Targets:   []topology.AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 1}}}
	ex := Record{ID: id, Version: 2, Attempt: 1, CurrentStep: 1, Steps: []Step{step}, Grants: []topology.AuthorityGrant{grant}}
	state := topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 10,
		ConfState: raftpb.ConfState{Voters: []uint64{1}}, Grant: &grant}
	native := map[string]topology.NativeState{"work": state}
	rules := Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}
	if err := ValidateRemainingPlan(RemainingPlanView{Target: target}, ex, native, rules); err != nil {
		t.Fatalf("exact approved founding seed could not obtain receipt: %v", err)
	}
	state.Grant = nil
	native["work"] = state
	if err := ValidateRemainingPlan(RemainingPlanView{Target: target}, ex, native, rules); !errors.Is(err, fault.ErrReplanRequired) {
		t.Fatalf("unapproved founding seed was reused: %v", err)
	}
}

// Fixtures describe genuine whole-document convergence, without intermediate identities.
func remainingInput(doc Document) topology.PlanInput {
	input := topology.PlanInput{Approved: doc, Groups: make(map[string]topology.GroupProgress),
		Nodes: make(map[string]topology.NodeProgress)}
	for i := range doc.Groups {
		group := &doc.Groups[i]
		input.Groups[group.ID] = topology.GroupProgress{GroupID: group.ID, Converged: group,
			ConvergedVersion: doc.Version}
	}
	for i := range doc.Nodes {
		node := &doc.Nodes[i]
		input.Nodes[node.Name] = topology.NodeProgress{NodeName: node.Name, Converged: node,
			ConvergedVersion: doc.Version, MemberID: doc.MemberID(node.Name)}
	}
	return input
}
