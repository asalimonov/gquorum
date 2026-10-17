//go:build !functional

package topology

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestVoterReplacementOrderingAndGuard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		surge, hardMax int
		unknown, lost  bool
		first          StepKind
	}{
		{name: "surge", surge: 1, hardMax: 5, first: StepWaitCaughtUp},
		{name: "no surge", surge: 0, hardMax: 5, first: StepWaitCaughtUp},
		{name: "hard maximum", surge: 1, hardMax: 3, first: StepWaitCaughtUp},
		{name: "unknown", surge: 1, hardMax: 5, unknown: true},
		{name: "quorum lost", surge: 1, hardMax: 5, lost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			active := planDoc(1, tc.hardMax, []string{"n1", "n2", "n3", "n4"}, shardOf("work", rep("n1", 1), rep("n2", 2), rep("n3", 3), learner("n4", 4)))
			p := DefaultVoterConfig()
			p.MaxSurgeVoters = tc.surge
			active.Group("work").VoterConfig = &p
			active.Group("work").Hosting = &HostingRule{Mode: HostingAllEligible}
			desired := active.Clone()
			desired.Version++
			desired.Group("work").Replicas[2].Role = ReplicaLearner
			desired.Group("work").Replicas[3].Role = ReplicaVoter
			statuses := statusForDocument(active)
			st := statuses["work"]
			st.HealthKnown = !tc.unknown
			st.LiveVoters = map[uint64]bool{1: true, 2: !tc.lost, 3: false, 4: true}
			statuses["work"] = st
			rules := Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 5, MaxReplicas: 5}}
			plan, err := BuildPlan(testPlanInput(active, statuses), desired, rules)
			if tc.unknown || tc.lost {
				if err == nil {
					t.Fatal("unsafe replacement accepted", plan)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if plan.Steps[0].Kind != tc.first {
				t.Fatal("wrong replacement order", plan)
			}
			for _, s := range plan.Steps {
				if s.Kind == StepDemoteVoter && (s.TransferReplicaID != 1 || s.TransferNodeName != "n1") {
					t.Fatal("demotion lacks exact retained transfer guard", s)
				}
			}
		})
	}
}

func TestDemotionGuardIsPartOfExactApproval(t *testing.T) {
	t.Parallel()
	step := Step{Seq: 1, Kind: StepDemoteVoter, GroupID: "work", NodeName: "n3", ReplicaID: 3, TransferNodeName: "n1", TransferReplicaID: 1}
	a := AuthorizedStep{OperationID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 1, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID}
	if !a.Matches(a.OperationID, a.Attempt, a.Version, step) {
		t.Fatal("exact guard rejected")
	}
	a.TransferReplicaID = 2
	if a.Matches(a.OperationID, a.Attempt, a.Version, step) {
		t.Fatal("changed destination accepted")
	}
	a.TransferReplicaID, a.TransferNodeName = 1, "successor"
	if a.Matches(a.OperationID, a.Attempt, a.Version, step) {
		t.Fatal("changed destination name accepted")
	}
	active := planDoc(1, 5, []string{"n1", "n2", "n3"}, shardOf("work", rep("n1", 1), rep("n2", 2), rep("n3", 3)))
	desired := active.Clone()
	desired.Version++
	desired.Group("work").Replicas[2].Role = ReplicaLearner
	plan := Plan{Version: desired.Version, Steps: []Step{step}}
	if err := bindVoterGuards(&plan, map[string]GroupStatus{"work": groupStatus("work", 1, []uint64{1, 2, 3}, nil)}, desired); err != nil {
		t.Fatal(err)
	}
	bindPlanBlocks(convergedPlanInput(active), desired, &plan, []string{"work"})
	if err := ValidatePlanShape(convergedPlanInput(active), desired, plan); err != nil {
		t.Fatal(err)
	}
	plan.Steps[0].TransferReplicaID = 3
	if err := ValidatePlanShape(convergedPlanInput(active), desired, plan); err == nil {
		t.Fatal("guard accepted learner or mismatched identity")
	}
	_, err := BuildPlan(testPlanInput(active, map[string]GroupStatus{}), desired, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}})
	if !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
}
