package reconcile

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

const reconcileTestStability = 60 * time.Second

func TestRestoreDegradedPolicyAllowList(t *testing.T) {
	t.Parallel()
	doc := clusterDocument(true)
	matching := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	missing := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}
	asLearner := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{
		Voters: []uint64{1, 2}, Learners: []uint64{3},
	}}

	tests := []struct {
		name       string
		mutate     func(*ReconcileInput)
		want       *ReconcileAction
		wantReason string
	}{
		{name: "matching membership", wantReason: ReconcileReasonConverged},
		{
			name: "stable required voter absent",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = missing
			},
			want: &ReconcileAction{Kind: ActionRepairReplica, GroupID: "shard0", NodeName: "n3", ReplicaID: 3, Reason: ReconcileReasonMissingReplica},
		},
		{
			name: "missing voter node is not stable",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = missing
				in.Alive["n3"] = reconcileTestStability - time.Second
			},
			wantReason: ReconcileReasonStabilityWindow,
		},
		{
			name: "quorum lost group",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = missing
				in.Health["shard0"] = topology.GroupHealthEvaluation{GroupID: "shard0", ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupQuorumLost}}
			},
			wantReason: ReconcileReasonQuorumLost,
		},
		{
			name:   "voter identity already present as learner",
			mutate: func(in *ReconcileInput) { in.Status["shard0"] = asLearner },
		},
		{
			name: "intentional Active learner is never promoted",
			mutate: func(in *ReconcileInput) {
				in.Active.Group("shard0").Replicas[2].Role = topology.ReplicaLearner
				in.Status["shard0"] = missing
			},
		},
		{
			name: "missing voter belongs to draining node",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = missing
				in.Active.Node("n3").State = topology.NodeDraining
			},
		},
		{
			name: "deleted group",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = missing
				in.Active.Groups = in.Active.Groups[:1]
			},
		},
		{
			name:   "group status unavailable",
			mutate: func(in *ReconcileInput) { delete(in.Status, "shard0") },
		},
		{
			name: "surplus live voter does not authorize removal",
			mutate: func(in *ReconcileInput) {
				in.Status["shard0"] = topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3, 99}}}
			},
		},
	}

	policy := NewRestoreDegradedPolicy(reconcileTestStability)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := ReconcileInput{
				Latest: doc.Clone(), Target: doc.Clone(), Active: doc.Clone(),
				Status: map[string]topology.GroupStatus{"shard0": matching},
				Health: map[string]topology.GroupHealthEvaluation{"shard0": {GroupID: "shard0"}},
				Alive: map[string]time.Duration{
					"n1": 2 * reconcileTestStability,
					"n2": 2 * reconcileTestStability,
					"n3": 2 * reconcileTestStability,
				},
			}
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			got := policy.Evaluate(in)
			if tc.want == nil {
				if got.Action != nil {
					t.Fatalf("action = %+v, want none", got.Action)
				}
				if tc.wantReason != "" && got.Reason != tc.wantReason {
					t.Fatalf("reason = %q, want %q", got.Reason, tc.wantReason)
				}
				return
			}
			if got.Action == nil || *got.Action != *tc.want {
				t.Fatalf("action = %+v, want %+v", got.Action, *tc.want)
			}
			if got.Reason != ReconcileReasonMissingReplica {
				t.Fatalf("decision reason = %q, want %q", got.Reason, ReconcileReasonMissingReplica)
			}
		})
	}
}

func TestRestoreDegradedPolicyNeverRepairsMeta(t *testing.T) {
	t.Parallel()
	doc := clusterDocument(false)
	decision := NewRestoreDegradedPolicy(time.Second).Evaluate(ReconcileInput{
		Active: doc,
		Status: map[string]topology.GroupStatus{topology.MetaGroupID: {
			GroupID: topology.MetaGroupID, Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
		}},
		Alive: map[string]time.Duration{"n3": time.Hour},
	})
	if decision.Action != nil {
		t.Fatalf("meta action = %+v, want none; meta repair uses admission/readmission", decision.Action)
	}
}
