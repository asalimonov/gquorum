//go:build !functional

package scheduling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

const reconcileTestStability = 60 * time.Second

func TestDefaultRepairPolicyAllowList(t *testing.T) {
	t.Parallel()
	doc := clusterDocument(true)
	matching := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	missing := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}
	asLearner := topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{
		Voters: []uint64{1, 2}, Learners: []uint64{3},
	}}

	tests := []struct {
		name       string
		mutate     func(*ScheduleInput)
		want       *ScheduleAction
		wantReason string
	}{
		{name: "matching membership", wantReason: "converged"},
		{
			name: "stable required voter absent",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = missing
			},
			want: &ScheduleAction{Kind: RestoreMissingReplica, Identity: storage.Identity{ClusterID: "repair-test", GroupID: "shard0", NodeName: "n3", ReplicaID: 3}},
		},
		{
			name: "missing voter node is not stable",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = missing
				in.Alive["n3"] = reconcileTestStability - time.Second
			},
			wantReason: "stability_window",
		},
		{
			name: "quorum lost group",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = missing
				in.Health["shard0"] = topology.GroupHealthEvaluation{GroupID: "shard0", ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupQuorumLost}}
			},
			wantReason: "quorum_lost",
		},
		{
			name:   "voter identity already present as learner",
			mutate: func(in *ScheduleInput) { in.Status["shard0"] = asLearner },
		},
		{
			name: "intentional Active learner is never promoted",
			mutate: func(in *ScheduleInput) {
				in.Active.Group("shard0").Replicas[2].Role = topology.ReplicaLearner
				in.Status["shard0"] = missing
			},
		},
		{
			name: "missing voter belongs to draining node",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = missing
				in.Active.Node("n3").State = topology.NodeDraining
			},
		},
		{
			name: "deleted group",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = missing
				in.Active.Groups = in.Active.Groups[:1]
			},
		},
		{
			name:   "group status unavailable",
			mutate: func(in *ScheduleInput) { delete(in.Status, "shard0") },
		},
		{
			name: "surplus live voter does not authorize removal",
			mutate: func(in *ScheduleInput) {
				in.Status["shard0"] = topology.GroupStatus{GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3, 99}}}
			},
		},
	}

	scheduler, err := NewDefaultRepairPolicy(DefaultRepairOptions{StableFor: reconcileTestStability})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := ScheduleInput{
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
			got, err := scheduler.Schedule(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
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
			if got.Reason != "missing_replica" {
				t.Fatalf("decision reason = %q, want %q", got.Reason, "missing_replica")
			}
		})
	}
}

func TestDefaultRepairPolicyNeverRepairsUnconfiguredMeta(t *testing.T) {
	t.Parallel()
	doc := clusterDocument(false)
	scheduler, err := NewDefaultRepairPolicy(DefaultRepairOptions{StableFor: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := scheduler.Schedule(context.Background(), ScheduleInput{
		Active: doc,
		Status: map[string]topology.GroupStatus{topology.MetaGroupID: {
			GroupID: topology.MetaGroupID, Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
		}},
		Alive: map[string]time.Duration{"n3": time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != nil {
		t.Fatalf("meta action = %+v, want none; unconfigured meta repair uses admission/readmission", decision.Action)
	}
}

func TestDefaultRepairPolicyHoldAndReasonPriority(t *testing.T) {
	t.Parallel()
	p, err := NewDefaultRepairPolicy(DefaultRepairOptions{StableFor: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	doc := clusterDocument(true)
	second := doc.Clone().Groups[1]
	second.ID = "second"
	doc.Groups = append(doc.Groups, second)
	in := ScheduleInput{
		Active: doc,
		Status: map[string]topology.GroupStatus{
			"shard0": {GroupID: "shard0", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
			"second": {GroupID: "second", Found: true, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
		},
		Health: map[string]topology.GroupHealthEvaluation{"shard0": {ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupQuorumLost}}},
		Alive:  map[string]time.Duration{"n3": time.Second},
	}
	decision, err := p.Schedule(context.Background(), in)
	if err != nil || decision.Action == nil || decision.Action.Identity.GroupID != "second" {
		t.Fatalf("blocked group suppressed valid candidate: %+v/%v", decision, err)
	}
	in.Held = []storage.Identity{{ClusterID: doc.ClusterID, GroupID: "second", NodeName: "n3", ReplicaID: 3}}
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action != nil || decision.Reason != "quorum_lost" {
		t.Fatalf("hold or block priority: %+v/%v", decision, err)
	}
	delete(in.Status, "shard0")
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action != nil || decision.Reason != "local_stop" {
		t.Fatalf("held identity replaced: %+v/%v", decision, err)
	}
	in.Held[0].ReplicaID = 99 // an obsolete hold cannot suppress the current identity
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action == nil || decision.Action.Identity.ReplicaID != 3 {
		t.Fatalf("stale hold: %+v/%v", decision, err)
	}
}

func clusterDocument(load bool) topology.Document {
	doc := topology.Document{Version: 1, ClusterID: "repair-test", Origin: topology.OriginFounding, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}}
	for i := range 3 {
		doc.Nodes = append(doc.Nodes, topology.Node{Name: fmt.Sprintf("n%d", i+1), Type: "worker", State: topology.NodeActive})
	}
	for _, id := range []string{topology.MetaGroupID, "shard0"} {
		if id == "shard0" && !load {
			continue
		}
		group := topology.Group{ID: id, NextReplicaID: 4}
		for i := range 3 {
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: doc.Nodes[i].Name, ReplicaID: uint64(i + 1), Role: topology.ReplicaVoter})
		}
		doc.Groups = append(doc.Groups, group)
	}
	return doc
}
