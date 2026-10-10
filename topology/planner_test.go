package topology

import (
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

func planDoc(lo, hi int, nodeNames []string, groups ...Group) Document {
	doc := Document{
		Version: 7, ClusterID: "test", Placement: PlacementRule{MinReplicas: lo, MaxReplicas: hi},
		ChangedBy: "seed", Origin: OriginAPI, CreatedAt: time.Now().UTC(),
	}
	meta := Group{ID: MetaGroupID, NextReplicaID: uint64(len(nodeNames) + 1)}
	for i, name := range nodeNames {
		doc.Nodes = append(doc.Nodes, Node{Name: name, Type: "worker", State: NodeActive})
		meta.Replicas = append(meta.Replicas, Replica{NodeName: name, ReplicaID: uint64(i + 1), Role: ReplicaVoter})
	}
	doc.Groups = append([]Group{meta}, groups...)
	return doc
}

func shardOf(id string, replicas ...Replica) Group {
	next := uint64(1)
	for _, replica := range replicas {
		if replica.ReplicaID >= next {
			next = replica.ReplicaID + 1
		}
	}
	return Group{ID: id, Replicas: replicas, NextReplicaID: next}
}

func rep(node string, id uint64) Replica {
	return Replica{NodeName: node, ReplicaID: id, Role: ReplicaVoter}
}

func learner(node string, id uint64) Replica {
	return Replica{NodeName: node, ReplicaID: id, Role: ReplicaLearner}
}

func groupStatus(id string, leader uint64, voters, learners []uint64) GroupStatus {
	return GroupStatus{
		GroupID: id, Found: true, HostNode: "remote-host", LeaderID: leader,
		ConfState: raftpb.ConfState{Voters: voters, Learners: learners},
	}
}

func statusForDocument(doc Document) map[string]GroupStatus {
	statuses := make(map[string]GroupStatus, len(doc.Groups))
	for _, group := range doc.Groups {
		var voters []uint64
		var learners []uint64
		for _, replica := range group.Replicas {
			if replica.Role == ReplicaVoter {
				voters = append(voters, replica.ReplicaID)
			} else {
				learners = append(learners, replica.ReplicaID)
			}
		}
		leader := uint64(0)
		if len(voters) > 0 {
			leader = voters[0]
		}
		statuses[group.ID] = groupStatus(group.ID, leader, voters, learners)
	}
	return statuses
}

func step(groupID string, kind StepKind, node string, replicaID uint64) Step {
	return Step{GroupID: groupID, Kind: kind, NodeName: node, ReplicaID: replicaID}
}

func requirePlan(t *testing.T, got Plan, version uint64, want []Step) {
	t.Helper()
	if got.Version != version {
		t.Fatalf("plan version = %d, want %d", got.Version, version)
	}
	for i := range want {
		want[i].Seq = i + 1
	}
	if len(want) == 0 && len(got.Steps) == 0 {
		return
	}
	if !reflect.DeepEqual(got.Steps, want) {
		t.Fatalf("steps mismatch:\n got: %+v\nwant: %+v", got.Steps, want)
	}
}

func TestBuildPlanFreshFounding(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"})
	desired := active.Clone()
	desired.Version++
	desired.Groups = append(desired.Groups, shardOf("shard1", rep("n1", 1), rep("n2", 2)))
	statuses := statusForDocument(active)
	statuses["shard1"] = GroupStatus{GroupID: "shard1", Found: false}

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{step("shard1", StepCreateFoundingGroup, "", 0)})
}

func TestBuildPlanFreshFoundingAddsIntentionalLearnerAfterFounders(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"})
	desired := active.Clone()
	desired.Version++
	desired.Groups = append(desired.Groups, shardOf("shard1", rep("n1", 1), learner("n2", 2)))
	statuses := statusForDocument(active)
	statuses["shard1"] = GroupStatus{GroupID: "shard1", Found: false}

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard1", StepCreateFoundingGroup, "", 0),
		step("shard1", StepPrepareReplica, "n2", 2),
		step("shard1", StepAddLearner, "n2", 2),
	})
}

func TestBuildPlanEstablishedVoterGrowAddsLearnerThenVoter(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("shard0", rep("n1", 1)))
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas = append(desired.Group("shard0").Replicas, rep("n2", 2))
	desired.Group("shard0").NextReplicaID = 3
	statuses := statusForDocument(active)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard0", StepPrepareReplica, "n2", 2),
		step("shard0", StepAddLearner, "n2", 2),
		step("shard0", StepWaitCaughtUp, "n2", 2),
		step("shard0", StepPromoteVoter, "n2", 2),
	})
}

func TestBuildPlanExistingLearnerPromotion(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("shard0", rep("n1", 1), learner("n2", 2)))
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas[1].Role = ReplicaVoter
	statuses := statusForDocument(active)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard0", StepWaitCaughtUp, "n2", 2),
		step("shard0", StepPromoteVoter, "n2", 2),
	})
}

func TestBuildPlanIntentionalLearnerStopsAfterAdd(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("shard0", rep("n1", 1)))
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas = append(desired.Group("shard0").Replicas, learner("n2", 2))
	desired.Group("shard0").NextReplicaID = 3
	statuses := statusForDocument(active)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard0", StepPrepareReplica, "n2", 2),
		step("shard0", StepAddLearner, "n2", 2),
	})
}

func TestBuildPlanDemotesVoterAndTransfersLeader(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2", "n3"}, shardOf("shard0", rep("n1", 1), rep("n2", 2), rep("n3", 3)))
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas[1].Role = ReplicaLearner
	statuses := statusForDocument(active)
	statuses["shard0"] = groupStatus("shard0", 2, []uint64{1, 2, 3}, nil)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard0", StepTransferLeader, "n1", 1),
		step("shard0", StepDemoteVoter, "n2", 2),
	})
}

func TestBuildPlanIncludesMetaLearnerThenVoterChange(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2", "n3"})
	desired := active.Clone()
	desired.Version++
	active.Group(MetaGroupID).Replicas = active.Group(MetaGroupID).Replicas[:2]
	active.Group(MetaGroupID).NextReplicaID = 4
	statuses := statusForDocument(active)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step(MetaGroupID, StepPrepareReplica, "n3", 3),
		step(MetaGroupID, StepAddLearner, "n3", 3),
		step(MetaGroupID, StepWaitCaughtUp, "n3", 3),
		step(MetaGroupID, StepPromoteVoter, "n3", 3),
	})
}

func TestBuildPlanReplacementAtMaxShrinksFirst(t *testing.T) {
	active := planDoc(1, 4, []string{"n1", "n2", "n3", "n4"}, shardOf("shard0", rep("n1", 1), rep("n2", 2), rep("n3", 3)))
	active.Group("shard0").Placement = &PlacementRule{MinReplicas: 1, MaxReplicas: 3}
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas = []Replica{rep("n1", 1), rep("n3", 3), rep("n4", 4)}
	desired.Group("shard0").NextReplicaID = 5
	statuses := statusForDocument(active)
	statuses["shard0"] = groupStatus("shard0", 2, []uint64{1, 2, 3}, nil)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard0", StepTransferLeader, "n1", 1),
		step("shard0", StepRemoveReplica, "n2", 2),
		step("shard0", StepStopReplica, "n2", 2),
		step("shard0", StepCleanupReplicaState, "n2", 2),
		step("shard0", StepPrepareReplica, "n4", 4),
		step("shard0", StepAddLearner, "n4", 4),
		step("shard0", StepWaitCaughtUp, "n4", 4),
		step("shard0", StepPromoteVoter, "n4", 4),
	})
}

func TestBuildPlanWholeGroupDeletionStopsEveryFormerHost(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("shard9", rep("n1", 1), rep("n2", 2)))
	desired := active.Clone()
	desired.Version++
	desired.Groups = desired.Groups[:1]
	statuses := statusForDocument(active)

	plan, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	requirePlan(t, plan, desired.Version, []Step{
		step("shard9", StepStopReplica, "n1", 1),
		step("shard9", StepStopReplica, "n2", 2),
		step("shard9", StepDeleteGroupState, "", 0),
	})
}

func TestBuildPlanRefusesUnsafeDisjointReplacement(t *testing.T) {
	active := planDoc(1, 5, []string{"n1", "n2", "n3", "n4", "n5"}, shardOf("shard0", rep("n1", 1), rep("n2", 2), rep("n3", 3)))
	active.Group("shard0").Placement = &PlacementRule{MinReplicas: 1, MaxReplicas: 3}
	desired := active.Clone()
	desired.Version++
	desired.Group("shard0").Replicas = []Replica{rep("n3", 3), rep("n4", 4), rep("n5", 5)}
	desired.Group("shard0").NextReplicaID = 6
	statuses := statusForDocument(active)

	_, err := BuildPlan(active, desired, statuses, plannerRules())
	if !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatalf("BuildPlan error = %v, want fault.ErrUnsafeMembership", err)
	}
}

func TestBuildPlanEstablishedGroupMissingGloballyIsUnavailable(t *testing.T) {
	active := planDoc(1, 3, []string{"n1"}, shardOf("shard0", rep("n1", 1)))
	desired := active.Clone()
	desired.Version++
	statuses := statusForDocument(active)
	statuses["shard0"] = GroupStatus{GroupID: "shard0", Found: false}

	_, err := BuildPlan(active, desired, statuses, plannerRules())
	if !errors.Is(err, fault.ErrUnavailable) {
		t.Fatalf("BuildPlan error = %v, want fault.ErrUnavailable", err)
	}
}

func TestBuildPlanRefusesObservedGroupWithoutActiveIdentities(t *testing.T) {
	active := planDoc(1, 3, []string{"n1"})
	desired := active.Clone()
	desired.Version++
	statuses := statusForDocument(active)
	statuses["orphan"] = groupStatus("orphan", 1, []uint64{1}, nil)

	_, err := BuildPlan(active, desired, statuses, plannerRules())
	if !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatalf("BuildPlan error = %v, want fault.ErrUnsafeMembership", err)
	}
}

func TestBuildPlanDeterministic(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2", "n3"},
		shardOf("shardB", rep("n1", 1)), shardOf("shardA", rep("n1", 1)))
	desired := active.Clone()
	desired.Version++
	desired.Group("shardA").Replicas = append(desired.Group("shardA").Replicas, rep("n2", 2))
	desired.Group("shardB").Replicas = append(desired.Group("shardB").Replicas, rep("n3", 3))
	desired.Group("shardA").NextReplicaID = 3
	desired.Group("shardB").NextReplicaID = 4
	statuses := statusForDocument(active)

	first, err := BuildPlan(active, desired, statuses, plannerRules())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	for i := range 50 {
		again, err := BuildPlan(active, desired, statuses, plannerRules())
		if err != nil {
			t.Fatalf("BuildPlan run %d: %v", i, err)
		}
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("plan changed on run %d", i)
		}
	}
}

func TestValidatePlanShapeRejectsMissingAcknowledgementAdjacency(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("shard0", rep("n1", 1), rep("n2", 2)))
	desired := active.Clone()
	desired.Version++
	cases := []struct {
		name  string
		steps []Step
	}{
		{"prepare without add", []Step{step("shard0", StepPrepareReplica, "n2", 2)}},
		{"add without prepare", []Step{step("shard0", StepAddLearner, "n2", 2)}},
		{"wait without promote", []Step{step("shard0", StepWaitCaughtUp, "n2", 2)}},
		{"promote without wait", []Step{step("shard0", StepPromoteVoter, "n2", 2)}},
		{"remove without stop", []Step{step("shard0", StepRemoveReplica, "n2", 2)}},
		{"delete without former stops", []Step{step("shard0", StepDeleteGroupState, "", 0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := range tc.steps {
				tc.steps[i].Seq = i + 1
			}
			if err := ValidatePlanShape(active, desired, Plan{Version: desired.Version, Steps: tc.steps}); err == nil {
				t.Fatalf("ValidatePlanShape accepted %+v", tc.steps)
			}
		})
	}
}

func plannerRules() Rules {
	return Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 7, MaxReplicas: 5}}
}

func TestPlannerRejectsMalformedObservedMembership(t *testing.T) {
	active := planDoc(1, 3, []string{"n1", "n2"}, shardOf("load", rep("n1", 1), rep("n2", 2)))
	desired := active.Clone()
	desired.Version++
	cases := []struct {
		name string
		cs   raftpb.ConfState
		want error
	}{
		{"no voters", raftpb.ConfState{}, fault.ErrUnavailable},
		{"zero", raftpb.ConfState{Voters: []uint64{0, 1}}, fault.ErrUnsafeMembership},
		{"duplicate", raftpb.ConfState{Voters: []uint64{1, 1}}, fault.ErrUnsafeMembership},
		{"two roles", raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{1}}, fault.ErrUnsafeMembership},
		{"joint", raftpb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{1, 2}}, fault.ErrUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := statusForDocument(active)
			status["load"] = GroupStatus{GroupID: "load", Found: true, ConfState: tc.cs}
			if _, err := BuildPlan(active, desired, status, plannerRules()); !errors.Is(err, tc.want) {
				t.Fatal("invalid observation admitted", err)
			}
		})
	}
}
