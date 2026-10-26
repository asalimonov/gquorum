//go:build !functional

package scheduling

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/votercheck"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func voterInput() ScheduleInput {
	p := topology.DefaultVoterConfig()
	doc := topology.Document{ClusterID: "cluster", Version: 1, Groups: []topology.Group{{ID: topology.MetaGroupID, VoterConfig: &p}}}
	group := metrics.GroupObservation{SourceVersion: 1, GroupID: topology.MetaGroupID, LeaderID: 1, Term: 2, MembershipDigest: "members", State: metrics.Current, TargetVoters: 3, NativeVoters: 3, EligibleStandbys: 2}
	for i := uint64(1); i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		role := topology.ReplicaVoter
		if i > 3 {
			role = topology.ReplicaLearner
		}
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		doc.Groups[0].Replicas = append(doc.Groups[0].Replicas, topology.Replica{NodeName: name, ReplicaID: i, Role: role})
		stamp := metrics.Stamp{State: metrics.Current, Sequence: 10}
		observation := metrics.ReplicaObservation{State: metrics.Current, Reachability: "alive", Sample: metrics.ReplicaSample{Identity: metrics.ReplicaIdentity{ClusterID: "cluster", GroupID: topology.MetaGroupID, NodeName: name, ReplicaID: i, InstanceID: name + "-process", RunID: name + "-run"}, Meter: stamp, Raft: stamp, Writer: stamp, Ready: true, Running: true, LeaderID: 1, Term: 2, Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}, MembershipDigest: "members"}, ReplicationLag: metrics.UnsignedValue{State: metrics.Current}, ApplyBacklog: metrics.UnsignedValue{State: metrics.Current}, Evidence: metrics.Evidence{StandbyReadyFor: metrics.DurationValue{State: metrics.Current, Value: time.Minute}}}
		if i == 3 {
			observation.Reachability = "unreachable"
			observation.Evidence.UnavailableFor = metrics.DurationValue{State: metrics.Current, Value: 30 * time.Second}
		}
		group.Replicas = append(group.Replicas, observation)
	}
	return ScheduleInput{Target: doc, Groups: provenPolicyGroups(doc), Metrics: metrics.ClusterSnapshot{TargetVersion: 1, IsCollectorLeader: true, CollectorRunID: "collector", Groups: []metrics.GroupObservation{group}}, Status: map[string]topology.GroupStatus{topology.MetaGroupID: {GroupID: topology.MetaGroupID, SourceVersion: 1, Found: true, LeaderID: 1, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: false, 4: true, 5: true}}}}
}

func TestRetainedThreeAZPolicyCanPromoteSecondEligibleNode(t *testing.T) {
	t.Parallel()
	in := voterInput()
	in.Target.Nodes = []topology.Node{in.Target.Nodes[0], in.Target.Nodes[3]}
	in.Target.Nodes[0].Labels = map[string]string{"az": "a"}
	in.Target.Nodes[1].Labels = map[string]string{"az": "b"}
	group := in.Target.MetaGroup()
	group.Replicas = []topology.Replica{group.Replicas[0], group.Replicas[3]}
	group.VoterConfig.DistinctVoterLabel = "az"
	observed := &in.Metrics.Groups[0]
	observed.NativeVoters = 1
	observed.Replicas = []metrics.ReplicaObservation{observed.Replicas[0], observed.Replicas[3]}
	for i := range observed.Replicas {
		observed.Replicas[i].Sample.Voters = []uint64{1}
		observed.Replicas[i].Sample.Learners = []uint64{4}
	}
	status := in.Status[topology.MetaGroupID]
	status.ConfState = raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{4}}
	status.LiveVoters = map[uint64]bool{1: true, 4: true}
	in.Status[topology.MetaGroupID] = status
	in.Groups = provenPolicyGroups(in.Target)
	policy, err := NewDefaultRepairPolicy(DefaultRepairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := policy.Schedule(t.Context(), in)
	if err != nil || decision.Action == nil || decision.Action.VoterChange.Promote == nil || decision.Action.VoterChange.Promote.ReplicaID != 4 || decision.Action.VoterChange.Demote != nil {
		t.Fatal("retained third-domain deficit blocked second voter", decision, err)
	}
	if in.Target.MetaGroup().VoterConfig.TargetVoters != 3 {
		t.Fatal("policy input was normalized")
	}
}
func TestVoterConfigSelectsOneStableLearnerAndPreservesHealthyVoters(t *testing.T) {
	t.Parallel()
	in := voterInput()
	before := in.Clone()
	p, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	got, err := p.Schedule(context.Background(), in)
	if err != nil || got.Action == nil || got.Action.Kind != ReconfigureVoters || got.Action.VoterChange.Promote.ReplicaID != 4 || got.Action.VoterChange.Demote.ReplicaID != 3 {
		t.Fatalf("decision %+v %v", got, err)
	}
	if !reflect.DeepEqual(in, before) {
		t.Fatal("mutated input")
	}
	in.Metrics.Groups[0].Replicas[2].Evidence.UnavailableFor = metrics.DurationValue{State: metrics.Current}
	got, err = p.Schedule(context.Background(), in)
	if err != nil || got.Action != nil || got.Reason != "converged" {
		t.Fatalf("rotated healthy seats %+v %v", got, err)
	}
}

func TestVoterConfigSkipsSameAZAndLearnerOnlyCandidates(t *testing.T) {
	t.Parallel()
	in := voterInput()
	in.Target.Groups[0].VoterConfig.DistinctVoterLabel = "az"
	for i, zone := range []string{"a", "b", "c", "b", "c"} {
		in.Target.Nodes[i].Labels = map[string]string{"az": zone}
	}
	p, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	decision, err := p.Schedule(context.Background(), in)
	if err != nil || decision.Action == nil || decision.Action.VoterChange.Promote.ReplicaID != 5 {
		t.Fatal("same-AZ learner blocked a valid alternative", decision, err)
	}
	in.Target.Nodes[3].Labels[topology.LabelLearnerOnly] = "true"
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action == nil || decision.Action.VoterChange.Promote.ReplicaID != 5 {
		t.Fatal("learner-only node blocked a valid alternative", decision, err)
	}
	in.Target.Nodes[4].Labels["az"] = "b"
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action != nil || decision.Reason != "node_learner_only" {
		t.Fatal("unsatisfied placement was not reported", decision, err)
	}
}

func TestLegacyRepairSkipsReservedSuccessor(t *testing.T) {
	t.Parallel()
	doc := topology.Document{ClusterID: "cluster", Version: 1, Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive,
		Labels: map[string]string{topology.LabelReserved: "true"}}}, Groups: []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	in := ScheduleInput{Target: doc, Groups: provenPolicyGroups(doc), Status: map[string]topology.GroupStatus{"work": {GroupID: "work", SourceVersion: 1, Found: true}}, Alive: map[string]time.Duration{"n1": time.Minute}}
	p, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	decision, err := p.Schedule(context.Background(), in)
	if err != nil || decision.Action != nil || decision.Reason != "node_reserved" {
		t.Fatal("reserved successor repair was selected", decision, err)
	}
	delete(in.Target.Nodes[0].Labels, topology.LabelReserved)
	decision, err = p.Schedule(context.Background(), in)
	if err != nil || decision.Action == nil || decision.Action.Identity.ReplicaID != 1 {
		t.Fatal("unreserved legacy repair was not selected", decision, err)
	}
}
func TestVoterConfigMandatoryGuards(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*ScheduleInput){
		"stale group":       func(in *ScheduleInput) { in.Metrics.Groups[0].State = metrics.Stale },
		"wrong source":      func(in *ScheduleInput) { in.Metrics.Groups[0].SourceVersion++ },
		"follower observer": func(in *ScheduleInput) { in.Metrics.IsCollectorLeader = false },
		"lost quorum":       func(in *ScheduleInput) { in.Status[topology.MetaGroupID].LiveVoters[2] = false },
		"changed leader": func(in *ScheduleInput) {
			s := in.Status[topology.MetaGroupID]
			s.Term++
			in.Status[topology.MetaGroupID] = s
		},
		"failure too short": func(in *ScheduleInput) {
			in.Metrics.Groups[0].Replicas[2].Evidence.UnavailableFor.Value = 29 * time.Second
		},
		"all pinned": func(in *ScheduleInput) {
			in.Target.Groups[0].Replicas[3].PinnedLearner = true
			in.Target.Groups[0].Replicas[4].PinnedLearner = true
		},
		"both cooling": func(in *ScheduleInput) {
			for i := 3; i <= 4; i++ {
				in.Metrics.Groups[0].Replicas[i].Evidence.DemotionCooldownRemaining = time.Second
			}
		},
		"group cooling": func(in *ScheduleInput) { in.Metrics.Groups[0].CooldownRemaining = time.Minute },
		"both stale writer": func(in *ScheduleInput) {
			for i := 3; i <= 4; i++ {
				in.Metrics.Groups[0].Replicas[i].Sample.Writer.State = metrics.Stale
			}
		},
		"stability too short": func(in *ScheduleInput) {
			for i := 3; i <= 4; i++ {
				in.Metrics.Groups[0].Replicas[i].Evidence.StandbyReadyFor.Value = 59 * time.Second
			}
		},
		"held learners": func(in *ScheduleInput) {
			for i := uint64(4); i <= 5; i++ {
				in.Held = append(in.Held, storage.Identity{ClusterID: "cluster", GroupID: topology.MetaGroupID, NodeName: fmt.Sprintf("n%d", i), ReplicaID: i})
			}
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			in := voterInput()
			edit(&in)
			p, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
			got, err := p.Schedule(context.Background(), in)
			if err != nil || got.Action != nil {
				t.Fatalf("unsafe decision %+v %v", got, err)
			}
		})
	}
}
func TestVoterConfigCooldownExceptionAndDeficit(t *testing.T) {
	t.Parallel()
	in := voterInput()
	p, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	in.Metrics.Groups[0].CooldownRemaining = time.Minute
	in.Metrics.Groups[0].Replicas[2].Evidence.Reasons = []string{"new_failure"}
	in.Target.Groups[0].Replicas[3].PinnedLearner = true
	got, err := p.Schedule(context.Background(), in)
	if err != nil || got.Action == nil || got.Action.VoterChange.Promote.ReplicaID != 5 {
		t.Fatalf("exception failed %+v %v", got, err)
	}
	in.Target.Groups[0].VoterConfig.TargetVoters = 4
	in.Metrics.Groups[0].TargetVoters = 4
	in.Metrics.Groups[0].Replicas[2].Evidence.Reasons = nil
	got, err = p.Schedule(context.Background(), in)
	if err != nil || got.Action == nil || got.Action.VoterChange.Promote == nil || got.Action.VoterChange.Demote != nil {
		t.Fatalf("cooldown blocked deficit %+v %v", got, err)
	}
}
func TestScheduleInputOwnsMetricsAndSwitchRecords(t *testing.T) {
	t.Parallel()
	in := voterInput()
	in.Switches = []topology.VoterSwitchState{{GroupID: topology.MetaGroupID, Replicas: []topology.RoleChangeRecord{{Index: 7}}}}
	copy := in.Clone()
	copy.Metrics.Groups[0].Replicas[0].Sample.Voters[0] = 99
	copy.Switches[0].Replicas[0].Index = 99
	if in.Metrics.Groups[0].Replicas[0].Sample.Voters[0] != 1 || in.Switches[0].Replicas[0].Index != 7 {
		t.Fatal("callback aliases evidence")
	}
}

func TestSnapshotCatchupEvidenceIsSharedByPolicyAndMandatoryCheck(t *testing.T) {
	t.Parallel()
	in := voterInput()
	policy, err := NewDefaultRepairPolicy(DefaultRepairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := policy.Schedule(t.Context(), in)
	if err != nil || failed.Action == nil || failed.Action.VoterChange == nil {
		t.Fatal(failed, err)
	}
	change := *failed.Action.VoterChange
	victim := &in.Metrics.Groups[0].Replicas[2]
	victim.Evidence.UnavailableFor = metrics.DurationValue{State: metrics.Current}
	victim.Evidence.ReplicationLagFor = metrics.DurationValue{State: metrics.Current}
	victim.Evidence.NoProgressFor = metrics.DurationValue{State: metrics.Current}
	victim.Evidence.Reasons = []string{"snapshot_catchup"}
	victim.ReplicationLag = metrics.UnsignedValue{State: metrics.Current, Value: 1990}
	decision, err := policy.Schedule(t.Context(), in)
	if err != nil || decision.Action != nil {
		t.Fatal("policy replaced progressing snapshot", decision, err)
	}
	mandatory := votercheck.Input{Target: in.Target, Groups: in.Groups, Metrics: in.Metrics, Status: in.Status, Held: in.Held}
	if why := votercheck.Check(mandatory, change); why == "" {
		t.Fatal("mandatory check bypassed catch-up evidence")
	}
	victim.Evidence.Reasons = []string{"snapshot_transfer_failed"}
	victim.Evidence.ReplicationLagFor.Value = 30 * time.Second
	decision, err = policy.Schedule(t.Context(), in)
	if err != nil || decision.Action == nil {
		t.Fatal("bounded episode prevented later replacement", decision, err)
	}
	mandatory.Metrics = in.Metrics
	if why := votercheck.Check(mandatory, *decision.Action.VoterChange); why != "" {
		t.Fatal("mandatory check disagreed with policy", why)
	}
}
