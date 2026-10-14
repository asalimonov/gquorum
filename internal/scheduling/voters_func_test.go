//go:build functional

package scheduling

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/metrics"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type gatedObservationResetHost struct {
	Host
	entered chan struct{}
	release chan struct{}
}

func (h gatedObservationResetHost) ResetObservations() {
	close(h.entered)
	<-h.release
	h.Host.ResetObservations()
}

func TestEnablementHoldsSchedulerGateUntilVoterEvidenceReset(t *testing.T) {
	t.Parallel()
	h, _ := voterHostFixture(t)
	gate := gatedObservationResetHost{Host: h, entered: make(chan struct{}), release: make(chan struct{})}
	r, err := New(Config{Host: gate, Settings: Settings{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var release sync.Once
	go func() { done <- r.SetEnabled(t.Context(), Enablement{Enabled: true}) }()
	defer func() { release.Do(func() { close(gate.release) }); <-done }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("enablement did not reset evidence")
	}
	bounded, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := r.Tick(bounded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("tick bypassed the pending evidence reset", err)
	}
	if h.commits != 0 {
		t.Fatal("old standby evidence authorized a voter change")
	}
	release.Do(func() { close(gate.release) })
	if err := <-done; err != nil {
		done <- err
		t.Fatal(err)
	} else {
		done <- nil
	}
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.commits != 0 {
		t.Fatal("re-enablement retained a pre-disable failure/stability window")
	}
}

func voterHostFixture(t *testing.T) (*testHost, *Scheduler) {
	return voterHostFixtureWith(t, nil)
}

func voterHostFixtureWith(t *testing.T, configure func(*topology.Document)) (*testHost, *Scheduler) {
	t.Helper()
	limits := topology.Limits{MaxNodes: 5, MaxReplicas: 5}
	h := &testHost{t: t, leader: true, limits: limits, machine: metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: limits}, nil), health: map[string]topology.Health{}, status: map[string]topology.GroupStatus{}}
	doc := clusterDocument(false)
	p := topology.DefaultVoterConfig()
	g := doc.MetaGroup()
	g.VoterConfig = &p
	g.NextReplicaID = 6
	for i := 4; i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		g.Replicas = append(g.Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: topology.ReplicaLearner})
	}
	if configure != nil {
		configure(&doc)
	}
	observed := metrics.GroupObservation{GroupID: topology.MetaGroupID, LeaderID: 1, Term: 2, MembershipDigest: "membership", State: metrics.Current, TargetVoters: 3, NativeVoters: 3, EligibleStandbys: 2}
	status := topology.GroupStatus{GroupID: topology.MetaGroupID, Found: true, LeaderID: 1, Term: 2, Commit: 10, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}}, HealthKnown: true, LiveVoters: map[uint64]bool{}, Progress: map[uint64]topology.ReplicaProgress{}}
	for i := uint64(1); i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		h.health[name] = topology.HealthAlive
		status.LiveVoters[i] = true
		status.Progress[i] = topology.ReplicaProgress{Match: 10, RecentActive: true}
		stamp := metrics.Stamp{State: metrics.Current, Sequence: 20}
		r := metrics.ReplicaObservation{State: metrics.Current, Reachability: "alive", Sample: metrics.ReplicaSample{Identity: metrics.ReplicaIdentity{ClusterID: doc.ClusterID, GroupID: topology.MetaGroupID, NodeName: name, ReplicaID: i, InstanceID: name + "process", RunID: name + "run"}, Meter: stamp, Raft: stamp, Writer: stamp, Ready: true, Running: true, Term: 2, LeaderID: 1, MembershipDigest: "membership", Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}}, ReplicationLag: metrics.UnsignedValue{State: metrics.Current}, ApplyBacklog: metrics.UnsignedValue{State: metrics.Current}, Evidence: metrics.Evidence{StandbyReadyFor: metrics.DurationValue{State: metrics.Current, Value: time.Minute}}}
		if i == 3 {
			h.health[name] = topology.HealthDead
			status.LiveVoters[i] = false
			r.Reachability = "unreachable"
			r.Evidence.UnavailableFor = metrics.DurationValue{State: metrics.Current, Value: 30 * time.Second}
		}
		observed.Replicas = append(observed.Replicas, r)
	}
	h.status[topology.MetaGroupID] = status
	h.observations = metrics.ClusterSnapshot{CollectorRunID: "collector/epoch", ObserverInstanceID: "process", ActiveVersion: 1, IsCollectorLeader: true, Groups: []metrics.GroupObservation{observed}}
	h.mustApply(metaengine.ProposeDocument{Doc: doc, ExpectedVersion: 0})
	op := h.mustApply(metaengine.BeginOperatorProposal{Version: 1, Plan: topology.Plan{Version: 1}, Origin: topology.OriginFounding, At: time.Now().UTC()})
	h.mustApply(metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: op.OperatorID, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: time.Now().UTC()}})
	h.mustApply(metaengine.ActivateTargetProposal{OperatorID: op.OperatorID, Version: 1, Attempt: 1})
	driver, err := New(Config{Host: h})
	if err != nil {
		t.Fatal(err)
	}
	return h, driver
}

func TestSchedulerSkipsSameAZCandidateAndRejectsCustomBypass(t *testing.T) {
	t.Parallel()
	configure := func(doc *topology.Document) {
		doc.MetaGroup().VoterConfig.DistinctVoterLabel = "az"
		for i, zone := range []string{"a", "b", "c", "b", "c"} {
			doc.Nodes[i].Labels = map[string]string{"az": zone}
		}
	}
	h, r := voterHostFixtureWith(t, configure)
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	target := h.machine.Target()
	if h.commits != 1 || target.MetaGroup().Replicas[3].Role != topology.ReplicaLearner || target.MetaGroup().Replicas[4].Role != topology.ReplicaVoter {
		t.Fatal("driver selected same-AZ learner or refused valid alternative", h.commits, target.MetaGroup().Replicas, r.Status())
	}
	h, r = voterHostFixtureWith(t, configure)
	r.policy = schedulerFunc(func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		promote := topology.ReplicaKey{NodeName: "n4", ReplicaID: 4}
		demote := topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}
		return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.ReconfigureVoters,
			VoterChange: &topology.VoterChange{GroupID: topology.MetaGroupID, Promote: &promote, Demote: &demote}}}, nil
	})
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.commits != 0 || h.machine.Target().Version != 1 {
		t.Fatal("custom scheduler bypassed AZ placement", h.commits, r.Status())
	}
}
func TestSchedulerApprovesExactStableLearnerReplacement(t *testing.T) {
	t.Parallel()
	h, r := voterHostFixture(t)
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.commits != 1 || h.approvals != 0 {
		t.Fatalf("commits=%d approvals=%d status=%+v", h.commits, h.approvals, r.Status())
	}
	views := h.machine.Views()
	latest, target, active := views.Latest, views.Target, views.Active
	if latest.Version != 2 || target.Version != 2 || active.Version != 1 {
		t.Fatalf("approval did not preserve convergence boundary: %d/%d/%d", latest.Version, target.Version, active.Version)
	}
	got := map[uint64]topology.ReplicaRole{}
	for _, p := range target.MetaGroup().Replicas {
		got[p.ReplicaID] = p.Role
	}
	if got[3] != topology.ReplicaLearner || got[4] != topology.ReplicaVoter || got[5] != topology.ReplicaLearner || target.MetaGroup().NextReplicaID != 6 {
		t.Fatal(got)
	}
	if len(h.machine.VoterSwitches()) != 0 {
		t.Fatal("approval fabricated completed switch record")
	}
}
func TestSchedulerRevalidatesVoterEvidenceAndScopedCAS(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"callback staged intent", "callback stale learner", "callback lost quorum", "callback changed observer", "approval CAS race", "custom healthy rotation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			h, r := voterHostFixture(t)
			builtin, _ := schedulingapi.NewDefaultRepairPolicy(schedulingapi.DefaultRepairOptions{})
			stage := func() {
				doc := h.machine.Latest()
				version := doc.Version
				doc.Version++
				doc.ParentVersion = version
				doc.ChangedBy = "operator"
				h.mustApply(metaengine.ProposeDocument{Doc: doc, ExpectedVersion: version})
			}
			r.policy = schedulerFunc(func(ctx context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				decision, err := builtin.Schedule(ctx, in)
				if err != nil {
					return decision, err
				}
				if decision.Action == nil {
					t.Fatal("fixture has no eligible action")
				}
				switch scenario {
				case "callback staged intent":
					stage()
				case "callback stale learner":
					h.observations.Groups[0].Replicas[3].Sample.Writer.State = metrics.Stale
				case "callback lost quorum":
					h.status[topology.MetaGroupID].LiveVoters[2] = false
				case "callback changed observer":
					h.observations.CollectorRunID = "successor-epoch"
				case "approval CAS race":
					h.beforeCommit = stage
				case "custom healthy rotation":
					decision.Action.VoterChange.Demote = &topology.ReplicaKey{NodeName: "n2", ReplicaID: 2}
				}
				return decision, nil
			})
			if err := r.Tick(t.Context()); err != nil {
				t.Fatal(err)
			}
			if h.commits != 0 || h.approvals != 0 || h.machine.Target().Version != 1 {
				t.Fatalf("unsafe approval commits=%d approvals=%d status=%+v", h.commits, h.approvals, r.Status())
			}
			if scenario == "callback staged intent" || scenario == "approval CAS race" {
				if h.machine.Latest().Version != 2 {
					t.Fatal("unrelated staged edit disappeared")
				}
			}
		})
	}
}
