//go:build functional

package scheduling

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/metrics"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

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

type testHost struct {
	limits                                   topology.Limits
	observations                             metrics.ClusterSnapshot
	t                                        *testing.T
	machine                                  *metaengine.Machine
	index                                    uint64
	leader                                   bool
	health                                   map[string]topology.Health
	status                                   map[string]topology.GroupStatus
	viewErr, statusErr, planErr              error
	beforeCommit                             func()
	blockViews                               bool
	wakes, commits, healthCalls, statusCalls int
}

func newTestHost(t *testing.T, voters ...uint64) *testHost {
	t.Helper()
	machine, err := metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &testHost{t: t, leader: true, machine: machine, health: map[string]topology.Health{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}, status: map[string]topology.GroupStatus{}}
	h.status[topology.MetaGroupID] = topology.GroupStatus{GroupID: topology.MetaGroupID, Found: true, LeaderID: 1, Commit: 10, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	h.status["shard0"] = topology.GroupStatus{GroupID: "shard0", Found: true, LeaderID: 1, Commit: 10, ConfState: raftpb.ConfState{Voters: voters}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	h.restoreConverged(clusterDocument(true))
	h.observations = metrics.ClusterSnapshot{CollectorRunID: "epoch", ObserverInstanceID: "process", IsCollectorLeader: true}
	return h
}

func TestCustomLegacyRepairCannotAllocateOnReservedNode(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	doc := h.machine.Latest()
	doc.Node("n3").Labels = map[string]string{topology.LabelReserved: "true"}
	h.restoreConverged(doc)
	r, err := New(Config{Host: h, Policy: schedulerFunc(func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica,
			Identity: storage.Identity{ClusterID: doc.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.commits != 0 || h.machine.Target().Version != doc.Version {
		t.Fatal("custom repair bypassed reserved admission", r.Status())
	}
}
func (h *testHost) apply(ctx context.Context, proposal any) (metaengine.ApplyResult, error) {
	data, err := metaengine.Encode(proposal)
	if err != nil {
		return metaengine.ApplyResult{}, err
	}
	h.index++
	result, err := h.machine.Apply(ctx, raftengine.Command{Index: h.index, Term: 1, Data: data, ConfState: h.status[topology.MetaGroupID].ConfState})
	if err != nil {
		return metaengine.ApplyResult{}, err
	}
	var out metaengine.ApplyResult
	if err = json.Unmarshal(result.Data, &out); err != nil {
		return out, err
	}
	if !out.Accepted {
		code := fault.OperationAuthorization
		if string(out.Code) == "version_conflict" {
			code = fault.VersionConflict
		}
		return out, fault.New(code, out.Reason, nil)
	}
	return out, nil
}
func (h *testHost) mustApply(p any) metaengine.ApplyResult {
	h.t.Helper()
	out, err := h.apply(context.Background(), p)
	if err != nil {
		h.t.Fatal(err)
	}
	return out
}
func (h *testHost) IsLeader() bool { return h.leader }
func (h *testHost) Health(context.Context) (map[string]topology.Health, error) {
	h.healthCalls++
	return h.health, nil
}
func (h *testHost) WakeOperations() { h.wakes++ }
func (h *testHost) stage(origin topology.Origin, parent uint64) topology.Document {
	doc := h.machine.Latest()
	doc.Version++
	doc.ParentVersion = parent
	doc.Origin = origin
	doc.Comment = "staged intent"
	doc.Groups[len(doc.Groups)-1].Labels = map[string]string{"intent": "staged"}
	return h.mustApply(metaengine.ProposeDocument{Doc: doc, ExpectedVersion: doc.Version - 1}).Doc
}
func newTestScheduler(t *testing.T, h *testHost, settings Settings) *Scheduler {
	t.Helper()
	r, err := New(Config{Host: h, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	for _, n := range h.machine.Target().Nodes {
		r.aliveSince[n.Name] = now.Add(-2 * r.settings.StabilityWindow)
	}
	return r
}
func tick(t *testing.T, r *Scheduler) {
	t.Helper()
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func assertStatus(t *testing.T, r *Scheduler, state topology.SchedulingState, reason string) {
	t.Helper()
	s := r.Status()
	if s.State != state || s.Reason != reason {
		t.Fatalf("status=%+v, want %s/%s", s, state, reason)
	}
}

func TestSchedulerStartStopLifecycle(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2, 3)
	r := newTestScheduler(t, h, Settings{})
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	r = newTestScheduler(t, h, Settings{})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestSchedulerGatesLeaderHealthAndDisabled(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"follower", "unknown", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			h := newTestHost(t, 1, 2)
			settings := Settings{Disabled: mode == "disabled"}
			if mode == "follower" {
				h.leader = false
			}
			if mode == "unknown" {
				h.health = nil
			}
			r := newTestScheduler(t, h, settings)
			tick(t, r)
			if h.commits != 0 || mode != "unknown" && h.statusCalls != 0 {
				t.Fatal("gate allowed repair observations")
			}
		})
	}
}

func TestSchedulerLocalKillSwitchResetsStability(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	if err := r.SetEnabled(context.Background(), Enablement{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	if len(r.aliveSince) != 0 || h.commits != 0 {
		t.Fatal("disabled worker retained observations or repaired")
	}
	if err := r.SetEnabled(context.Background(), Enablement{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonStarting)
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonStabilityWindow)
	if h.commits != 0 {
		t.Fatal("re-enabled worker trusted old observations")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.SetEnabled(ctx, Enablement{Enabled: false}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !r.Status().Enabled {
		t.Fatal("canceled switch changed state")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.SetEnabled(context.Background(), Enablement{Enabled: false}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := r.Tick(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
func TestSchedulerCommitsExactLearnerFirstRepair(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	tick(t, r)
	views := h.machine.Views()
	latest, target := views.Latest, views.Target
	if latest.Version != 2 || target.Version != 2 || views.FullyConverged.Version != 1 || h.commits != 1 || h.wakes != 1 {
		t.Fatal("repair was not atomic", latest.Version, target.Version, views.FullyConverged.Version, h.commits)
	}
	if latest.Origin != topology.OriginReconciler || latest.ParentVersion != 1 || latest.ChangedBy != "auto-repair" || latest.Group("shard0").NextReplicaID != 5 || latest.Group("shard0").Replicas[2].ReplicaID != 4 {
		t.Fatalf("wrong repair %+v", latest)
	}
	work, err := h.machine.ReadyBlocks("", 1)
	if err != nil || len(work) != 1 {
		t.Fatal(work, err)
	}
	ex := work[0].Operation
	// A fresh replacement on the same host cannot be prepared until the old
	// exact identity is removed and cleaned; the capacity bound forbids surge.
	want := []topology.StepKind{topology.StepRemoveReplica, topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter}
	if len(ex.Plan.Steps) != len(want) {
		t.Fatal(ex.Plan)
	}
	for i, k := range want {
		wantID := uint64(3)
		if i >= 3 {
			wantID = 4
		}
		if ex.Plan.Steps[i].Kind != k || ex.Plan.Steps[i].ReplicaID != wantID {
			t.Fatal(ex.Plan)
		}
	}
	assertStatus(t, r, topology.SchedulingRunning, ReconcileReasonApprovedOperation)
}
func TestSchedulerCASRaceLeavesOperatorVersionStaged(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	h.beforeCommit = func() { h.stage(topology.OriginOperator, 1) }
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonCASConflict)
	if h.machine.Latest().Origin != topology.OriginOperator || h.machine.Target().Version != 1 || h.commits != 0 || h.wakes != 0 {
		t.Fatal("repair overwrote operator CAS")
	}
}

func TestSchedulerNeverApprovesStagingByOrigin(t *testing.T) {
	t.Parallel()
	for _, origin := range []topology.Origin{topology.OriginOperator, topology.OriginInternal, topology.OriginReconciler} {
		t.Run(string(origin), func(t *testing.T) {
			t.Parallel()
			h := newTestHost(t, 1, 2)
			staged := h.stage(origin, 1)
			r := newTestScheduler(t, h, Settings{})
			for range 2 {
				tick(t, r)
			}
			assertStatus(t, r, topology.SchedulingWaiting, "staged_conflict")
			if h.commits != 0 || h.machine.Target().Version != 1 || !reflect.DeepEqual(h.machine.Latest(), staged) {
				t.Fatal("scheduler approved or overwrote staged intent", h.machine.Views())
			}
		})
	}
}

func TestSchedulerFollowerDropsStabilityEvidence(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	h.leader = false
	tick(t, r)
	assertStatus(t, r, topology.SchedulingStandby, ReconcileReasonNotLeader)
	if len(r.aliveSince) != 0 || len(r.aliveMembers) != 0 || h.healthCalls != 0 || h.commits != 0 {
		t.Fatal("follower retained or sampled repair evidence")
	}
	h.leader = true
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonStabilityWindow)
	if h.commits != 0 {
		t.Fatal("new leader reused previous stability evidence")
	}
}

func TestSchedulerWaitsForApprovedTopologyBeforeCallingPolicy(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1)
	var err error
	h.machine, err = metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := newTestScheduler(t, h, Settings{})
	r.policy = schedulerFunc(func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		t.Fatal("policy called before any topology was approved")
		return schedulingapi.ScheduleDecision{}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonStarting)
	if h.healthCalls != 0 || h.statusCalls != 0 || h.commits != 0 {
		t.Fatal("unapproved topology initiated repair work")
	}
}

func TestSchedulerStatusTransitionsAreOwnedAndLoggedOnce(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2, 3)
	r := newTestScheduler(t, h, Settings{})
	var buf bytes.Buffer
	r.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	changes := 0
	r.onTransition = func(s topology.SchedulingStatus) {
		changes++
		if s.CurrentOperationID != nil {
			*s.CurrentOperationID = "changed"
		}
		if s.CurrentAction != nil {
			s.CurrentAction.ReplicaID = 99
		}
	}
	blocked := r.newStatus(true, topology.SchedulingBlocked, ReconcileReasonQuorumLost)
	r.transition(blocked, "quorum lost")
	since := r.Status().Since
	r.now = func() time.Time { return since.Add(time.Second) }
	r.transition(blocked, "same observation")
	if strings.Count(buf.String(), "level=WARN") != 1 || changes != 1 || r.Status().Since != since {
		t.Fatal("unchanged status emitted another transition", buf.String(), changes, r.Status())
	}
	running := r.runningOperationStatus(true, "exec-v2-a1")
	running.CurrentAction = &topology.SchedulingActionStatus{ReplicaID: 3}
	r.transition(running, "repair approved")
	if strings.Count(buf.String(), "level=INFO") != 1 || changes != 2 {
		t.Fatal("recovery log lost", buf.String(), changes)
	}
	status := r.Status()
	*status.CurrentOperationID, status.CurrentAction.ReplicaID = "mutated", 100
	status = r.Status()
	if *status.CurrentOperationID != "exec-v2-a1" || status.CurrentAction.ReplicaID != 3 {
		t.Fatal("status or transition callback aliased scheduler state", status)
	}
}

type stubPolicy struct {
	decision schedulingapi.ScheduleDecision
}

func (p *stubPolicy) Schedule(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
	return p.decision, nil
}
func TestSchedulerRejectsPolicyActionsOutsideAllowList(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2, 3)
	r := newTestScheduler(t, h, Settings{})
	r.policy = &stubPolicy{schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: "remove_voter", Identity: storage.Identity{GroupID: "shard0", ReplicaID: 3}}}}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
	if h.commits != 0 || h.wakes != 0 {
		t.Fatal("forbidden action")
	}
}
func TestReconcileTickIsBoundedAndCancelable(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2, 3)
	h.blockViews = true
	r := newTestScheduler(t, h, Settings{Interval: 100 * time.Millisecond, TickTimeout: 20 * time.Millisecond})
	start := time.Now()
	err := r.Tick(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatal(err, time.Since(start))
	}
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
	// Stop must cancel a read already inside the automatic worker, rather than
	// waiting for the configured tick timeout.
	r = newTestScheduler(t, h, Settings{Interval: 100 * time.Millisecond, TickTimeout: 90 * time.Millisecond})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(110 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestSchedulerStatusConcurrentAccess(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2, 3)
	r := newTestScheduler(t, h, Settings{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				_ = r.Status()
			}
		})
	}
	for range 200 {
		tick(t, r)
	}
	wg.Wait()
}
func TestSchedulerSettingsValidation(t *testing.T) {
	t.Parallel()
	for _, s := range []Settings{{Interval: -1}, {TickTimeout: -1}, {StabilityWindow: -1}, {Cooldown: -1}, {Interval: time.Second, TickTimeout: time.Second}} {
		if _, err := New(Config{Host: newTestHost(t, 1, 2, 3), Settings: s}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(s, err)
		}
	}
	if _, err := New(Config{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func (h *testHost) Observations() metrics.ClusterSnapshot         { return h.observations.Clone() }
func (h *testHost) VoterSwitches() []topology.VoterSwitchState    { return h.machine.VoterSwitches() }
func (_ *testHost) RefreshObservations(ctx context.Context) error { return ctx.Err() }

func (h *testHost) ResetObservations() {
	h.observations.CollectorRunID += "/reset"
	for gi := range h.observations.Groups {
		for ri := range h.observations.Groups[gi].Replicas {
			h.observations.Groups[gi].Replicas[ri].Evidence = metrics.Evidence{}
		}
	}
}
