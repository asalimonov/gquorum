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
	"github.com/asalimonov/gquorum/policy"
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
	t                                                    *testing.T
	machine                                              *metaengine.Machine
	index                                                uint64
	leader                                               bool
	health                                               map[string]topology.Health
	status                                               map[string]topology.GroupStatus
	viewErr, operatorErr, statusErr, planErr, approveErr error
	beforeCommit                                         func()
	operatorOverride                                     *topology.Operator
	planOverride                                         *topology.Plan
	blockViews                                           bool
	wakes, commits, approvals, healthCalls, statusCalls  int
}

func newTestHost(t *testing.T, voters ...uint64) *testHost {
	t.Helper()
	h := &testHost{t: t, leader: true, machine: metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}, nil), health: map[string]topology.Health{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}, status: map[string]topology.GroupStatus{}}
	h.status[topology.MetaGroupID] = topology.GroupStatus{GroupID: topology.MetaGroupID, Found: true, LeaderID: 1, Commit: 10, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	h.status["shard0"] = topology.GroupStatus{GroupID: "shard0", Found: true, LeaderID: 1, Commit: 10, ConfState: raftpb.ConfState{Voters: voters}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	h.mustApply(metaengine.ProposeDocument{Doc: clusterDocument(true), ExpectedVersion: 0})
	ex := h.mustApply(metaengine.BeginOperatorProposal{Version: 1, Plan: topology.Plan{Version: 1}, Origin: topology.OriginFounding, At: time.Now().UTC()})
	h.mustApply(metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: ex.OperatorID, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: time.Now().UTC()}})
	h.mustApply(metaengine.ActivateTargetProposal{OperatorID: ex.OperatorID, Version: 1, Attempt: 1})
	return h
}
func (h *testHost) apply(ctx context.Context, proposal any) (metaengine.ApplyResult, error) {
	data, err := metaengine.Encode(proposal)
	if err != nil {
		return metaengine.ApplyResult{}, err
	}
	h.index++
	result, err := h.machine.Apply(ctx, raftengine.Command{Index: h.index, Term: 1, Data: data})
	if err != nil {
		return metaengine.ApplyResult{}, err
	}
	var out metaengine.ApplyResult
	if err = json.Unmarshal(result.Data, &out); err != nil {
		return out, err
	}
	if !out.Accepted {
		code := fault.OperatorAuthorization
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
func (h *testHost) Views(ctx context.Context) (topology.Document, topology.Document, topology.Document, error) {
	if h.blockViews {
		<-ctx.Done()
		return topology.Document{}, topology.Document{}, topology.Document{}, ctx.Err()
	}
	a, b, c := h.machine.Views()
	return a, b, c, h.viewErr
}
func (h *testHost) CurrentOperator(context.Context) (topology.Operator, bool, error) {
	if h.operatorOverride != nil {
		return *h.operatorOverride, true, h.operatorErr
	}
	ex, ok := h.machine.CurrentOperator()
	return ex, ok, h.operatorErr
}
func (h *testHost) IsLeader() bool { return h.leader }
func (h *testHost) Health(context.Context) (map[string]topology.Health, error) {
	h.healthCalls++
	return h.health, nil
}
func (h *testHost) GlobalStatus(context.Context, topology.Document) (map[string]topology.GroupStatus, error) {
	h.statusCalls++
	return h.status, h.statusErr
}
func (h *testHost) PlanFor(_ context.Context, doc topology.Document) (topology.Plan, error) {
	if h.planErr != nil {
		return topology.Plan{}, h.planErr
	}
	if h.planOverride != nil {
		p := *h.planOverride
		p.Version = doc.Version
		return p, nil
	}
	return topology.BuildPlan(h.machine.Active(), doc, h.status, topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()})
}
func (h *testHost) Approve(ctx context.Context, version uint64) (topology.OperatorID, error) {
	if h.approveErr != nil {
		return "", h.approveErr
	}
	doc, _ := h.machine.Get(version)
	plan, err := h.PlanFor(ctx, doc)
	if err != nil {
		return "", err
	}
	res, err := h.apply(ctx, metaengine.BeginOperatorProposal{Version: version, Plan: plan, At: time.Now().UTC(), Actor: "auto-repair", Origin: doc.Origin})
	if err == nil {
		h.approvals++
	}
	return res.OperatorID, err
}
func (h *testHost) CommitRepair(ctx context.Context, doc topology.Document, expected uint64, plan topology.Plan) (topology.OperatorID, error) {
	if h.beforeCommit != nil {
		h.beforeCommit()
	}
	res, err := h.apply(ctx, metaengine.CommitRepairOperatorProposal{Doc: doc, ExpectedVersion: expected, Plan: plan, At: time.Now().UTC()})
	if err == nil {
		h.commits++
	}
	return res.OperatorID, err
}
func (h *testHost) WakeOperator() { h.wakes++ }
func (h *testHost) stage(origin topology.Origin, parent uint64) topology.Document {
	doc := h.machine.Latest()
	doc.Version++
	doc.ParentVersion = parent
	doc.Origin = origin
	doc.Comment = "staged intent"
	return h.mustApply(metaengine.ProposeDocument{Doc: doc, ExpectedVersion: doc.Version - 1}).Doc
}
func newTestDriver(t *testing.T, h *testHost, settings Settings) *Driver {
	t.Helper()
	r, err := New(Config{Host: h, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	for _, n := range h.machine.Active().Nodes {
		r.aliveSince[n.Name] = now.Add(-2 * r.settings.StabilityWindow)
	}
	return r
}
func tick(t *testing.T, r *Driver) {
	t.Helper()
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func assertStatus(t *testing.T, r *Driver, state topology.SchedulingState, reason string) {
	t.Helper()
	s := r.Status()
	if s.State != state || s.Reason != reason {
		t.Fatalf("status=%+v, want %s/%s", s, state, reason)
	}
}

func TestDriverStartStopLifecycle(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	r := newTestDriver(t, h, Settings{})
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	r = newTestDriver(t, h, Settings{})
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
func TestDriverGatesLeaderHealthAndDisabled(t *testing.T) {
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
			r := newTestDriver(t, h, settings)
			tick(t, r)
			if h.commits != 0 || h.statusCalls != 0 {
				t.Fatal("gate allowed repair observations")
			}
		})
	}
}

func TestDriverLocalKillSwitchResetsStability(t *testing.T) {
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	if err := r.SetEnabled(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	if len(r.aliveSince) != 0 || h.commits != 0 {
		t.Fatal("disabled worker retained observations or repaired")
	}
	if err := r.SetEnabled(context.Background(), true); err != nil {
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
	if err := r.SetEnabled(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !r.Status().Enabled {
		t.Fatal("canceled switch changed state")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.SetEnabled(context.Background(), false); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := r.Tick(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
func TestDriverResumesApprovedOperatorBeforeHealthGate(t *testing.T) {
	for _, state := range []topology.OperatorState{topology.OperatorPending, topology.OperatorRunning, topology.OperatorWaitingForCatchUp, topology.OperatorSucceeded} {
		t.Run(string(state), func(t *testing.T) {
			for _, leader := range []bool{true, false} {
				h := newTestHost(t, 1, 2, 3)
				h.leader = leader
				h.health = nil
				h.stage(topology.OriginOperator, 1)
				h.mustApply(metaengine.BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2}, At: time.Now().UTC()})
				h.operatorOverride = &topology.Operator{Status: topology.OperatorStatus{ID: "exec-v2-a1", Version: 2, State: state}}
				r := newTestDriver(t, h, Settings{})
				tick(t, r)
				assertStatus(t, r, topology.SchedulingRunning, ReconcileReasonApprovedOperator)
				want := 0
				if leader {
					want = 1
				}
				if h.wakes != want || h.healthCalls != 0 || h.commits != 0 {
					t.Fatal("resume ordering or follower action")
				}
			}
		})
	}
}
func TestDriverDoesNotMergeStagedOrFailedOperatorIntent(t *testing.T) {
	for _, failed := range []bool{true, false} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			h := newTestHost(t, 1, 2)
			latest := h.stage(topology.OriginOperator, 1)
			if failed {
				h.operatorOverride = &topology.Operator{Status: topology.OperatorStatus{ID: "exec-v2-a1", Version: 2, State: topology.OperatorFailed}}
			}
			r := newTestDriver(t, h, Settings{})
			tick(t, r)
			want := ReconcileReasonStagedLatest
			if failed {
				want = ReconcileReasonFailedOperator
			}
			assertStatus(t, r, topology.SchedulingBlocked, want)
			if !reflect.DeepEqual(latest, h.machine.Latest()) || h.commits != 0 || h.approvals != 0 {
				t.Fatal("merged operator intent")
			}
		})
	}
}
func TestDriverCommitsExactLearnerFirstRepair(t *testing.T) {
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	tick(t, r)
	latest, target, active := h.machine.Views()
	if latest.Version != 2 || target.Version != 2 || active.Version != 1 || h.commits != 1 || h.wakes != 1 {
		t.Fatal("repair was not atomic", latest.Version, target.Version, active.Version, h.commits)
	}
	if latest.Origin != topology.OriginReconciler || latest.ParentVersion != 1 || latest.ChangedBy != "auto-repair" || latest.Group("shard0").NextReplicaID != 5 || latest.Group("shard0").Replicas[2].ReplicaID != 4 {
		t.Fatalf("wrong repair %+v", latest)
	}
	ex, _ := h.machine.CurrentOperator()
	want := []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter}
	if len(ex.Plan.Steps) != len(want) {
		t.Fatal(ex.Plan)
	}
	for i, k := range want {
		if ex.Plan.Steps[i].Kind != k || ex.Plan.Steps[i].ReplicaID != 4 {
			t.Fatal(ex.Plan)
		}
	}
	assertStatus(t, r, topology.SchedulingRunning, ReconcileReasonApprovedOperator)
}
func TestDriverCASRaceLeavesOperatorVersionStaged(t *testing.T) {
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	h.beforeCommit = func() { h.stage(topology.OriginOperator, 1) }
	tick(t, r)
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonCASConflict)
	if h.machine.Latest().Origin != topology.OriginOperator || h.machine.Target().Version != 1 || h.machine.Active().Version != 1 || h.commits != 0 || h.wakes != 0 {
		t.Fatal("repair overwrote operator CAS")
	}
}

type stubPolicy struct{ decision policy.ScheduleDecision }

func (p *stubPolicy) Schedule(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error) {
	return p.decision, nil
}
func TestDriverRejectsPolicyActionsOutsideAllowList(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	r := newTestDriver(t, h, Settings{})
	r.policy = &stubPolicy{policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: "remove_voter", Identity: storage.Identity{GroupID: "shard0", ReplicaID: 3}}}}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonPolicyRejected)
	if h.commits != 0 || h.wakes != 0 {
		t.Fatal("forbidden action")
	}
}
func TestHealableMatrix(t *testing.T) {
	t.Parallel()
	for _, origin := range []topology.Origin{topology.OriginInternal, topology.OriginOperator, topology.OriginSystem, topology.OriginReconciler, topology.OriginFounding} {
		for _, parentOK := range []bool{true, false} {
			for _, empty := range []bool{true, false} {
				for _, known := range []bool{true, false} {
					t.Run(fmt.Sprintf("%s/%t/%t/%t", origin, parentOK, empty, known), func(t *testing.T) {
						h := newTestHost(t, 1, 2, 3)
						if !known {
							h.health = nil
						}
						if !empty {
							s := h.status["shard0"]
							s.ConfState.Voters = []uint64{1, 2}
							h.status["shard0"] = s
						}
						parent := uint64(1)
						if !parentOK {
							parent = 99
						}
						latest := h.stage(origin, parent)
						r := newTestDriver(t, h, Settings{})
						got, err := r.healable(context.Background(), latest, h.machine.Active())
						want := origin == topology.OriginInternal && parentOK && empty && known
						if err != nil || got != want {
							t.Fatalf("healable=%v/%v want %v", got, err, want)
						}
					})
				}
			}
		}
	}
}
func TestDriverHealsStagedInternal(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	h.stage(topology.OriginInternal, 1)
	r := newTestDriver(t, h, Settings{})
	r.lastRun = r.now()
	tick(t, r)
	assertStatus(t, r, topology.SchedulingRunning, ReconcileReasonApprovedOperator)
	if h.approvals != 1 || h.machine.Target().Version != 2 {
		t.Fatal("internal heal blocked by cooldown")
	}
}
func TestDriverHealRefusals(t *testing.T) {
	for _, mode := range []string{"operator", "laundered", "follower", "plan-error", "apply-error", "nonempty", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			h := newTestHost(t, 1, 2, 3)
			origin, parent := topology.OriginInternal, uint64(1)
			switch mode {
			case "operator":
				origin = topology.OriginOperator
			case "laundered":
				parent = 99
			case "follower":
				h.leader = false
			case "plan-error":
				h.planErr = errors.New("unavailable detail")
			case "apply-error":
				h.approveErr = fault.ErrUnavailable
			case "nonempty":
				s := h.status["shard0"]
				s.ConfState.Voters = []uint64{1, 2}
				h.status["shard0"] = s
			case "unknown":
				h.health = nil
			}
			h.stage(origin, parent)
			r := newTestDriver(t, h, Settings{})
			tick(t, r)
			if mode == "plan-error" {
				assertStatus(t, r, topology.SchedulingError, ReconcileReasonStatusUnavailable)
			} else {
				assertStatus(t, r, topology.SchedulingBlocked, ReconcileReasonStagedLatest)
			}
			if h.approvals != 0 || h.commits != 0 || r.Status().BlockingVersion == nil || *r.Status().BlockingVersion != 2 {
				t.Fatal("heal escaped refusal", r.Status())
			}
		})
	}
}
func TestReconcileTickIsBoundedAndCancelable(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	h.blockViews = true
	r := newTestDriver(t, h, Settings{Interval: 100 * time.Millisecond, TickTimeout: 20 * time.Millisecond})
	start := time.Now()
	err := r.Tick(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatal(err, time.Since(start))
	}
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonStatusUnavailable)
	// Stop must cancel a read already inside the automatic worker, rather than
	// waiting for the configured tick timeout.
	r = newTestDriver(t, h, Settings{Interval: 100 * time.Millisecond, TickTimeout: 90 * time.Millisecond})
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
func TestDriverFollowerResetsStabilityObservations(t *testing.T) {
	for _, mode := range []string{"standby", "staged", "failed", "approved"} {
		t.Run(mode, func(t *testing.T) {
			h := newTestHost(t, 1, 2)
			r := newTestDriver(t, h, Settings{})
			h.leader = false
			if mode != "standby" {
				h.stage(topology.OriginOperator, 1)
			}
			if mode == "failed" || mode == "approved" {
				state := topology.OperatorFailed
				if mode == "approved" {
					state = topology.OperatorPending
				}
				h.operatorOverride = &topology.Operator{Status: topology.OperatorStatus{Version: 2, State: state}}
			}
			tick(t, r)
			if len(r.aliveSince) != 0 || h.healthCalls != 0 || h.commits != 0 {
				t.Fatal("follower sampled or retained health")
			}
			if mode == "standby" {
				h.leader = true
				tick(t, r)
				assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonStabilityWindow)
			}
		})
	}
}
func TestDriverStatusGatesAndStableSince(t *testing.T) {
	for _, mode := range []string{"converged", "stability", "quorum", "cooldown", "status-error", "read-error", "execution-error"} {
		t.Run(mode, func(t *testing.T) {
			h := newTestHost(t, 1, 2)
			r := newTestDriver(t, h, Settings{})
			state, reason := topology.SchedulingIdle, ReconcileReasonConverged
			switch mode {
			case "converged":
				s := h.status["shard0"]
				s.ConfState.Voters = []uint64{1, 2, 3}
				h.status["shard0"] = s
			case "stability":
				r.resetAlive()
				state, reason = topology.SchedulingWaiting, ReconcileReasonStabilityWindow
			case "quorum":
				h.health["n1"] = topology.HealthDead
				h.health["n2"] = topology.HealthDead
				state, reason = topology.SchedulingBlocked, ReconcileReasonQuorumLost
			case "cooldown":
				r.lastRun = r.now()
				state, reason = topology.SchedulingWaiting, ReconcileReasonCooldown
			case "status-error":
				h.statusErr = fault.ErrUnavailable
				state, reason = topology.SchedulingError, ReconcileReasonStatusUnavailable
			case "read-error":
				h.viewErr = fault.ErrUnreadable
				state, reason = topology.SchedulingError, ReconcileReasonStatusUnavailable
			case "execution-error":
				h.operatorErr = fault.ErrUnreadable
				state, reason = topology.SchedulingError, ReconcileReasonStatusUnavailable
			}
			tick(t, r)
			assertStatus(t, r, state, reason)
			since := r.Status().Since
			r.now = func() time.Time { return since.Add(time.Second) }
			tick(t, r)
			if r.Status().Since != since {
				t.Fatal("identical status changed Since")
			}
		})
	}
}
func TestDriverStatusTransitionsAreOwnedAndLoggedOnce(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	r := newTestDriver(t, h, Settings{})
	var buf bytes.Buffer
	r.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	changes := 0
	r.onTransition = func(s topology.SchedulingStatus) {
		changes++
		if s.BlockingVersion != nil {
			*s.BlockingVersion = 99
		}
	}
	h.stage(topology.OriginOperator, 1)
	tick(t, r)
	tick(t, r)
	if strings.Count(buf.String(), "level=WARN") != 1 || changes != 1 {
		t.Fatal(buf.String(), changes)
	}
	s := r.Status()
	*s.BlockingVersion = 99
	if *r.Status().BlockingVersion != 2 {
		t.Fatal("status mutation escaped")
	}
	h.operatorOverride = &topology.Operator{Status: topology.OperatorStatus{ID: "exec-v2-a1", Version: 2, State: topology.OperatorRunning}}
	tick(t, r)
	if strings.Count(buf.String(), "level=INFO") != 1 {
		t.Fatal(buf.String())
	}
	s = r.Status()
	*s.CurrentOperatorID = "changed"
	if *r.Status().CurrentOperatorID != "exec-v2-a1" {
		t.Fatal("execution alias")
	}
	r.transition(topology.SchedulingStatus{State: topology.SchedulingRunning, CurrentAction: &topology.SchedulingActionStatus{ReplicaID: 3}}, "")
	s = r.Status()
	s.CurrentAction.ReplicaID = 99
	if r.Status().CurrentAction.ReplicaID != 3 {
		t.Fatal("action alias")
	}
}
func TestDriverStatusErrorWarnsOnce(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	r := newTestDriver(t, h, Settings{})
	var buf bytes.Buffer
	r.logger = slog.New(slog.NewTextHandler(&buf, nil))
	h.viewErr = errors.New("raw error detail")
	for range 3 {
		tick(t, r)
	}
	if strings.Count(buf.String(), "level=WARN") != 1 || !strings.Contains(buf.String(), "raw error detail") || r.Status().Reason != ReconcileReasonStatusUnavailable {
		t.Fatal(buf.String(), r.Status())
	}
}
func TestDriverStatusConcurrentAccess(t *testing.T) {
	h := newTestHost(t, 1, 2, 3)
	r := newTestDriver(t, h, Settings{})
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
func TestDriverSettingsValidation(t *testing.T) {
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
