//go:build functional

package scheduling

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type scopedTestHost struct {
	*testHost
	read        metaengine.PlanRead
	beforePlan  func()
	unreadable  string
	attempts    []RepairCommit
	commitError error
}

func (h *scopedTestHost) ReadRepair(ctx context.Context, group string) (metaengine.PlanRead, error) {
	if err := ctx.Err(); err != nil {
		return metaengine.PlanRead{}, err
	}
	if group != "" && group == h.unreadable {
		return metaengine.PlanRead{}, fault.ErrUnavailable
	}
	out := metaengine.PlanRead{Latest: h.read.Latest.Clone(), Target: h.read.Target.Clone(), Input: topology.PlanInput{Approved: h.read.Input.Approved.Clone(), Groups: make(map[string]topology.GroupProgress), Nodes: make(map[string]topology.NodeProgress)}}
	for id, row := range h.read.Input.Groups {
		out.Input.Groups[id] = row.Clone()
	}
	for id, row := range h.read.Input.Nodes {
		out.Input.Nodes[id] = row.Clone()
	}
	out.Input.Native = make(map[string]topology.NativeState)
	for _, scope := range h.read.Scopes {
		if group != "" && group != scope.GroupID {
			continue
		}
		out.Scopes = append(out.Scopes, scope)
		status := h.status[scope.GroupID].Clone()
		row := out.Input.Groups[scope.GroupID]
		row.Observation = &status
		out.Input.Groups[scope.GroupID] = row
		if len(status.ConfState.Voters) != 0 {
			out.Input.Native[scope.GroupID] = topology.NativeState{GroupID: scope.GroupID, ReplicaID: status.ConfState.Voters[0], Applied: max(10, status.Applied), ConfState: status.ConfState}
		}
	}
	return out, nil
}

func (h *scopedTestHost) PlanRepair(_ context.Context, read metaengine.PlanRead, doc topology.Document) (topology.Plan, error) {
	if h.beforePlan != nil {
		h.beforePlan()
	}
	return topology.BuildPlanPreferred(read.Input, doc, topology.PlanOptions{Rules: topology.Rules{SchemaVersion: 1, Limits: h.limits.WithDefaults()}})
}

func (h *scopedTestHost) CommitRepair(_ context.Context, c RepairCommit) (topology.OperationID, error) {
	h.attempts = append(h.attempts, c)
	return "repair", h.commitError
}
func (h *scopedTestHost) CommitVoterChange(ctx context.Context, _ topology.VoterChange, c RepairCommit) (topology.OperationID, error) {
	return h.CommitRepair(ctx, c)
}

func newScopedHost(t *testing.T) *scopedTestHost {
	t.Helper()
	legacy := newTestHost(t, 1, 2)
	h := &scopedTestHost{testHost: legacy}
	doc := legacy.machine.Target()
	b := *doc.Group("shard0")
	b.ID, b.Replicas = "b", slices.Clone(b.Replicas)
	doc.Groups = append(doc.Groups, b)
	h.read = metaengine.PlanRead{Target: doc.Clone(), Latest: doc.Clone(), Input: topology.PlanInput{Approved: doc.Clone(), Groups: make(map[string]topology.GroupProgress), Nodes: make(map[string]topology.NodeProgress)}}
	for _, g := range doc.Groups {
		group := g
		h.read.Input.Groups[g.ID] = topology.GroupProgress{GroupID: g.ID, ConvergedVersion: 1, Converged: &group}
		h.read.Scopes = append(h.read.Scopes, metaengine.ScopeExpectation{GroupID: g.ID, SourceVersion: 1})
		status := h.status[g.ID].Clone()
		if g.ID == "b" {
			status = h.status["shard0"].Clone()
		}
		status.GroupID, status.SourceVersion = g.ID, 1
		h.status[g.ID] = status
	}
	for _, n := range doc.Nodes {
		node := n
		h.read.Input.Nodes[n.Name] = topology.NodeProgress{NodeName: n.Name, MemberID: doc.MemberID(n.Name), ConvergedVersion: 1, Converged: &node}
	}
	h.observations.CollectorRunID, h.observations.ObserverInstanceID, h.observations.IsCollectorLeader = "epoch", "process", true
	return h
}

func missingDecision(in schedulingapi.ScheduleInput) schedulingapi.ScheduleDecision {
	return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica,
		Identity: storage.Identity{ClusterID: in.Target.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}
}
func scopedDriver(t *testing.T, h *scopedTestHost, policy schedulingapi.RepairPolicy) *Scheduler {
	t.Helper()
	r, err := New(Config{Host: h, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	r.aliveSince["n3"] = time.Now().Add(-time.Hour)
	return r
}

func TestScopedSchedulerRefreshesOnlyIndependentCallbackChanges(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unrelated", "source", "owner", "declaration", "staged_overlap", "native", "leadership"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			h := newScopedHost(t)
			r := scopedDriver(t, h, schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				switch kind {
				case "unrelated":
					h.read.Target.Version = 3
					h.read.Target.Group("b").Labels = map[string]string{"approved": "new"}
					h.read.Input.Approved = h.read.Target.Clone()
					h.read.Latest = h.read.Target.Clone()
					h.read.Latest.Version = 4
					h.read.Latest.Group("b").Labels = map[string]string{"staged": "intent"}
					row := h.read.Input.Groups["b"]
					row.OperationID = "independent-operation"
					h.read.Input.Groups["b"] = row
				case "source":
					row := h.read.Input.Groups["shard0"]
					row.ConvergedVersion++
					h.read.Input.Groups["shard0"] = row
				case "owner":
					row := h.read.Input.Groups["shard0"]
					row.OperationID = "new-owner"
					h.read.Input.Groups["shard0"] = row
				case "declaration":
					h.read.Target.Group("shard0").Labels = map[string]string{"changed": "yes"}
					h.read.Input.Approved = h.read.Target.Clone()
				case "staged_overlap":
					h.read.Latest.Version = 2
					h.read.Latest.Group("shard0").Labels = map[string]string{"staged": "conflict"}
				case "native":
					status := h.status["shard0"]
					status.ConfState.Voters = []uint64{1, 2, 3}
					h.status["shard0"] = status
				case "leadership":
					h.observations.CollectorRunID = "new-epoch"
				}
				// The callback's owned input cannot override the driver's capture.
				in.Groups["shard0"] = topology.GroupProgress{}
				return missingDecision(in), nil
			}))
			if err := r.Tick(t.Context()); err != nil {
				t.Fatal(err)
			}
			if kind != "unrelated" {
				if len(h.attempts) != 0 {
					t.Fatal("stale action committed", kind, h.attempts)
				}
				if kind == "staged_overlap" && r.Status().Reason != "staged_conflict" {
					t.Fatal(r.Status())
				}
				return
			}
			if len(h.attempts) != 1 {
				t.Fatal("independent repair blocked", r.Status())
			}
			c := h.attempts[0]
			if c.ExpectedVersion != 4 || c.ExpectedTarget != 3 || c.Document.ParentVersion != 3 || c.Document.Version != 5 ||
				!reflect.DeepEqual(c.Document.Group("b"), h.read.Target.Group("b")) || len(c.Scopes) != 1 || c.Scopes[0].GroupID != "shard0" {
				t.Fatal(c)
			}
		})
	}
}

func TestScopedSchedulerRechecksNativeAfterPlanning(t *testing.T) {
	t.Parallel()
	h := newScopedHost(t)
	h.beforePlan = func() {
		status := h.status["shard0"]
		status.ConfState.Voters = []uint64{1, 2, 3}
		h.status["shard0"] = status
	}
	r := scopedDriver(t, h, schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		return missingDecision(in), nil
	}))
	if err := r.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(h.attempts) != 0 {
		t.Fatal("native change during planning committed")
	}
}

func TestScopedSchedulerRotatesPastRefusalAndUnavailableGroup(t *testing.T) {
	t.Parallel()
	h := newScopedHost(t)
	h.unreadable = topology.MetaGroupID
	h.commitError = fault.ErrVersionConflict
	r := scopedDriver(t, h, nil)
	for range 2 {
		if err := r.Tick(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.attempts) != 2 || h.attempts[0].Scopes[0].GroupID == h.attempts[1].Scopes[0].GroupID {
		t.Fatal(h.attempts, r.Status())
	}
}

func TestScopedSchedulerCallbackCancellationRetainsSerialization(t *testing.T) {
	t.Parallel()
	h := newScopedHost(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	r := scopedDriver(t, h, schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		calls.Add(1)
		close(entered)
		<-release
		return missingDecision(in), nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Tick(ctx) }()
	<-entered
	cancel()
	next, finish := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer finish()
	if err := r.Tick(next); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(h.attempts) != 0 {
		t.Fatal("overlap or late callback mutation")
	}
}

func TestScopedSchedulerDoesNotReplicateOrRepeatUnchangedStagedBlocker(t *testing.T) {
	t.Parallel()
	h := newScopedHost(t)
	h.read.Latest.Version = 2
	h.read.Latest.Group("shard0").Labels = map[string]string{"staged": "overlap"}
	r := scopedDriver(t, h, schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		return missingDecision(in), nil
	}))
	transitions := 0
	r.onTransition = func(topology.SchedulingStatus) { transitions++ }
	for range 2 {
		if err := r.Tick(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	// The second tick has the same scoped refusal. No proposal/blocker is persisted.
	if len(h.attempts) != 0 || transitions != 1 || r.Status().CurrentAction.GroupID != "shard0" {
		t.Fatal(transitions, h.attempts, r.Status())
	}
}

func TestScopedSchedulerKeepsAliveHistoryOnlyForSameMember(t *testing.T) {
	t.Parallel()
	h := newScopedHost(t)
	r := scopedDriver(t, h, nil)
	at := time.Now()
	r.sampleAlive(h.read.Target, h.health, at)
	first := r.aliveSince["n3"]
	changed := h.read.Target.Clone()
	changed.Version++
	changed.Group("b").Labels = map[string]string{"independent": "yes"}
	r.sampleAlive(changed, h.health, at.Add(time.Minute))
	if r.aliveSince["n3"] != first {
		t.Fatal("independent edit reset alive history")
	}
	changed.MetaGroup().Replicas[2].ReplicaID = 4
	r.sampleAlive(changed, h.health, at.Add(2*time.Minute))
	if r.aliveSince["n3"] == first {
		t.Fatal("successor inherited former member stability")
	}
}

func TestScopedVoterRepairPreservesStagingAndRechecksCollectedEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"independent", "native", "sample_after_plan"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			legacy, _ := voterHostFixture(t)
			doc := legacy.machine.Target()
			other := topology.Group{ID: "other", NextReplicaID: 4, Replicas: slices.Clone(doc.MetaGroup().Replicas[:3])}
			doc.Groups = append(doc.Groups, other)
			input := topology.PlanInput{Approved: doc.Clone(), Groups: map[string]topology.GroupProgress{}, Nodes: map[string]topology.NodeProgress{}}
			for _, group := range doc.Groups {
				g := group
				input.Groups[g.ID] = topology.GroupProgress{GroupID: g.ID, ConvergedVersion: 1, Converged: &g}
			}
			for _, node := range doc.Nodes {
				n := node
				input.Nodes[n.Name] = topology.NodeProgress{NodeName: n.Name, MemberID: doc.MemberID(n.Name), ConvergedVersion: 1, Converged: &n}
			}
			h := &scopedTestHost{testHost: legacy, read: metaengine.PlanRead{Latest: doc.Clone(), Target: doc.Clone(), Input: input,
				Scopes: []metaengine.ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1}, {GroupID: "other", SourceVersion: 1}}}}
			h.read.Latest.Version = 2
			h.read.Latest.Group("other").Labels = map[string]string{"staged": "keep"}
			status := h.status[topology.MetaGroupID]
			status.SourceVersion = 1
			h.status[topology.MetaGroupID] = status
			h.observations.Groups[0].SourceVersion = 1
			if mode == "sample_after_plan" {
				h.beforePlan = func() { h.observations.Groups[0].SourceVersion = 2 }
			}
			r := scopedDriver(t, h, schedulerFunc(func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				if mode == "native" {
					st := h.status[topology.MetaGroupID]
					st.ConfState.Learners = []uint64{5}
					h.status[topology.MetaGroupID] = st
				}
				return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.ReconfigureVoters,
					VoterChange: &topology.VoterChange{GroupID: topology.MetaGroupID, Promote: &topology.ReplicaKey{NodeName: "n4", ReplicaID: 4}, Demote: &topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}}}}, nil
			}))
			if err := r.Tick(t.Context()); err != nil {
				t.Fatal(err)
			}
			if mode != "independent" {
				if len(h.attempts) != 0 {
					t.Fatal("stale voter evidence committed")
				}
				return
			}
			if len(h.attempts) != 1 || h.attempts[0].ExpectedVersion != 2 || h.attempts[0].ExpectedTarget != 1 ||
				len(h.attempts[0].Scopes) != 1 || h.attempts[0].Scopes[0].GroupID != topology.MetaGroupID {
				t.Fatal(h.attempts, r.Status())
			}
		})
	}
}

func TestScopedSchedulerAllowsAppliedProgressButRefusesChangedLeaderEpoch(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"progress", "leader_epoch"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			h := newScopedHost(t)
			h.beforePlan = func() {
				status := h.status["shard0"]
				status.Applied = 100
				if mode == "leader_epoch" {
					status.Term++
				}
				h.status["shard0"] = status
			}
			r := scopedDriver(t, h, schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				return missingDecision(in), nil
			}))
			if err := r.Tick(t.Context()); err != nil {
				t.Fatal(err)
			}
			if (len(h.attempts) == 1) != (mode == "progress") {
				t.Fatal(mode, h.attempts, r.Status())
			}
		})
	}
}
