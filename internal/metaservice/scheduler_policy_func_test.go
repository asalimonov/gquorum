//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type schedulerFunc func(context.Context, scheduling.ScheduleInput) (scheduling.ScheduleDecision, error)

func (f schedulerFunc) Schedule(ctx context.Context, in scheduling.ScheduleInput) (scheduling.ScheduleDecision, error) {
	return f(ctx, in)
}

func TestSchedulingStatusLeaderNamesUseCapturedProgress(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	captured := f.g.state().Target()
	replacement := captured.Clone()
	replacement.Group("work").Replicas[2].ReplicaID = 4
	replacement.Group("work").NextReplicaID = 5
	f.commit(t, replacement, true)
	current := f.g.state().Target()
	c := &statusController{}
	f.g.opts.LoadGroups = c
	for _, tt := range []struct {
		name       string
		doc        topology.Document
		leader     uint64
		want       string
		observeErr error
		wrongGroup bool
	}{
		{name: "captured owner", doc: captured, leader: 3, want: "n3"},
		{name: "replacement outside capture", doc: captured, leader: 4},
		{name: "retired ID", doc: current, leader: 3},
		{name: "current replacement", doc: current, leader: 4, want: "n3"},
		{name: "unknown leader", doc: captured},
		{name: "unmapped leader", doc: captured, leader: 99},
		{name: "unavailable", doc: captured, observeErr: fault.ErrUnavailable},
		{name: "foreign group", doc: captured, wrongGroup: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c.err = tt.observeErr
			c.value = topology.GroupStatus{GroupID: "work", Found: true, LeaderID: tt.leader, LeaderNodeName: "peer-supplied-name"}
			if tt.wrongGroup {
				c.value.GroupID = "foreign"
			}
			read := metaengine.PlanRead{Latest: tt.doc.Clone(), Target: tt.doc.Clone(),
				Scopes: []metaengine.ScopeExpectation{{GroupID: "work", SourceVersion: tt.doc.Version}},
				Input: topology.PlanInput{Approved: tt.doc.Clone(), Groups: map[string]topology.GroupProgress{
					"work": {GroupID: "work", Converged: tt.doc.Group("work"), ConvergedVersion: tt.doc.Version},
				}}}
			got, err := f.g.capturePlan(t.Context(), metaengine.QueryRequest{},
				func(context.Context, metaengine.QueryRequest) (metaengine.PlanRead, error) { return read, nil })
			if tt.observeErr != nil || tt.wrongGroup {
				want := tt.observeErr
				if tt.wrongGroup {
					want = fault.ErrIdentityMismatch
				}
				if !errors.Is(err, want) {
					t.Fatal("observation refusal lost", err)
				}
				return
			}
			observed := got.Input.Groups["work"].Observation
			if err != nil || observed == nil || observed.LeaderID != tt.leader || observed.LeaderNodeName != tt.want ||
				c.value.LeaderNodeName != "peer-supplied-name" {
				t.Fatal("leader name did not follow captured exact identity", observed, err)
			}
		})
	}
}

func TestMetaUsesSelectedSchedulerWithOwnedHoldsAndCallbackDeadline(t *testing.T) {
	t.Parallel()
	type observation struct {
		input       scheduling.ScheduleInput
		duration    time.Duration
		hasDeadline bool
	}
	seen := make(chan observation, 1)
	held := []storage.Identity{{NodeName: "n1", ClusterID: "cluster", GroupID: "work", ReplicaID: 9}}
	groups := singleMetaFixtureWith(t, func(o *Options) {
		o.Scheduling = SchedulingOptions{Interval: 100 * time.Millisecond, TickTimeout: 80 * time.Millisecond}
		o.CallbackTimeout = 25 * time.Millisecond
		o.HeldLoads = func() []storage.Identity { return held }
		o.RepairPolicy = schedulerFunc(func(ctx context.Context, in scheduling.ScheduleInput) (scheduling.ScheduleDecision, error) {
			if !in.Status[topology.MetaGroupID].Found {
				return scheduling.ScheduleDecision{Reason: "waiting_for_observation"}, nil
			}
			deadline, hasDeadline := ctx.Deadline()
			select {
			case seen <- observation{input: in.Clone(), duration: time.Until(deadline), hasDeadline: hasDeadline}:
			default:
			}
			// Callback snapshots are application-owned scratch values.
			in.Held[0].ReplicaID = 100
			in.Target.Nodes[0].Name = "changed"
			return scheduling.ScheduleDecision{Reason: "application_scheduler"}, nil
		})
	})
	leader := readyPublic(t, groups)
	select {
	case got := <-seen:
		// Descheduling may consume the deadline before callback inspection.
		// Its presence and configured upper bound are the contract.
		if !got.hasDeadline || got.duration > 25*time.Millisecond || len(got.input.Held) != 1 || got.input.Held[0] != held[0] {
			t.Fatal("selected scheduler lost deadline or hold", got)
		}
		meta := got.input.Status[topology.MetaGroupID]
		if meta.LeaderID == 0 || meta.LeaderNodeName == "" || got.input.Target.MemberID(meta.LeaderNodeName) != meta.LeaderID {
			t.Fatal("scheduler meta leader name was not resolved through captured progress", meta)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selected scheduler was not invoked")
	}
	waitPublic(t, func() bool { return leader.SchedulingStatus().Reason == "application_scheduler" })
	target, err := leader.Target(context.Background())
	if err != nil || target.Nodes[0].Name == "changed" || held[0].ReplicaID != 9 || leader.SchedulingStatus().State != topology.SchedulingIdle {
		t.Fatal("policy inputs shared live state", target, held, err)
	}
}
