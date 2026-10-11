//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type schedulerFunc func(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error)

func (f schedulerFunc) Schedule(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
	return f(ctx, in)
}

func TestSchedulingStatusLeaderNamesUseCapturedActive(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	captured := f.g.state().Active()
	replacement := captured.Clone()
	replacement.Group("work").Replicas[2].ReplicaID = 4
	replacement.Group("work").NextReplicaID = 5
	f.commit(t, replacement, true)
	current := f.g.state().Active()
	// Exercise the load observation boundary without opening a meta transport.
	captured.Groups = []topology.Group{*captured.Group("work")}
	current.Groups = []topology.Group{*current.Group("work")}
	c := &statusController{}
	f.g.opts.LoadGroups = c
	for _, tt := range []struct {
		name   string
		active topology.Document
		leader uint64
		want   string
	}{
		{"captured owner", captured, 3, "n3"},
		{"replacement outside captured view", captured, 4, ""},
		{"retired ID in current view", current, 3, ""},
		{"current replacement", current, 4, "n3"},
		{"unknown leader", captured, 0, ""},
		{"unmapped leader", captured, 99, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c.value = topology.GroupStatus{GroupID: "work", Found: true, LeaderID: tt.leader, LeaderNodeName: "peer-supplied-name"}
			got, err := (schedulingHost{f.g}).GlobalStatus(context.Background(), tt.active)
			if err != nil || len(got) != 1 || got["work"].LeaderID != tt.leader || got["work"].LeaderNodeName != tt.want || c.value.LeaderNodeName != "peer-supplied-name" {
				t.Fatal("leader name did not follow the captured Active identity", got, err)
			}
		})
	}
	c.err = fault.ErrUnavailable
	if got, err := (schedulingHost{f.g}).GlobalStatus(context.Background(), captured); err != fault.ErrUnavailable || got != nil {
		t.Fatal("observation error was hidden", got, err)
	}
	c.err = nil
	c.value.GroupID = "another-group"
	if got, err := (schedulingHost{f.g}).GlobalStatus(context.Background(), captured); !errors.Is(err, fault.ErrIdentityMismatch) || got != nil {
		t.Fatal("cross-group observation was accepted", got, err)
	}
}

func TestMetaUsesSelectedSchedulerWithOwnedHoldsAndCallbackDeadline(t *testing.T) {
	t.Parallel()
	type observation struct {
		input    policy.ScheduleInput
		duration time.Duration
	}
	seen := make(chan observation, 1)
	held := []storage.Identity{{NodeName: "n1", ClusterID: "cluster", GroupID: "work", ReplicaID: 9}}
	groups := singleMetaFixtureWith(t, func(o *Options) {
		o.Scheduling = SchedulingOptions{Interval: 100 * time.Millisecond, TickTimeout: 80 * time.Millisecond}
		o.CallbackTimeout = 25 * time.Millisecond
		o.HeldLoads = func() []storage.Identity { return held }
		o.Scheduler = schedulerFunc(func(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
			deadline, _ := ctx.Deadline()
			select {
			case seen <- observation{input: in.Clone(), duration: time.Until(deadline)}:
			default:
			}
			// Callback snapshots are application-owned scratch values.
			in.Held[0].ReplicaID = 100
			in.Active.Nodes[0].Name = "changed"
			return policy.ScheduleDecision{Reason: "application_scheduler"}, nil
		})
	})
	leader := readyPublic(t, groups)
	select {
	case got := <-seen:
		if got.duration <= 0 || got.duration > 25*time.Millisecond || len(got.input.Held) != 1 || got.input.Held[0] != held[0] {
			t.Fatal("selected scheduler lost deadline or hold", got)
		}
		meta := got.input.Status[topology.MetaGroupID]
		if meta.LeaderID == 0 || meta.LeaderNodeName == "" || got.input.Active.MemberID(meta.LeaderNodeName) != meta.LeaderID {
			t.Fatal("scheduler meta leader name was not resolved through its Active view", meta)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selected scheduler was not invoked")
	}
	waitPublic(t, func() bool { return leader.SchedulingStatus().Reason == "application_scheduler" })
	active, err := leader.Active(context.Background())
	if err != nil || active.Nodes[0].Name == "changed" || held[0].ReplicaID != 9 || leader.SchedulingStatus().State != topology.SchedulingIdle {
		t.Fatal("policy inputs shared live state", active, held, err)
	}
}
