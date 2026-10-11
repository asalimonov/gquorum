package metaservice

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type schedulerFunc func(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error)

func (f schedulerFunc) Schedule(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
	return f(ctx, in)
}

func TestMetaUsesSelectedSchedulerWithOwnedHoldsAndCallbackDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	type observation struct {
		input    policy.ScheduleInput
		duration time.Duration
	}
	seen := make(chan observation, 1)
	held := []storage.Identity{{NodeName: "n1", ClusterID: "cluster", GroupID: "work", ReplicaID: 9}}
	groups := publicFoundingClusterWith(t, 1, func(o *Options) {
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
	case <-time.After(5 * time.Second):
		t.Fatal("selected scheduler was not invoked")
	}
	waitPublic(t, func() bool { return leader.SchedulingStatus().Reason == "application_scheduler" })
	active, err := leader.Active(context.Background())
	if err != nil || active.Nodes[0].Name == "changed" || held[0].ReplicaID != 9 || leader.SchedulingStatus().State != topology.SchedulingIdle {
		t.Fatal("policy inputs shared live state", active, held, err)
	}
}
