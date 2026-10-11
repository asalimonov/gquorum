//go:build functional

package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
)

// Each application participant deliberately supplies a different policy result.
// Leadership chooses the active callback; no replicated policy declaration is
// needed to transfer leadership or continue operating the cluster.
type callerScheduler struct {
	reason string
	calls  atomic.Uint64
}

func (s *callerScheduler) Schedule(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
	if err := ctx.Err(); err != nil {
		return policy.ScheduleDecision{}, err
	}
	s.calls.Add(1)
	return policy.ScheduleDecision{Reason: s.reason}, nil
}

func TestPublicNodeCallerSchedulersFollowLeadershipWithoutAgreementGate(t *testing.T) {
	policies := make([]*callerScheduler, 3)
	c := newAPICluster(t, 3, false, func(i int, o *quorum.NodeOptions) {
		// Give native leadership transfer time to deliver under the race detector.
		o.Meta.Timing.ElectionTicks = 50
		policies[i] = &callerScheduler{reason: "caller-policy-" + o.Identity.NodeName}
		o.Policies.Scheduler = policies[i]
		o.Policies.Scheduling = quorum.SchedulingOptions{Interval: 100 * time.Millisecond, Timeout: 70 * time.Millisecond, Cooldown: 10 * time.Millisecond}
	})
	c.startAll(t)
	defer func() {
		if t.Failed() {
			for i, n := range c.nodes {
				t.Logf("node %s calls=%d status=%+v scheduling=%+v", n.Identity().NodeName, policies[i].calls.Load(), n.Status(), n.SchedulingStatus())
			}
		}
	}()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	first := c.leader(t)
	var next quorum.Node
	var nextPolicy *callerScheduler
	for i, n := range c.nodes {
		if n == first {
			apiEventually(t, func() bool { return policies[i].calls.Load() > 0 && n.SchedulingStatus().Reason == policies[i].reason })
		} else if next == nil {
			next, nextPolicy = n, policies[i]
		}
	}
	before, err := first.Cluster().Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool {
		status := first.Status().Meta
		progress, ok := status.Progress[next.Identity().MemberID]
		return ok && progress.RecentActive && progress.Match >= status.Commit
	})
	if err := first.Cluster().TransferLeader(ctx, topology.MetaGroupID, next.Identity().NodeName); err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool {
		return next.Cluster().IsLeader() && nextPolicy.calls.Load() > 0 && next.SchedulingStatus().Reason == nextPolicy.reason
	})
	for _, n := range c.nodes {
		if !n.Status().Ready || !n.OwnSessionLive() {
			t.Fatal("policy configuration interrupted participation", n.Status())
		}
		after, err := n.Cluster().Latest(ctx)
		if err != nil || after.Version != before.Version {
			t.Fatal("policy change staged topology", after, err)
		}
	}
	if err := next.SetSchedulingEnabled(ctx, false); err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool { return next.SchedulingStatus().State == topology.SchedulingDisabled })
	if err := next.SetSchedulingEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	apiEventually(t, func() bool { return next.SchedulingStatus().Reason == nextPolicy.reason })
}
