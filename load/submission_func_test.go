//go:build functional

package load_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestSubmissionStandaloneExactRunAndLogicalID(t *testing.T) {
	t.Parallel()
	ctx, owner, opts := observationFixture(t)
	g := owner.Group()
	for _, id := range []string{"", " \t", "bad\x00id", string([]byte{0xff})} {
		s, err := g.Propose(ctx, load.Proposal{RequestID: id})
		if !errors.Is(err, fault.ErrInvalid) || s.Disposition != load.SubmissionNotSubmitted || s.AttemptID == "" {
			t.Fatal(s, err)
		}
	}
	require(t, owner.Start(ctx))
	require(t, g.WaitReady(ctx))
	waitLeader(ctx, t, g)
	p := load.Proposal{RequestID: " exact/операция ", Data: []byte("value")}
	first, err := g.Propose(ctx, p)
	require(t, err)
	second, err := g.Propose(ctx, p)
	require(t, err)
	if first.RequestID != p.RequestID || second.RequestID != p.RequestID || first.AttemptID == second.AttemptID ||
		first.Index == second.Index || first.Identity != opts.Identity || first.RunID == "" || first.RunID != second.RunID {
		t.Fatal(first, second)
	}
	require(t, owner.Stop(ctx))
	stopped, err := g.Propose(ctx, p)
	if !errors.Is(err, fault.ErrStopped) || stopped.Disposition != load.SubmissionNotSubmitted ||
		stopped.RunID != first.RunID || stopped.Identity != first.Identity {
		t.Fatal(stopped, err)
	}
	opts.Mode, opts.Peers, opts.StateMachine = load.Joining, nil, &deduplicatingMachine{}
	next, err := load.NewStandalone(opts)
	require(t, err)
	defer func() { require(t, next.Stop(context.Background())) }()
	require(t, next.Start(ctx))
	require(t, next.Group().WaitReady(ctx))
	waitLeader(ctx, t, next.Group())
	fresh, err := next.Group().Propose(ctx, p)
	require(t, err)
	if fresh.RunID == first.RunID || fresh.Identity != first.Identity {
		t.Fatal(first, fresh)
	}
	old, err := g.Propose(ctx, p)
	if !errors.Is(err, fault.ErrStopped) || old.RunID != first.RunID {
		t.Fatal(old, err)
	}
}

func TestSubmissionExpiredLeadershipStopsRetry(t *testing.T) {
	t.Parallel()
	ctx, owner, _ := observationFixture(t)
	require(t, owner.Start(ctx))
	require(t, owner.Group().WaitReady(ctx))
	waitLeader(ctx, t, owner.Group())
	type scope struct {
		leader *load.Leadership
		ctx    context.Context
	}
	captured := make(chan scope, 1)
	supervisor, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- owner.Group().RunWhileLeader(supervisor, func(ctx context.Context, l *load.Leadership) error {
			captured <- scope{leader: l, ctx: ctx}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	var old scope
	select {
	case old = <-captured:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-old.ctx.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	calls := 0
	s, err := load.ProposeRetry(ctx, func(call context.Context, p load.Proposal) (load.Submission, error) {
		calls++
		return old.leader.Propose(call, p)
	}, load.Proposal{RequestID: "expired", Data: []byte("must not apply")}, load.ProposalRetryOptions{})
	if calls != 1 || !errors.Is(err, fault.ErrStopped) || s.Disposition != load.SubmissionNotSubmitted || s.CanResubmit(err) {
		t.Fatal(s, err, calls)
	}
}
