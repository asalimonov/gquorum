//go:build functional

package load_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func observationFixture(t *testing.T) (context.Context, *load.Standalone, load.StandaloneOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	prewrite := false
	store, err := walstore.Open(walstore.Options{RootDir: t.TempDir(), NodeName: "a", ClusterID: "cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite})
	require(t, err)
	t.Cleanup(func() { require(t, store.Close()) })
	options := load.StandaloneOptions{
		Identity: storage.Identity{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 1}, InstanceID: "process-1",
		Mode: load.Founding, Peers: []raft.Peer{{ID: 1}}, Store: store, StateMachine: &deduplicatingMachine{}, Transport: transport{},
		Engine: load.EngineOptions{MaxWatchers: 2, Timing: load.Timing{TickInterval: 10 * time.Millisecond}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	owner, err := load.NewStandalone(options)
	require(t, err)
	t.Cleanup(func() { require(t, owner.Stop(context.Background())) })
	return ctx, owner, options
}

func nextObservation(ctx context.Context, t *testing.T, watch <-chan load.Observation, match func(load.Observation) bool) load.Observation {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case o, open := <-watch:
			if !open {
				t.Fatal("observation closed before expected state", o)
			}
			if match(o) {
				return o
			}
		}
	}
}

func TestGroupWatchExactRunAndTerminalState(t *testing.T) {
	t.Parallel()
	ctx, owner, opts := observationFixture(t)
	g := owner.Group()
	watch, err := g.Watch(ctx)
	require(t, err)
	initial := nextObservation(ctx, t, watch, func(load.Observation) bool { return true })
	if initial.State != load.ReplicaStarting || initial.Ready || initial.RunID == "" || initial.InstanceID != opts.InstanceID || initial.Identity != opts.Identity {
		t.Fatal(initial)
	}
	slow, err := g.Watch(ctx)
	require(t, err)
	if _, err := g.Watch(ctx); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	require(t, owner.Start(ctx))
	ready := nextObservation(ctx, t, watch, func(o load.Observation) bool { return o.IsLeader })
	if !ready.Ready || !ready.IsVoter || ready.Term == 0 || ready.Revision <= initial.Revision {
		t.Fatal(ready)
	}
	require(t, owner.Stop(ctx))
	final := nextObservation(ctx, t, slow, func(o load.Observation) bool { return o.State == load.ReplicaStopped })
	if final.Ready || final.IsLeader || final.RunID != initial.RunID || final.Revision <= ready.Revision {
		t.Fatal(final)
	}
	if _, open := <-slow; open {
		t.Fatal("final observation not closed")
	}
	late, err := g.Watch(ctx)
	require(t, err)
	if o := <-late; o != final {
		t.Fatal("late watch not terminal", o, final)
	}
	opts.Mode, opts.Peers, opts.StateMachine = load.Joining, nil, &deduplicatingMachine{}
	successor, err := load.NewStandalone(opts)
	require(t, err)
	defer func() { require(t, successor.Stop(context.Background())) }()
	next, err := successor.Group().Watch(ctx)
	require(t, err)
	fresh := <-next
	if fresh.RunID == initial.RunID || fresh.Identity != initial.Identity {
		t.Fatal("restart identity", fresh)
	}
	old, err := g.Watch(ctx)
	require(t, err)
	if o := <-old; o != final {
		t.Fatal("old watch redirected", o)
	}
}

func TestLeadershipDirectCancellationAndJoinAcrossTerms(t *testing.T) {
	t.Parallel()
	ctx, owner, _ := observationFixture(t)
	g := owner.Group()
	watch, err := g.Watch(ctx)
	require(t, err)
	require(t, owner.Start(ctx))
	var calls atomic.Int32
	entered := make(chan *load.Leadership, 2)
	canceled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	supervisorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- g.RunWhileLeader(supervisorCtx, func(ctx context.Context, l *load.Leadership) error {
			n := calls.Add(1)
			entered <- l
			<-ctx.Done()
			if n == 1 {
				close(canceled)
				<-release
			}
			return ctx.Err()
		})
	}()
	var first *load.Leadership
	select {
	case first = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err := first.Propose(ctx, load.Proposal{RequestID: "scoped", Data: []byte("value")})
	require(t, err)
	if result.Code != "ok" {
		t.Fatal(result)
	}
	if err := g.RunWhileLeader(ctx, func(context.Context, *load.Leadership) error { return nil }); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overlapping supervisor", err)
	}
	// A protocol double delivers a higher-term heartbeat. There is still just one
	// real participant; this tests local loss/reelection, not remote quorum.
	require(t, owner.Step(ctx, raftpb.Message{From: 2, To: 1, Type: raftpb.MsgHeartbeat, Term: first.Term() + 1}))
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("leadership cancellation depended on watch consumption")
	}
	if _, err := first.Propose(ctx, load.Proposal{Data: []byte("stale")}); err == nil {
		t.Fatal("lost epoch accepted a proposal")
	}
	nextObservation(ctx, t, watch, func(o load.Observation) bool { return o.IsLeader && o.Term > first.Term() })
	if calls.Load() != 1 {
		t.Fatal("new callback overlapped ignored cancellation")
	}
	release <- struct{}{}
	var second *load.Leadership
	select {
	case second = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if second.Term() <= first.Term() {
		t.Fatal("epoch did not advance")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("supervisor did not join")
	}
	if _, err := second.Propose(ctx, load.Proposal{}); err == nil {
		t.Fatal("retained callback handle still usable")
	}
}

func TestLeadershipCallbackCompletionAndErrors(t *testing.T) {
	t.Parallel()
	ctx, owner, _ := observationFixture(t)
	require(t, owner.Start(ctx))
	g := owner.Group()
	expected := errors.New("application worker failed")
	if err := g.RunWhileLeader(ctx, func(context.Context, *load.Leadership) error { return expected }); !errors.Is(err, expected) {
		t.Fatal(err)
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var calls atomic.Int32
	returned := make(chan *load.Leadership, 1)
	done := make(chan error, 1)
	go func() {
		done <- g.RunWhileLeader(child, func(_ context.Context, l *load.Leadership) error { calls.Add(1); returned <- l; return nil })
	}()
	var l *load.Leadership
	select {
	case l = <-returned:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require(t, g.ReadBarrier(ctx))
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if calls.Load() != 1 {
		t.Fatal("nil return restarted same term")
	}
	if _, err := l.Propose(ctx, load.Proposal{}); err == nil {
		t.Fatal("completed callback retained authority")
	}
}
