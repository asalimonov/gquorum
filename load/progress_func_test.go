//go:build functional

package load_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestGroupWatchReportsAppliedIndex(t *testing.T) {
	t.Parallel()
	ctx, owner, _ := observationFixture(t)
	g := owner.Group()
	watch, err := g.Watch(ctx)
	require(t, err)
	require(t, owner.Start(ctx))
	ready := nextObservation(ctx, t, watch, func(o load.Observation) bool { return o.IsLeader })
	if _, err := g.Propose(ctx, load.Proposal{RequestID: "applied", Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	applied := nextObservation(ctx, t, watch, func(o load.Observation) bool { return o.AppliedIndex > ready.AppliedIndex })
	if applied.Revision <= ready.Revision || applied.RunID != ready.RunID {
		t.Fatal("applied index did not advance the same run's revision", ready, applied)
	}
	if status := g.Status(); status.Applied < applied.AppliedIndex {
		t.Fatal("observation ahead of status", status.Applied, applied.AppliedIndex)
	}
}

func TestLeadershipReplicasReportsLeaderProgress(t *testing.T) {
	t.Parallel()
	ctx, owner, opts := observationFixture(t)
	require(t, owner.Start(ctx))
	g := owner.Group()
	retained := make(chan *load.Leadership, 1)
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- g.RunWhileLeader(child, func(scope context.Context, l *load.Leadership) error {
			replicas, err := l.Replicas(scope)
			if err != nil {
				return err
			}
			if len(replicas) != 1 {
				t.Errorf("replicas = %+v", replicas)
				return nil
			}
			r := replicas[0]
			if r.ReplicaID != opts.Identity.ReplicaID || r.NodeName != opts.Identity.NodeName || !r.Voter || !r.Self || !r.RecentActive || r.Match == 0 {
				t.Errorf("unexpected progress %+v", r)
			}
			retained <- l
			return nil
		})
	}()
	var l *load.Leadership
	select {
	case l = <-retained:
	case err := <-done:
		t.Fatal("supervisor ended early", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := l.Replicas(ctx); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal("retained leadership still reports progress", err)
	}
	var zero *load.Leadership
	if _, err := zero.Replicas(ctx); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
