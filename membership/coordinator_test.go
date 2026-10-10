package membership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCoordinatorCanceledWaitDoesNotPropose(t *testing.T) {
	c := NewCoordinator()
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	g := &fakeGroup{id: "load", st: statusOf([]uint64{1}, nil, 1, 1, nil), onPropose: func(raftpb.ConfChangeV2) { close(entered); <-release }}
	done := make(chan error, 1)
	go func() { done <- c.Apply(context.Background(), g, knownLive(1), AddLearner(2)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first proposal not admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := c.Apply(ctx, g, knownLive(1), AddLearner(3)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("proposal did not return")
	}
	if g.proposalCount() != 1 {
		t.Fatal("canceled waiter proposed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.locks) != 0 {
		t.Fatal("coordinator retained idle group lock")
	}
}

func TestSafetyRejectsZeroAndCurrentJointConfiguration(t *testing.T) {
	g := &fakeGroup{id: "load", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	if err := (SafetyPolicy{}).Check(g, knownLive(1, 2, 3), AddLearner(0)); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
	g.st.Config.Voters[1] = quorum.MajorityConfig{1: {}, 2: {}}
	if err := (SafetyPolicy{}).Check(g, knownLive(1, 2, 3), AddLearner(4)); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
}
