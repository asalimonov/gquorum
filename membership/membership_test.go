package membership

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type fakeGroup struct {
	id         string
	st         raft.Status
	leader     bool
	proposeErr error
	onPropose  func(raftpb.ConfChangeV2)

	mu       sync.Mutex
	proposed []raftpb.ConfChangeV2
}

func (f *fakeGroup) GroupID() string     { return f.id }
func (f *fakeGroup) IsLeader() bool      { return f.leader }
func (f *fakeGroup) Status() raft.Status { return f.st }
func (f *fakeGroup) ProposeConfChangeV2(_ context.Context, cc raftpb.ConfChangeV2) error {
	if f.onPropose != nil {
		f.onPropose(cc)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proposed = append(f.proposed, cc)
	return f.proposeErr
}
func (f *fakeGroup) TransferLeader(context.Context, uint64) error { return nil }

func (f *fakeGroup) proposalCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.proposed)
}

type fakeHealth struct {
	live  map[uint64]bool
	known bool
}

func (f fakeHealth) Live(string) (map[uint64]bool, bool) { return f.live, f.known }

func statusOf(voters, learners []uint64, leader, commit uint64, match map[uint64]uint64) raft.Status {
	mc := quorum.MajorityConfig{}
	for _, voter := range voters {
		mc[voter] = struct{}{}
	}
	cfg := tracker.Config{Voters: quorum.JointConfig{mc}}
	if len(learners) > 0 {
		cfg.Learners = make(map[uint64]struct{}, len(learners))
		for _, learner := range learners {
			cfg.Learners[learner] = struct{}{}
		}
	}
	progress := make(map[uint64]tracker.Progress, len(match))
	for id, replicaMatch := range match {
		progress[id] = tracker.Progress{Match: replicaMatch, RecentActive: true}
	}
	st := raft.Status{Config: cfg, Progress: progress}
	st.Lead = leader
	st.Commit = commit
	return st
}

func knownLive(ids ...uint64) fakeHealth {
	live := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		live[id] = true
	}
	return fakeHealth{live: live, known: true}
}

func TestChangeConstructorsUseLearnerThenVoterGrowth(t *testing.T) {
	t.Parallel()
	add := AddLearner(7)
	if len(add.Changes) != 1 || add.Changes[0].Type != raftpb.ConfChangeAddLearnerNode || add.Changes[0].NodeID != 7 {
		t.Fatalf("AddLearner = %+v", add)
	}
	if add.Transition != raftpb.ConfChangeTransitionAuto {
		t.Fatalf("transition = %v, want Auto", add.Transition)
	}
	if got := PromoteLearner(7).Changes[0].Type; got != raftpb.ConfChangeAddNode {
		t.Fatalf("PromoteLearner type = %v, want AddNode", got)
	}
	if got := RemoveReplica(7).Changes[0].Type; got != raftpb.ConfChangeRemoveNode {
		t.Fatalf("RemoveReplica type = %v, want RemoveNode", got)
	}
	if got := DemoteVoter(7).Changes[0].Type; got != raftpb.ConfChangeAddLearnerNode {
		t.Fatalf("DemoteVoter type = %v, want AddLearnerNode", got)
	}
}

func TestSafetyAllowsLearnerAddWithUnknownHealth(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	if err := (SafetyRules{}).Check(g, nil, AddLearner(4)); err != nil {
		t.Fatalf("learner add rejected: %v", err)
	}
}

func TestSafetyAllowsSafeRemoval(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 3), RemoveReplica(3)); err != nil {
		t.Fatalf("safe removal rejected: %v", err)
	}
}

func TestSafetyUnknownHealthRefusesRemovalAndDemotion(t *testing.T) {
	t.Parallel()
	for name, cc := range map[string]raftpb.ConfChangeV2{
		"remove": RemoveReplica(3),
		"demote": DemoteVoter(3),
	} {
		t.Run(name, func(t *testing.T) {
			g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
			err := (SafetyRules{}).Check(g, fakeHealth{known: false}, cc)
			var refusal *SafetyRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("error = %v, want *SafetyRefusal", err)
			}
			if refusal.HealthKnown || !strings.Contains(refusal.Reason, "unknown") {
				t.Fatalf("refusal = %+v, want unknown health", refusal)
			}
		})
	}
}

func TestSafetyKnownTotalOutageRefusesRemoval(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	err := (SafetyRules{}).Check(g, knownLive(), RemoveReplica(3))
	var refusal *SafetyRefusal
	if !errors.As(err, &refusal) || !refusal.HealthKnown || refusal.LiveVoters != 0 || refusal.Quorum != 2 {
		t.Fatalf("refusal = %+v, error = %v", refusal, err)
	}
}

func TestSafetyRefusesQuorumBreakingRemoval(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	if err := (SafetyRules{}).Check(g, knownLive(1), RemoveReplica(2)); err == nil {
		t.Fatal("quorum-breaking removal was allowed")
	}
}

func TestSafetyRequiresLeaderTransfer(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "meta", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	for _, cc := range []raftpb.ConfChangeV2{RemoveReplica(1), DemoteVoter(1)} {
		if err := (SafetyRules{}).Check(g, knownLive(1, 2, 3), cc); err == nil || !strings.Contains(err.Error(), "transfer") {
			t.Fatalf("leader change error = %v, want transfer refusal", err)
		}
	}
}

func TestSafetyRefusesFinalVoterRemoval(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "meta", st: statusOf([]uint64{1}, nil, 0, 10, nil)}
	if err := (SafetyRules{}).Check(g, knownLive(1), RemoveReplica(1)); err == nil {
		t.Fatal("final voter removal was allowed")
	}
}

func TestSafetyRefusesDirectVoterGrow(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", leader: true, st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	err := (SafetyRules{}).Check(g, knownLive(1, 2, 3, 4), PromoteLearner(4))
	if err == nil || !strings.Contains(err.Error(), "not a current learner") {
		t.Fatalf("direct voter grow error = %v", err)
	}
}

func TestCaughtUpRequiresExactActiveLearner(t *testing.T) {
	t.Parallel()
	base := statusOf([]uint64{1}, []uint64{4, 5}, 1, 2000, map[uint64]uint64{4: 1000, 5: 1999})
	g := &fakeGroup{id: "s0", leader: true, st: base}
	if !CaughtUp(g, 4) {
		t.Fatal("learner at fixed lag limit was not caught up")
	}
	if !CaughtUp(g, 5) {
		t.Fatal("exact approved learner was not caught up")
	}
	if CaughtUp(g, 1) {
		t.Fatal("current voter reported caught up for promotion")
	}

	stale := base
	progress := stale.Progress[5]
	progress.RecentActive = false
	stale.Progress[5] = progress
	g.st = stale
	if CaughtUp(g, 5) {
		t.Fatal("inactive learner reported caught up")
	}
}

func TestCaughtUpRejectsLagMissingProgressZeroCommitAndFollower(t *testing.T) {
	t.Parallel()
	tests := map[string]*fakeGroup{
		"lag": {
			id: "s0", leader: true,
			st: statusOf([]uint64{1}, []uint64{4}, 1, 2000, map[uint64]uint64{4: 999}),
		},
		"missing progress": {
			id: "s0", leader: true,
			st: statusOf([]uint64{1}, []uint64{4}, 1, 2000, nil),
		},
		"zero commit": {
			id: "s0", leader: true,
			st: statusOf([]uint64{1}, []uint64{4}, 1, 0, map[uint64]uint64{4: 0}),
		},
		"follower": {
			id: "s0", leader: false,
			st: statusOf([]uint64{1}, []uint64{4}, 1, 2000, map[uint64]uint64{4: 2000}),
		},
	}
	for name, group := range tests {
		t.Run(name, func(t *testing.T) {
			if CaughtUp(group, 4) {
				t.Fatal("candidate reported caught up")
			}
		})
	}
}

func TestSafetyPromotesOnlyCaughtUpActiveLearner(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{
		id: "s0", leader: true,
		st: statusOf([]uint64{1, 2}, []uint64{4}, 1, 100, map[uint64]uint64{1: 100, 2: 100, 4: 100}),
	}
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 4), PromoteLearner(4)); err != nil {
		t.Fatalf("caught-up learner promotion rejected: %v", err)
	}

	progress := g.st.Progress[4]
	progress.RecentActive = false
	g.st.Progress[4] = progress
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 4), PromoteLearner(4)); err != nil {
		t.Fatalf("idle caught-up learner promotion rejected: %v", err)
	}

	progress.Match = 90
	g.st.Progress[4] = progress
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 4), PromoteLearner(4)); err == nil {
		t.Fatal("lagging inactive learner promotion was allowed")
	}
}

func TestSafetyRefusalIncludesOperationalContext(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{
		id: "shard7", leader: true,
		st: statusOf([]uint64{1}, []uint64{9}, 1, 2500, map[uint64]uint64{9: 1000}),
	}
	err := (SafetyRules{}).Check(g, knownLive(1, 9), PromoteLearner(9))
	for _, want := range []string{"shard7", "promote_learner", "replica 9", "quorum=2", "catch_up_lag=1500"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}

func TestSafetyRejectsMultiChange(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{
		{Type: raftpb.ConfChangeRemoveNode, NodeID: 3},
		{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 4},
	}}
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 3), cc); err == nil {
		t.Fatal("multi-change ConfChangeV2 was allowed")
	}
}

func TestSafetyRejectsJointTransition(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	cc := AddLearner(4)
	cc.Transition = raftpb.ConfChangeTransitionJointExplicit
	if err := (SafetyRules{}).Check(g, knownLive(1, 2, 3), cc); err == nil {
		t.Fatal("joint ConfChangeV2 was allowed")
	}
}

func TestCoordinatorAppliesOneExactChange(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	c := NewCoordinator()
	if err := c.Apply(context.Background(), g, nil, AddLearner(4)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := g.proposalCount(); got != 1 {
		t.Fatalf("proposal count = %d, want 1", got)
	}
}

func TestCoordinatorStopsBeforeUnsafeProposal(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1, 2, 3}, nil, 1, 10, nil)}
	err := NewCoordinator().Apply(context.Background(), g, knownLive(1), RemoveReplica(2))
	if err == nil {
		t.Fatal("unsafe change was accepted")
	}
	if got := g.proposalCount(); got != 0 {
		t.Fatalf("proposal count = %d, want 0", got)
	}
}

func TestCoordinatorWrapsProposalFailure(t *testing.T) {
	t.Parallel()
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1}, nil, 1, 10, nil), proposeErr: errors.New("boom")}
	err := NewCoordinator().Apply(context.Background(), g, nil, AddLearner(4))
	if err == nil || !strings.Contains(err.Error(), "group \"s0\"") || !strings.Contains(err.Error(), "replica 4") {
		t.Fatalf("proposal error = %v", err)
	}
}

func TestCoordinatorSerializesPerGroup(t *testing.T) {
	t.Parallel()
	var active atomic.Int32
	var maxActive atomic.Int32
	g := &fakeGroup{id: "s0", st: statusOf([]uint64{1}, nil, 1, 10, nil)}
	g.onPropose = func(raftpb.ConfChangeV2) {
		current := active.Add(1)
		for {
			seen := maxActive.Load()
			if current <= seen || maxActive.CompareAndSwap(seen, current) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
	}

	c := NewCoordinator()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for id := uint64(2); id < 18; id++ {
		wg.Add(1)
		go func(replicaID uint64) {
			defer wg.Done()
			errs <- c.Apply(context.Background(), g, nil, AddLearner(replicaID))
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent proposals = %d, want 1", got)
	}
}
