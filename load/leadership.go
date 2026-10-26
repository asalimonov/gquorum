package load

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
)

// Leadership permits proposals only in one exact replica run and leader term.
// A successful proposal can still have an uncertain outcome after cancellation;
// application deduplication and side-effect fencing remain necessary.
type Leadership struct {
	core *raftengine.Group
	term uint64
	ctx  context.Context
}

// Term returns the captured term, or zero for an invalid handle.
func (l *Leadership) Term() uint64 {
	if l == nil {
		return 0
	}
	return l.term
}

// Propose checks the captured term at Raft admission, including internal retry,
// and never forwards through a follower. Both ctx and the leadership lifetime
// bound the call. A handle retained after its callback exits cannot submit.
func (l *Leadership) Propose(ctx context.Context, p Proposal) (Submission, error) {
	s := newSubmissionFromLeadership(l, p)
	if err := validateRequestID(p.RequestID); err != nil {
		return refusedSubmission(s), err
	}
	if l == nil || l.core == nil || l.ctx == nil || l.term == 0 {
		return refusedSubmission(s), fault.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		if l.ctx.Err() != nil {
			err = fault.New(fault.Stopped, "leadership scope ended", err)
		}
		return refusedSubmission(s), err
	}
	if err := l.ctx.Err(); err != nil {
		return refusedSubmission(s), fault.New(fault.Stopped, "leadership scope ended", err)
	}
	scoped, cleanup := leadershipContext(l.ctx, ctx)
	defer cleanup()
	out, err := l.core.Submit(scoped, raftengine.Proposal(p), s.AttemptID, l.term)
	if err != nil {
		expired := errors.Is(err, fault.ErrNotLeader) || l.ctx.Err() != nil
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if expired {
			err = fault.New(fault.Stopped, "leadership scope ended", err)
		}
	}
	return submissionFromEngine(out), err
}

// ReplicaProgress is the leader's observation of one configured replica.
type ReplicaProgress struct {
	ReplicaID uint64 `json:"replica_id"`
	// NodeName names the local replica directly and resolves others through
	// the managed topology catalog; it is empty when unknown (Standalone
	// peers).
	NodeName string `json:"node_name,omitempty"`
	// Voter includes incoming and outgoing joint-consensus voters.
	Voter bool   `json:"voter"`
	Self  bool   `json:"self"`
	Match uint64 `json:"match"`
	// RecentActive reports that the leader heard from the replica within the
	// last election timeout. The leader itself is always active.
	RecentActive bool `json:"recent_active"`
}

// Replicas returns the leader's progress for every configured replica, sorted
// by ReplicaID, while this leadership term is current. After leadership ends
// it returns ErrNotLeader. The result is an observation, not a barrier.
func (l *Leadership) Replicas(ctx context.Context) ([]ReplicaProgress, error) {
	if l == nil || l.core == nil || l.ctx == nil || l.term == 0 {
		return nil, fault.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.ctx.Err() != nil {
		return nil, fault.ErrNotLeader
	}
	peers, err := l.core.LeaderProgress(l.term)
	if err != nil {
		return nil, err
	}
	result := make([]ReplicaProgress, len(peers))
	for i, p := range peers {
		result[i] = ReplicaProgress{
			ReplicaID:    p.ReplicaID,
			NodeName:     l.core.PeerName(ctx, p.ReplicaID),
			Voter:        p.Voter,
			Self:         p.Self,
			Match:        p.Match,
			RecentActive: p.RecentActive,
		}
	}
	return result, nil
}

// RunWhileLeader runs one callback at a time while this exact replica is ready
// and leader. Leadership loss cancels its context directly; this method joins
// the callback before starting a later term. Returning nil completes that
// term's
// work without restarting it. Unexpected errors end the supervisor. The caller
// must own/join this method; managed applications normally call it within Run.
// A callback must not wait for teardown which in turn waits for this callback.
func (g *Group) RunWhileLeader(ctx context.Context, run func(context.Context, *Leadership) error) error {
	core := g.core()
	if core == nil || run == nil {
		return fault.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	release, err := core.AcquireLeadershipSupervisor()
	if err != nil {
		return err
	}
	defer release()
	var lastTerm uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot, err := core.LeadershipState()
		epoch, changed := snapshot.Epoch, snapshot.Changed
		if err != nil {
			return err
		}
		if epoch.Context == nil || epoch.Term == lastTerm {
			waitLeadershipChange(ctx, changed)
			continue
		}
		child, cleanup := leadershipContext(epoch.Context, ctx)
		lastTerm = epoch.Term
		leader := &Leadership{core: core, term: epoch.Term, ctx: child}
		if err := leader.runCallback(run, cleanup); err != nil {
			return err
		}
	}
}

func waitLeadershipChange(ctx context.Context, changed <-chan struct{}) {
	select {
	case <-ctx.Done():
	case <-changed:
	}
}

func (l *Leadership) runCallback(run func(context.Context, *Leadership) error, cleanup func()) error {
	err := run(l.ctx, l)
	// Classify before cleanup cancels an otherwise-live context.
	expected := l.ctx.Err() != nil && errors.Is(err, l.ctx.Err())
	cleanup()
	if err != nil && !expected {
		return err
	}
	return nil
}

// leadershipContext preserves direct cancellation from the engine-owned scope,
// links caller cancellation, and joins the small context-link callback.
func leadershipContext(scope, caller context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(scope)
	var deadlineCancel context.CancelFunc
	if deadline, ok := caller.Deadline(); ok {
		ctx, deadlineCancel = context.WithDeadline(ctx, deadline)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(caller, func() { cancel(); close(done) })
	if caller.Err() != nil {
		cancel()
	}
	return ctx, func() {
		cancel()
		if deadlineCancel != nil {
			deadlineCancel()
		}
		if !stop() {
			<-done
		}
	}
}
