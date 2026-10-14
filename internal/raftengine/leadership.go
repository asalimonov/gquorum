package raftengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/replicastate"
)

// LeadershipEpoch is valid only for this exact engine run and leadership term.
// Context is canceled directly by engine state changes, independently of Watch.
type LeadershipEpoch struct {
	Term    uint64
	RunID   string
	Context context.Context
}

// AcquireLeadershipSupervisor reserves the one application supervisor slot.
func (g *Group) AcquireLeadershipSupervisor() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.supervised {
		return nil, fault.New(fault.Invalid, "leadership supervisor already running", nil)
	}
	g.supervised = true
	return func() { g.mu.Lock(); g.supervised = false; g.mu.Unlock() }, nil
}

// LeadershipSnapshot pairs one exact leadership term with its transition signal.
type LeadershipSnapshot struct {
	Epoch   LeadershipEpoch
	Changed <-chan struct{}
}

// LeadershipState captures the epoch and its wake channel together, preventing
// a missed transition between checking state and beginning a wait.
func (g *Group) LeadershipState() (LeadershipSnapshot, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.stopping || g.stopped || g.started && g.ctx.Err() != nil {
		err := g.err
		if err == nil {
			err = fault.ErrStopped
		}
		return LeadershipSnapshot{Changed: g.leadershipChanged}, err
	}
	return LeadershipSnapshot{Epoch: g.leadership, Changed: g.leadershipChanged}, nil
}

func (g *Group) refreshLeadershipLocked(o replicastate.Observation) {
	valid := o.Ready && o.IsLeader && o.IsVoter
	changed := false
	if g.leadership.Context != nil && (!valid || g.leadership.Term != o.Term) {
		g.cancelLeadership()
		g.cancelLeadership = nil
		g.leadership = LeadershipEpoch{}
		changed = true
	}
	if valid && g.leadership.Context == nil {
		ctx, cancel := context.WithCancel(g.ctx)
		g.leadership = LeadershipEpoch{Term: o.Term, RunID: g.nonce, Context: ctx}
		g.cancelLeadership = cancel
		changed = true
	}
	if changed || g.stopping || g.stopped {
		if g.leadershipChanged != nil {
			close(g.leadershipChanged)
		}
		g.leadershipChanged = make(chan struct{})
	}
}

// ProposeInTerm checks the captured term on the owner loop before admission and
// any definitive-drop retry. Unlike Propose, it never forwards to a new leader.
func (g *Group) ProposeInTerm(ctx context.Context, term uint64, p Proposal) (Result, error) {
	if term == 0 {
		return Result{}, fault.ErrInvalid
	}
	return g.submitProposal(ctx, p, term)
}

// PeerProgress is the leader's replication progress for one configured replica.
type PeerProgress struct {
	ReplicaID    uint64
	Match        uint64
	Voter        bool
	Self         bool
	RecentActive bool
}

// LeaderProgress returns the cached progress of every configured replica while
// this engine is the ready leader in term; otherwise it returns ErrNotLeader.
// RecentActive is etcd Raft's CheckQuorum activity flag. It is an observation.
func (g *Group) LeaderProgress(term uint64) ([]PeerProgress, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.stopping || g.stopped || g.started && g.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if term == 0 || g.leadership.Context == nil || g.leadership.Term != term || g.cached.Term != term {
		return nil, fault.ErrNotLeader
	}
	config := g.cached.Config
	voters := make(map[uint64]bool)
	for _, majority := range config.Voters {
		for id := range majority {
			voters[id] = true
		}
	}
	ids := make(map[uint64]bool, len(voters))
	for id := range voters {
		ids[id] = true
	}
	for id := range config.Learners {
		ids[id] = true
	}
	for id := range config.LearnersNext {
		ids[id] = true
	}
	result := make([]PeerProgress, 0, len(ids))
	for id := range ids {
		p := g.cached.Progress[id]
		self := id == g.opts.ReplicaID
		result = append(result, PeerProgress{
			ReplicaID:    id,
			Match:        p.Match,
			Voter:        voters[id],
			Self:         self,
			RecentActive: self || p.RecentActive,
		})
	}
	slices.SortFunc(result, func(a, b PeerProgress) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	return result, nil
}

// PeerName names the local replica directly and resolves others through
// Options.PeerName; it returns "" when unresolved.
func (g *Group) PeerName(ctx context.Context, replicaID uint64) string {
	if replicaID == g.opts.ReplicaID {
		return g.opts.NodeName
	}
	if g.opts.PeerName == nil {
		return ""
	}
	name, err := g.opts.PeerName(ctx, replicaID)
	if err != nil {
		return ""
	}
	return name
}
