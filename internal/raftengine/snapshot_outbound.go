package raftengine

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// SnapshotOffer pins immutable native bytes for one run and leadership epoch.
type SnapshotOffer struct {
	Message               raftpb.Message
	Attempt               SnapshotAttempt
	PublicationGeneration uint64
	Epoch                 context.Context
}

// SnapshotAttempt identifies one native offer; generations never wrap.
type SnapshotAttempt struct {
	RunID                       string
	Term, ReplicaID, Generation uint64
}

func (g *Group) prepareSnapshotOffer(m raftpb.Message) (SnapshotOffer, error) {
	status := g.rn.Status()
	g.refreshSnapshotEpochStatus(status)
	if g.snapshotEpoch == nil || m.Term != g.snapshotEpochTerm || m.Snapshot == nil ||
		m.From != g.opts.ReplicaID || g.snapshotEpoch.Err() != nil {
		return SnapshotOffer{}, fault.ErrNotLeader
	}
	progress, present := status.Progress[m.To]
	if !present || m.To == g.opts.ReplicaID {
		return SnapshotOffer{}, fault.ErrIdentityMismatch
	}
	if g.snapshotAttemptSequence == math.MaxUint64 {
		return SnapshotOffer{}, fault.ErrOverloaded
	}
	g.snapshotAttemptSequence++
	attempt := SnapshotAttempt{RunID: g.nonce, Term: m.Term, ReplicaID: m.To, Generation: g.snapshotAttemptSequence}
	g.snapshotReportMu.Lock()
	record := g.snapshotReports[m.To]
	if record == nil {
		record = &snapshotReportState{lastMatch: progress.Match}
		g.snapshotReports[m.To] = record
	}
	record.attempt = attempt
	record.pending = nil
	record.terminal = false
	record.progress = nil
	record.started = time.Now()
	g.snapshotReportMu.Unlock()
	return SnapshotOffer{Message: m, Attempt: attempt,
		PublicationGeneration: g.snapshotPublication, Epoch: g.snapshotEpoch}, nil
}

// refreshSnapshotEpoch runs only on the native loop, before offer/dispatch and
// after every term/role transition. Peer backoff survives leadership changes.
func (g *Group) refreshSnapshotEpoch() {
	g.refreshSnapshotEpochStatus(g.rn.Status())
}

func (g *Group) refreshSnapshotEpochStatus(status raft.Status) {
	leader := status.RaftState == raft.StateLeader && g.ctx != nil && g.ctx.Err() == nil
	changed := g.snapshotEpochTerm != status.Term || (g.snapshotEpoch != nil) != leader
	if changed && g.cancelSnapshotEpoch != nil {
		g.cancelSnapshotEpoch()
		g.cancelSnapshotEpoch = nil
		g.snapshotEpoch = nil
	}
	if leader && g.snapshotEpoch == nil {
		g.snapshotEpoch, g.cancelSnapshotEpoch = context.WithCancel(g.ctx)
	}
	g.snapshotEpochTerm = status.Term
	g.snapshotReportMu.Lock()
	defer g.snapshotReportMu.Unlock()
	if g.snapshotReports == nil {
		g.snapshotReports = make(map[uint64]*snapshotReportState)
	}
	voters := status.Config.Voters.IDs()
	for peer, record := range g.snapshotReports {
		_, voter := voters[peer]
		_, learner := status.Config.Learners[peer]
		_, nextLearner := status.Config.LearnersNext[peer]
		if !voter && !learner && !nextLearner {
			delete(g.snapshotReports, peer)
			continue
		}
		if changed {
			record.attempt = SnapshotAttempt{}
			record.pending = nil
			record.progress = nil
			record.terminal = false
		}
		if progress, ok := status.Progress[peer]; ok && progress.Match > record.lastMatch {
			record.lastMatch = progress.Match
			record.backoff = 0
		}
	}
}

func (g *Group) dispatchSnapshotJobs(jobs []SnapshotJob) {
	if len(jobs) == 0 {
		return
	}
	g.refreshSnapshotEpoch()
	for _, job := range jobs {
		if g.ctx.Err() != nil {
			job.Discard(g.ctx.Err())
			continue
		}
		// The Raft loop still owns one wg count, so Add cannot race the final Wait.
		// Admission's Node-wide permit bounds the number of live bulk workers.
		g.wg.Go(func() { job.Run(g.ctx) })
	}
}

func discardSnapshotJobs(jobs []SnapshotJob, err error) {
	for _, job := range jobs {
		job.Discard(err)
	}
}

// preparedSnapshotJob keeps epoch cancellation effective even for a protocol
// double that uses only Run's supplied context. The job is entered once.
type preparedSnapshotJob struct {
	job   SnapshotJob
	epoch context.Context
	state atomic.Uint32
}

func (j *preparedSnapshotJob) Run(owner context.Context) {
	if j.epoch.Err() != nil {
		j.Discard(j.epoch.Err())
		return
	}
	if !j.state.CompareAndSwap(0, 1) {
		return
	}
	ctx, cancel := context.WithCancel(owner)
	stop := context.AfterFunc(j.epoch, cancel)
	defer cancel()
	defer stop()
	if j.epoch.Err() != nil {
		cancel()
	}
	j.job.Run(ctx)
}
func (j *preparedSnapshotJob) Discard(err error) {
	if j.state.CompareAndSwap(0, 1) {
		j.job.Discard(err)
	}
}

func (g *Group) prepareReadySnapshot(m raftpb.Message, bulk SnapshotJobTransport) (SnapshotJob, bool) {
	offer, err := g.prepareSnapshotOffer(m)
	if err != nil {
		g.rn.ReportSnapshot(m.To, raft.SnapshotFailure)
		return nil, true
	}
	if bulk == nil {
		return nil, false
	}
	if offer.PublicationGeneration == 0 {
		g.ReportSnapshotAttemptError(offer.Attempt, fault.ErrInvalid)
		return nil, true
	}
	job, err := bulk.PrepareSnapshot(offer)
	if err != nil || job == nil {
		if job != nil {
			job.Discard(err)
		}
		if err == nil {
			err = fault.ErrInvalid
		}
		g.ReportSnapshotAttemptError(offer.Attempt, err)
		return nil, true
	}
	return &preparedSnapshotJob{job: job, epoch: offer.Epoch}, true
}
