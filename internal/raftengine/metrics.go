package raftengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"math"
	"slices"
	"time"
)

type peerAck struct {
	sequence uint64
	at       time.Time
}

// sampleRaftMetrics is called only on the RawNode owner loop. No independent
// sampler can refresh this generation while Apply or Restore blocks that loop.
func (g *Group) sampleRaftMetrics(now time.Time) {
	s := g.rn.Status()
	cs := copyConf(g.cs)
	slices.Sort(cs.Voters)
	slices.Sort(cs.Learners)
	slices.Sort(cs.VotersOutgoing)
	slices.Sort(cs.LearnersNext)
	b, _ := json.Marshal(cs)
	sum := sha256.Sum256(b)
	digest := hex.EncodeToString(sum[:])
	g.metricsMu.Lock()
	defer g.metricsMu.Unlock()
	old := g.raftSample
	if old.Term != s.Term || old.LeaderID != s.Lead || old.MembershipDigest != digest {
		clear(g.peerAcks)
		if old.Raft.Sequence > 0 && old.LeaderID != s.Lead {
			g.meter.ObserveLeaderChange()
		}
	}
	seq := old.Raft.Sequence
	if seq == math.MaxUint64 {
		return
	}
	seq++
	voters := append([]uint64{}, cs.Voters...)
	for _, id := range cs.VotersOutgoing {
		if !slices.Contains(voters, id) {
			voters = append(voters, id)
		}
	}
	slices.Sort(voters)
	learners := append([]uint64{}, cs.Learners...)
	for _, id := range cs.LearnersNext {
		if !slices.Contains(learners, id) {
			learners = append(learners, id)
		}
	}
	slices.Sort(learners)
	next := metrics.ReplicaSample{Raft: metrics.Stamp{Sequence: seq, State: metrics.Current}, LeaderID: s.Lead, Term: s.Term, Commit: s.Commit, Applied: g.applied, Voters: voters, Learners: learners, MembershipDigest: digest, Pending: len(g.props), Queued: len(g.proposals), ProposalLimit: cap(g.slots)}
	for id, p := range s.Progress {
		ack := g.peerAcks[id]
		next.Progress = append(next.Progress, metrics.PeerProgress{ReplicaID: id, Match: p.Match, RecentActive: p.RecentActive, AckSequence: ack.sequence, AckAge: internalmetrics.Age(now, ack.at), AckKnown: !ack.at.IsZero()})
	}
	slices.SortFunc(next.Progress, func(a, b metrics.PeerProgress) int {
		if a.ReplicaID < b.ReplicaID {
			return -1
		}
		if a.ReplicaID > b.ReplicaID {
			return 1
		}
		return 0
	})
	g.raftSample = next
	g.raftSampleAt = now
}

// recordPeerAck runs after successful Step. Rejects and old-term messages cannot
// provide standby catch-up proof. Heartbeats keep healthy idle peers observable.
func (g *Group) recordPeerAck(now time.Time, msg raftpb.Message) {
	if msg.Reject || msg.Type != raftpb.MsgAppResp && msg.Type != raftpb.MsgHeartbeatResp {
		return
	}
	s := g.rn.BasicStatus()
	if s.RaftState != raft.StateLeader || msg.Term != s.Term || msg.From == g.opts.ReplicaID {
		return
	}
	if !slices.Contains(g.cs.Voters, msg.From) && !slices.Contains(g.cs.Learners, msg.From) && !slices.Contains(g.cs.VotersOutgoing, msg.From) && !slices.Contains(g.cs.LearnersNext, msg.From) {
		return
	}
	g.metricsMu.Lock()
	defer g.metricsMu.Unlock()
	// A newly elected leader first publishes its new scope, then accepts proof.
	if g.raftSample.Term != s.Term || g.raftSample.LeaderID != s.Lead {
		return
	}
	a := g.peerAcks[msg.From]
	if a.sequence == math.MaxUint64 {
		return
	}
	a.sequence++
	a.at = now
	g.peerAcks[msg.From] = a
}

func (g *Group) sampleWriter(now time.Time) {
	g.metricsMu.Lock()
	defer g.metricsMu.Unlock()
	if g.writerSequence < math.MaxUint64 {
		g.writerSequence++
		g.writerSampleAt = now
	}
}
func (g *Group) beginWriteMetric(now time.Time) {
	g.metricsMu.Lock()
	g.writerSince = now
	g.metricsMu.Unlock()
}
func (g *Group) finishWriteMetric(now time.Time, err error) {
	g.metricsMu.Lock()
	g.writerSince = time.Time{}
	g.writerFailed = err != nil
	g.metricsMu.Unlock()
	g.sampleWriter(now)
}

func (g *Group) metricsSample(now time.Time) metrics.ReplicaSample {
	g.metricsMu.RLock()
	s := g.raftSample.Clone()
	s.Raft.Age = internalmetrics.Age(now, g.raftSampleAt)
	if s.Raft.Sequence == 0 {
		s.Raft.State = metrics.Unknown
	} else if s.Raft.Age > g.opts.Metrics.MaxSampleAge {
		s.Raft.State = metrics.Stale
	}
	s.Writer = metrics.Stamp{Sequence: g.writerSequence, Age: internalmetrics.Age(now, g.writerSampleAt), State: metrics.Current}
	if s.Writer.Sequence == 0 || g.writerFailed {
		s.Writer.State = metrics.Unknown
	} else if s.Writer.Age > g.opts.Metrics.MaxSampleAge {
		s.Writer.State = metrics.Stale
	}
	if !g.writerSince.IsZero() {
		s.WALStall = internalmetrics.Age(now, g.writerSince)
	}
	for i := range s.Progress {
		s.Progress[i].AckAge = internalmetrics.AddDuration(s.Progress[i].AckAge, internalmetrics.Age(now, g.raftSampleAt))
	}
	g.metricsMu.RUnlock()
	s.Running = g.Running()
	s.Restoring = g.restoring.Load()
	select {
	case <-g.ready:
		s.Ready = s.Running
	default:
	}
	s.OutboundQueued = len(g.sendC) + int(g.outboundPending.Load())
	s.OutboundLimit = cap(g.sendC) + int(g.sendWorkers.Load())*outboundPeerQueueCapacity
	return s
}
func (g *Group) sampleMetrics() {
	defer g.wg.Done()
	defer g.meter.Stop()
	t := time.NewTicker(g.opts.Metrics.SampleInterval)
	defer t.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case now := <-t.C:
			g.meter.Tick(now, g.metricsSample(now))
		}
	}
}

func (g *Group) Metrics() metrics.ReplicaObservation {
	return g.meter.Snapshot(time.Now())
}
func (g *Group) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	return g.meter.History(time.Now(), q)
}
