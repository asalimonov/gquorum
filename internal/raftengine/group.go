package raftengine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/raftlog"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type response struct {
	result Result
	err    error
}
type request struct {
	ctx         context.Context
	correlation string
	proposal    Proposal
	message     *raftpb.Message
	change      *raftpb.ConfChangeV2
	admin       *raftstate.Entry
	expected    *raftpb.ConfState
	transfer    uint64
	snapshot    bool
	reply       chan response
	retried     bool
	retryAt     time.Time
}
type snapshotReport struct {
	id     uint64
	status raft.SnapshotStatus
}
type Group struct {
	opts                                      Options
	rn                                        *raft.RawNode
	ms                                        *raft.MemoryStorage
	cs                                        raftpb.ConfState
	applied, snapshotIndex, recoveryCommit    uint64
	durableHS                                 raftpb.HardState
	mu                                        sync.RWMutex
	cached                                    raft.Status
	cachedPending                             int
	err                                       error
	started, stopped                          bool
	ctx                                       context.Context
	cancel                                    context.CancelFunc
	done, ready                               chan struct{}
	readyOnce                                 sync.Once
	wg                                        sync.WaitGroup
	proposals, inbound, controls, snapshots   chan *request
	slots                                     chan struct{}
	reads                                     chan *readRequest
	readSlots                                 chan struct{}
	readState                                 readState
	authority                                 raftstate.AuthorityState
	adminNotify                               []adminReply
	transferIssuedTerm                        uint64
	unreachable                               chan uint64
	reports                                   chan snapshotReport
	sendC                                     chan []raftpb.Message
	sendWorkers                               atomic.Int64
	writeC                                    chan writeJob
	writeDone                                 chan writeResult
	pending                                   *writeJob
	props, changes                            map[string]*request
	confNotify                                []*request
	retirementMessages                        []raftpb.Message
	membershipChanged                         bool
	nonce                                     string
	sequence                                  atomic.Uint64
	stallSince                                atomic.Int64
	meter                                     *internalmetrics.Meter
	metricsMu                                 sync.RWMutex
	raftSample                                metrics.ReplicaSample
	raftSampleAt, writerSampleAt, writerSince time.Time
	writerSequence                            uint64
	writerFailed                              bool
	peerAcks                                  map[uint64]peerAck
	restoring                                 atomic.Bool
	outboundPending                           atomic.Int64
}

func New(opts Options) (*Group, error) { return NewContext(context.Background(), opts) }

// NewContext binds storage and restores the machine under its owner's startup
// context. It starts no workers and never closes borrowed dependencies.
func NewContext(ctx context.Context, opts Options) (*Group, error) {
	return newGroup(ctx, opts, false)
}

// NewMeta is restricted to the library's internal topology runtime.
func NewMeta(opts Options) (*Group, error) { return newGroup(context.Background(), opts, true) }
func newGroup(parent context.Context, opts Options, meta bool) (*Group, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if err := opts.validate(meta); err != nil {
		return nil, err
	}
	opts = opts.defaults()
	if opts.InitialAuthority != nil {
		grant := opts.InitialAuthority.Clone()
		opts.InitialAuthority = &grant
	}
	peers := make([]raft.Peer, len(opts.Peers))
	for i, p := range opts.Peers {
		peers[i] = raft.Peer{ID: p.ID, Context: bytes.Clone(p.Context)}
	}
	opts.Peers = peers
	learners := make([]raft.Peer, len(opts.FoundingLearners))
	for i, p := range opts.FoundingLearners {
		learners[i] = raft.Peer{ID: p.ID, Context: bytes.Clone(p.Context)}
	}
	opts.FoundingLearners = learners
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	id := storage.Identity{NodeName: opts.NodeName, ClusterID: opts.ClusterID, GroupID: opts.GroupID, ReplicaID: opts.ReplicaID}
	if err := opts.Store.BindReplica(ctx, id); err != nil {
		return nil, err
	}
	probe, err := opts.Store.Probe(ctx, opts.GroupID)
	if err != nil {
		return nil, err
	}
	if probe.Exists && probe.Identity != id {
		return nil, fault.ErrIdentityMismatch
	}
	if err := opts.Store.Ensure(ctx, opts.GroupID); err != nil {
		return nil, err
	}
	recovery, err := opts.Store.Load(ctx, opts.GroupID)
	if err != nil {
		return nil, err
	}
	if err := validateRecovery(recovery); err != nil {
		return nil, err
	}
	var restored raftstate.Snapshot
	if !raft.IsEmptySnap(recovery.Snapshot) {
		restored, err = raftstate.DecodeRaftSnapshot(recovery.Snapshot, opts.ClusterID, opts.GroupID)
		if err != nil {
			return nil, err
		}
	}
	if err := validateAuthorityRecovery(opts, recovery, restored); err != nil {
		return nil, err
	}
	if err := opts.Machine.Restore(ctx, bytes.Clone(restored.Application)); err != nil {
		return nil, fault.New(fault.Unreadable, "restore load machine", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if raft.IsEmptyHardState(recovery.HardState) && raft.IsEmptySnap(recovery.Snapshot) && len(recovery.Entries) == 0 && opts.Mode == Founding {
		data, err := opts.Machine.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fault.New(fault.Invalid, "empty founding machine snapshot", nil)
		}
		cs := raftpb.ConfState{}
		for _, peer := range peers {
			cs.Voters = append(cs.Voters, peer.ID)
		}
		for _, peer := range learners {
			cs.Learners = append(cs.Learners, peer.ID)
		}
		slices.Sort(cs.Voters)
		slices.Sort(cs.Learners)
		if opts.InitialAuthority != nil {
			restored.Authority = raftstate.AuthorityState{Grant: opts.InitialAuthority, InstallIndex: 1, InstallTerm: 1}
		}
		data, err = encodeSnapshot(opts, data, restored.Authority)
		if err != nil {
			return nil, err
		}
		ss := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}
		if err := opts.Store.SaveSnapshot(ctx, opts.GroupID, ss); err != nil {
			return nil, err
		}
		recovery.Snapshot = ss
		recovery.HardState = raftpb.HardState{Term: 1, Commit: 1}
	}
	ms := raft.NewMemoryStorage()
	if !raft.IsEmptySnap(recovery.Snapshot) {
		if err := ms.ApplySnapshot(recovery.Snapshot); err != nil {
			return nil, err
		}
	}
	if err := ms.Append(recovery.Entries); err != nil {
		return nil, err
	}
	if err := ms.SetHardState(recovery.HardState); err != nil {
		return nil, err
	}
	rn, err := raft.NewRawNode(&raft.Config{ID: opts.ReplicaID, ElectionTick: opts.Timing.ElectionTicks, HeartbeatTick: opts.Timing.HeartbeatTicks, Storage: ms, Applied: recovery.Snapshot.Metadata.Index, MaxSizePerMsg: 64 << 10, MaxInflightMsgs: 256, MaxCommittedSizePerReady: opts.MaxApplyBatchBytes, MaxUncommittedEntriesSize: opts.MaxUncommittedBytes, PreVote: true, CheckQuorum: true, ReadOnlyOption: raft.ReadOnlySafe, Logger: raftlog.Logger{Log: opts.Logger}})
	if err != nil {
		return nil, err
	}
	var seed [16]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, err
	}
	g := &Group{opts: opts, rn: rn, ms: ms, cs: copyConf(recovery.Snapshot.Metadata.ConfState), applied: recovery.Snapshot.Metadata.Index, snapshotIndex: recovery.Snapshot.Metadata.Index, recoveryCommit: recovery.HardState.Commit, durableHS: recovery.HardState, done: make(chan struct{}), ready: make(chan struct{}), proposals: make(chan *request, 256), inbound: make(chan *request, 1024), controls: make(chan *request, 16), snapshots: make(chan *request, 16), slots: make(chan struct{}, opts.MaxPendingProposals), unreachable: make(chan uint64, 256), reports: make(chan snapshotReport, 64), sendC: make(chan []raftpb.Message, 256), writeC: make(chan writeJob, 1), writeDone: make(chan writeResult, 1), props: make(map[string]*request), changes: make(map[string]*request), nonce: hex.EncodeToString(seed[:])}
	g.authority = restored.Authority.Clone()
	g.reads = make(chan *readRequest, opts.Reads.MaxPending)
	g.readSlots = make(chan struct{}, opts.Reads.MaxPending)
	g.peerAcks = make(map[uint64]peerAck)
	g.meter, err = internalmetrics.NewMeter(metrics.ReplicaIdentity{ClusterID: opts.ClusterID, GroupID: opts.GroupID, NodeName: opts.NodeName, ReplicaID: opts.ReplicaID, InstanceID: opts.InstanceID, RunID: g.nonce}, opts.Metrics)
	if err != nil {
		return nil, err
	}
	g.refresh()
	return g, nil
}

func (g *Group) Start(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started || g.stopped {
		return fault.New(fault.Stopped, "group cannot start twice", nil)
	}
	g.started = true
	g.ctx, g.cancel = context.WithCancel(parent)
	g.wg.Add(4)
	go g.run()
	go g.writer()
	go g.sender()
	go g.sampleMetrics()
	go func() { g.wg.Wait(); close(g.done) }()
	return nil
}
func (g *Group) Stop(ctx context.Context) error {
	g.mu.Lock()
	if !g.started && !g.stopped {
		g.stopped = true
		g.err = fault.ErrStopped
		g.meter.Stop()
		close(g.done)
	}
	if g.cancel != nil {
		g.cancel()
	}
	g.mu.Unlock()
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "load group workers have not stopped", ctx.Err())
	}
}
func (g *Group) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.done:
		return g.exitError()
	case <-g.ready:
		select {
		case <-g.done:
			return g.exitError()
		default:
			return nil
		}
	}
}
func (g *Group) exitError() error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.err != nil {
		return g.err
	}
	return fault.ErrStopped
}
func (g *Group) Error() error      { g.mu.RLock(); defer g.mu.RUnlock(); return g.err }
func (g *Group) GroupID() string   { return g.opts.GroupID }
func (g *Group) ReplicaID() uint64 { return g.opts.ReplicaID }
func (g *Group) IsLeader() bool    { return g.RaftStatus().RaftState == raft.StateLeader }
func (g *Group) WALStall() time.Duration {
	n := g.stallSince.Load()
	if n == 0 {
		return 0
	}
	return time.Since(time.Unix(0, n))
}

// Propose waits for local committed application. Cancellation can leave an
// uncertain outcome: a submitted command may still commit. RequestID is passed
// to Machine for application-owned durable deduplication; it does not deduplicate
// commands in the runtime. Use one running Group per local group/replica identity.
func (g *Group) Propose(ctx context.Context, p Proposal) (result Result, err error) {
	if g.opts.GroupID != topology.MetaGroupID {
		bounded, cancel := context.WithTimeout(ctx, g.opts.ProposalTimeout)
		defer cancel()
		ctx = bounded
	}
	started := time.Now()
	g.meter.ObserveProposal()
	defer func() {
		outcome := internalmetrics.Applied
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			outcome = internalmetrics.TimedOut
		case errors.Is(err, context.Canceled):
			outcome = internalmetrics.Canceled
		case errors.Is(err, fault.ErrOverloaded):
			outcome = internalmetrics.Overloaded
		case err != nil:
			outcome = internalmetrics.Unavailable
		}
		g.meter.ObserveResult(time.Since(started), outcome)
	}()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if g.WALStall() > time.Second {
		return Result{}, fault.ErrOverloaded
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		return Result{}, fault.ErrOverloaded
	}
	req := &request{ctx: ctx, correlation: g.newID(), proposal: Proposal{RequestID: p.RequestID, Data: bytes.Clone(p.Data)}, reply: make(chan response, 1)}
	return g.submit(ctx, g.proposals, req)
}

// ProposalStats describes local proposal admission. InFlight includes callers
// waiting to enqueue or receive a result. AwaitingCommit is the last run-loop
// observation of registered waiters; canceled callers are pruned independently
// of persistence. The fields are observations, not a transactional snapshot.
type ProposalStats struct {
	Limit, InFlight, Queued, AwaitingCommit int
}

func (g *Group) ProposalStats() ProposalStats {
	g.mu.RLock()
	pending := g.cachedPending
	g.mu.RUnlock()
	return ProposalStats{Limit: cap(g.slots), InFlight: len(g.slots), Queued: len(g.proposals), AwaitingCommit: pending}
}
func (g *Group) newID() string { return fmt.Sprintf("%s-%d", g.nonce, g.sequence.Add(1)) }
func (g *Group) submit(ctx context.Context, queue chan *request, req *request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	g.mu.RLock()
	started, stopped := g.started, g.stopped
	g.mu.RUnlock()
	if !started || stopped {
		return Result{}, g.exitError()
	}
	enqueue, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case queue <- req:
	case <-enqueue.Done():
		return Result{}, enqueue.Err()
	case <-g.done:
		return Result{}, g.exitError()
	}
	select {
	case res := <-req.reply:
		return res.result, res.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-g.done:
		return Result{}, g.exitError()
	}
}
func (g *Group) Step(ctx context.Context, msg raftpb.Message) error {
	if g.opts.ManagedAuthority && msg.Type == raftpb.MsgTransferLeader {
		return fault.ErrUnsupported
	}
	_, known := raftpb.MessageType_name[int32(msg.Type)]
	if !known || raft.IsLocalMsg(msg.Type) || msg.From == 0 || msg.From == g.opts.ReplicaID || msg.To != g.opts.ReplicaID {
		return fault.New(fault.Invalid, "not a remote message for this replica", nil)
	}
	if msg.Type == raftpb.MsgReadIndex || msg.Type == raftpb.MsgReadIndexResp {
		if _, ok := readMessageContext(msg); !ok {
			return fault.ErrInvalid
		}
	}
	if msg.Type == raftpb.MsgProp {
		if err := validateForwardedProposal(g.opts, msg); err != nil {
			return err
		}
	}
	if msg.Type == raftpb.MsgSnap {
		if msg.Snapshot == nil {
			return fault.New(fault.Invalid, "snapshot message has no state", nil)
		}
		if err := validateSnapshot(*msg.Snapshot); err != nil {
			return err
		}
		state, err := raftstate.DecodeRaftSnapshot(*msg.Snapshot, g.opts.ClusterID, g.opts.GroupID)
		if err != nil {
			return err
		}
		if err := validateAuthoritySnapshot(g.opts, state); err != nil {
			return err
		}
	}
	if msg.Type == raftpb.MsgApp {
		for i, e := range msg.Entries {
			if e.Index != msg.Index+uint64(i)+1 || e.Term == 0 || e.Term > msg.Term || e.Type != raftpb.EntryNormal && e.Type != raftpb.EntryConfChangeV2 {
				return fault.New(fault.Invalid, "invalid append message entries", nil)
			}
			if err := validateEntryMode(g.opts, e); err != nil {
				return err
			}
		}
	}
	owned, err := copyMessage(msg)
	if err != nil {
		return err
	}
	_, err = g.submit(ctx, g.inbound, &request{ctx: ctx, message: &owned, reply: make(chan response, 1)})
	return err
}
func (g *Group) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeV2) error {
	if g.opts.ManagedAuthority {
		return fault.ErrUnsupported
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 || cc.Changes[0].NodeID == 0 {
		return fault.ErrUnsafeMembership
	}
	if typ := cc.Changes[0].Type; typ != raftpb.ConfChangeAddNode && typ != raftpb.ConfChangeAddLearnerNode && typ != raftpb.ConfChangeRemoveNode {
		return fault.ErrUnsafeMembership
	}
	cc.Changes = slices.Clone(cc.Changes)
	cc.Context = bytes.Clone(cc.Context)
	_, err := g.submit(ctx, g.controls, &request{ctx: ctx, correlation: g.newID(), change: &cc, reply: make(chan response, 1)})
	return err
}
func (g *Group) TransferLeader(ctx context.Context, target uint64) error {
	if g.opts.ManagedAuthority {
		return fault.ErrUnsupported
	}
	if target == 0 {
		return fault.ErrInvalid
	}
	_, err := g.submit(ctx, g.controls, &request{ctx: ctx, transfer: target, reply: make(chan response, 1)})
	return err
}
func (g *Group) Snapshot(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := g.submit(bounded, g.snapshots, &request{ctx: bounded, snapshot: true, reply: make(chan response, 1)})
	return err
}
func (g *Group) ReportUnreachable(id uint64) {
	select {
	case g.unreachable <- id:
	default:
	}
}
func (g *Group) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	select {
	case g.reports <- snapshotReport{id, status}:
	default:
	}
}
func copyConf(cs raftpb.ConfState) raftpb.ConfState {
	cs.Voters = slices.Clone(cs.Voters)
	cs.VotersOutgoing = slices.Clone(cs.VotersOutgoing)
	cs.Learners = slices.Clone(cs.Learners)
	cs.LearnersNext = slices.Clone(cs.LearnersNext)
	return cs
}
func copyMessage(m raftpb.Message) (raftpb.Message, error) {
	data, err := m.Marshal()
	if err != nil {
		return raftpb.Message{}, err
	}
	var result raftpb.Message
	err = result.Unmarshal(data)
	return result, err
}
func answer(req *request, result Result, err error) {
	if req != nil {
		req.reply <- response{Result{Code: result.Code, Data: bytes.Clone(result.Data)}, err}
	}
}

func (g *Group) RaftStatus() raft.Status {
	g.mu.RLock()
	defer g.mu.RUnlock()
	s := g.cached
	s.Config = g.cached.Config.Clone()
	s.Config.AutoLeave = g.cached.Config.AutoLeave
	s.Progress = make(map[uint64]tracker.Progress, len(g.cached.Progress))
	for id, p := range g.cached.Progress {
		if p.Inflights != nil {
			p.Inflights = p.Inflights.Clone()
		}
		s.Progress[id] = p
	}
	return s
}
func (g *Group) Status() topology.GroupStatus {
	s := g.RaftStatus()
	pt := tracker.ProgressTracker{Config: s.Config}
	result := topology.GroupStatus{GroupID: g.opts.GroupID, HostNode: g.opts.NodeName, Found: true, LeaderID: s.Lead, Term: s.Term, Commit: s.Commit, Applied: s.Applied, ConfState: pt.ConfState(), Progress: make(map[uint64]topology.ReplicaProgress)}
	for id, p := range s.Progress {
		result.Progress[id] = topology.ReplicaProgress{Match: p.Match, RecentActive: p.RecentActive}
	}
	select {
	case <-g.done:
		result.Found = false
	default:
	}
	return result
}

type membershipView struct{ group *Group }

func (v membershipView) GroupID() string     { return v.group.GroupID() }
func (v membershipView) IsLeader() bool      { return v.group.IsLeader() }
func (v membershipView) Status() raft.Status { return v.group.RaftStatus() }
func (v membershipView) ProposeConfChangeV2(ctx context.Context, cc raftpb.ConfChangeV2) error {
	return v.group.ProposeConfChange(ctx, cc)
}
func (v membershipView) TransferLeader(ctx context.Context, target uint64) error {
	return v.group.TransferLeader(ctx, target)
}
func (g *Group) Membership() membership.RaftGroup { return membershipView{g} }

// Running reports worker lifetime independently of leader/readiness state.
func (g *Group) Running() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.started && !g.stopped && g.ctx != nil && g.ctx.Err() == nil
}
