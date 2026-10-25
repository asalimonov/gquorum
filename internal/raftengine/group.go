package raftengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/internal/replicastate"
	"github.com/asalimonov/gquorum/internal/watchstate"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

const (
	proposalQueueCapacity      = 256
	inboundQueueCapacity       = 1024
	controlQueueCapacity       = 16
	snapshotQueueCapacity      = 16
	unreachableQueueCapacity   = 256
	outboundBatchQueueCapacity = 256
	engineWorkerCount          = 4 // Raft loop, writer, sender and metrics sampler.
)

type response struct {
	result Result
	err    error
}
type request struct {
	ownedSnapshot      *ownedSnapshotTicket
	preflightReply     chan snapshotPreflightReply
	expectedTerm       uint64
	ctx                context.Context
	correlation        string
	proposal           Proposal
	messages           []raftpb.Message
	change             *raftpb.ConfChangeV2
	admin              *raftstate.Entry
	expected           *raftpb.ConfState
	transfer           uint64
	snapshot           bool
	recoveryCheckpoint *model.SeedKey
	consumed           *model.Consumed
	reply              chan response
	retried            bool
	retryAt            time.Time
}

// Group owns one exact persistence-ordered Raft replica and its serialized
// application work.
type Group struct {
	snapshotEpoch           context.Context
	cancelSnapshotEpoch     context.CancelFunc
	snapshotEpochTerm       uint64
	snapshotPublication     uint64
	snapshotAttemptSequence uint64
	snapshotReportMu        sync.Mutex
	snapshotReports         map[uint64]*snapshotReportState
	snapshotReportWake      chan struct{}
	snapshotReportsClosed   bool

	snapshotIngress                           chan *request
	activeSnapshot                            *ownedSnapshotTicket
	headerOnlyTicket                          *request
	snapshotBounds                            SnapshotBounds
	observations                              *watchstate.Latest[replicastate.Observation]
	observation                               replicastate.Observation
	leadership                                LeadershipEpoch
	cancelLeadership                          context.CancelFunc
	leadershipChanged                         chan struct{}
	supervised, stopping                      bool
	opts                                      Options
	rn                                        *raft.RawNode
	ms                                        *raft.MemoryStorage
	checkpointMemory                          *checkpointMemory
	recoveryKey                               *model.SeedKey
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
	sendC                                     chan []raftpb.Message
	sendWorkers                               atomic.Int64
	writeC                                    chan writeJob
	writeDone                                 chan writeResult
	hint                                      appliedHint
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

// New validates a load replica and restores its retained consensus state before
// workers start.
func New(opts Options) (*Group, error) {
	return NewContext(context.Background(), opts)
}

// NewContext binds storage and restores the machine under its owner's startup
// context. It starts no workers and never closes borrowed dependencies.
func NewContext(ctx context.Context, opts Options) (*Group, error) {
	return newGroup(ctx, opts, loadEngine)
}

// NewMeta is restricted to the library's internal topology runtime.
func NewMeta(opts Options) (*Group, error) {
	return newGroup(context.Background(), opts, metaEngine)
}
func newGroup(parent context.Context, opts Options, mode engineMode) (*Group, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if err := opts.validateMode(mode); err != nil {
		return nil, err
	}
	options := ownGroupOptions(opts.defaults())
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	state, err := loadGroupRecovery(ctx, options)
	if err != nil {
		return nil, err
	}
	if err := initializeFounding(ctx, options, &state); err != nil {
		return nil, err
	}
	return newRestoredCore(ctx, options, state)
}

// Start begins engine work under the supplied lifetime context.
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
	g.wg.Add(engineWorkerCount)
	go g.run()
	go g.writer()
	go g.sender()
	go g.sampleMetrics()
	go func() {
		g.wg.Wait()
		g.finishSnapshotIngressAfterJoin()
		g.closeSnapshotReportsAfterJoin()
		g.finishObservations()
		close(g.done)
	}()
	return nil
}

// RequestStop cancels engine participation without waiting for owned workers.
func (g *Group) RequestStop() {
	g.mu.Lock()
	finish := !g.started && !g.stopped
	g.stopping = true
	if finish {
		g.stopped = true
		if g.err == nil {
			g.err = fault.ErrStopped
		}
		g.meter.Stop()
	}
	if g.cancel != nil {
		g.cancel()
	}
	g.publishObservationLocked(regularObservation)
	g.mu.Unlock()
	if finish {
		go func() { g.closeSnapshotReportsAfterJoin(); g.finishObservations(); close(g.done) }()
	}
}

// Fail preserves an external owner's failure and begins the same joined stop.
func (g *Group) Fail(err error) {
	if err == nil {
		return
	}
	g.mu.Lock()
	if g.err == nil {
		g.err = err
	}
	g.mu.Unlock()
	g.RequestStop()
}

// Stop requests engine teardown and waits for all engine-owned work to join.
func (g *Group) Stop(ctx context.Context) error {
	g.RequestStop()
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "load group workers have not stopped", ctx.Err())
	}
}

// WaitReady waits for replay readiness or engine failure without canceling its
// lifetime.
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

// RunContext belongs to the engine lifetime after Start. Managed callbacks
// inherit it so engine failure cancels them before resource release can join.
func (g *Group) RunContext() context.Context { g.mu.RLock(); defer g.mu.RUnlock(); return g.ctx }

// RunID returns this exact engine incarnation's local observation-series
// identity.
func (g *Group) RunID() string { return g.nonce }

func (g *Group) Error() error { g.mu.RLock(); defer g.mu.RUnlock(); return g.err }

// GroupID returns the configured Raft group name.
func (g *Group) GroupID() string { return g.opts.GroupID }

// ReplicaID returns the exact numeric Raft replica identity.
func (g *Group) ReplicaID() uint64 { return g.opts.ReplicaID }

// IsLeader reports leadership from the cached local Raft observation.
func (g *Group) IsLeader() bool { return g.RaftStatus().RaftState == raft.StateLeader }

// WALStall returns the elapsed duration of an outstanding persistence stall.
func (g *Group) WALStall() time.Duration {
	n := g.stallSince.Load()
	if n == 0 {
		return 0
	}
	return time.Since(time.Unix(0, n))
}

// Propose waits for local committed application. Cancellation can leave an
// uncertain outcome: a submitted command may still commit. RequestID is passed
// to Machine for application-owned durable deduplication; it does not
// deduplicate commands in the runtime. Use one running Group per local
// group/replica identity.
func (g *Group) Propose(ctx context.Context, p Proposal) (Result, error) {
	return g.submitProposal(ctx, p, 0)
}

func (g *Group) submitProposal(parent context.Context, p Proposal, expectedTerm uint64) (result Result, err error) {
	ctx := parent
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
		default: // Keep the applied outcome when no error applies.
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
	req := &request{
		ctx:          ctx,
		expectedTerm: expectedTerm,
		correlation:  g.newID(),
		proposal:     Proposal{RequestID: p.RequestID, Data: bytes.Clone(p.Data)},
		reply:        make(chan response, 1),
	}
	return g.submit(ctx, g.proposals, req)
}

// ProposalStats describes local proposal admission. InFlight includes callers
// waiting to enqueue or receive a result. AwaitingCommit is the last run-loop
// observation of registered waiters; canceled callers are pruned independently
// of persistence. The fields are observations, not a transactional snapshot.
type ProposalStats struct {
	Limit, InFlight, Queued, AwaitingCommit int
}

// ProposalStats reports bounded local proposal occupancy and commit waits.
func (g *Group) ProposalStats() ProposalStats {
	g.mu.RLock()
	pending := g.cachedPending
	g.mu.RUnlock()
	return ProposalStats{
		Limit:          cap(g.slots),
		InFlight:       len(g.slots),
		Queued:         len(g.proposals),
		AwaitingCommit: pending,
	}
}

func (g *Group) newID() string { return fmt.Sprintf("%s-%d", g.nonce, g.sequence.Add(1)) }

func (g *Group) submit(ctx context.Context, queue chan *request, req *request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	g.mu.RLock()
	started, stopped := g.started, g.stopped || g.stopping || g.started && g.ctx.Err() != nil
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

// Step delivers one owned Raft message through the bounded ingress path.
func (g *Group) Step(ctx context.Context, msg raftpb.Message) error {
	return g.StepBatch(ctx, []raftpb.Message{msg})
}

// StepBatch validates and copies every message, then steps them in order in one
// loop turn. It stops at the first refused message; earlier ones stay stepped.
func (g *Group) StepBatch(ctx context.Context, msgs []raftpb.Message) error {
	owned := make([]raftpb.Message, 0, len(msgs))
	for _, msg := range msgs {
		copied, err := g.validateRemote(msg)
		if err != nil {
			if len(owned) > 0 {
				_, submitErr := g.submit(ctx, g.inbound,
					&request{ctx: ctx, messages: owned, reply: make(chan response, 1)})
				if submitErr != nil {
					return submitErr
				}
			}
			return err
		}
		owned = append(owned, copied)
	}
	_, err := g.submit(ctx, g.inbound, &request{ctx: ctx, messages: owned, reply: make(chan response, 1)})
	return err
}

func (g *Group) validateRemote(msg raftpb.Message) (raftpb.Message, error) {
	if g.opts.ManagedAuthority && msg.Type == raftpb.MsgTransferLeader {
		return raftpb.Message{}, fault.ErrUnsupported
	}
	_, known := raftpb.MessageType_name[int32(msg.Type)]
	if !known || msg.From == 0 || msg.From == g.opts.ReplicaID || msg.To != g.opts.ReplicaID ||
		raft.IsLocalMsg(msg.Type) {
		return raftpb.Message{}, fault.New(fault.Invalid, "not a remote message for this replica", nil)
	}
	if err := g.validateRemotePayload(msg); err != nil {
		return raftpb.Message{}, err
	}
	return copyMessage(msg)
}

// ProposeConfChange submits a single automatic standalone membership change;
// managed replicas require grants.
func (g *Group) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeV2) error {
	if g.opts.ManagedAuthority {
		return fault.ErrUnsupported
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 || cc.Changes[0].NodeID == 0 {
		return fault.ErrUnsafeMembership
	}
	if typ := cc.Changes[0].Type; typ != raftpb.ConfChangeAddNode && typ != raftpb.ConfChangeAddLearnerNode &&
		typ != raftpb.ConfChangeRemoveNode {
		return fault.ErrUnsafeMembership
	}
	cc.Changes = slices.Clone(cc.Changes)
	cc.Context = bytes.Clone(cc.Context)
	_, err := g.submit(ctx, g.controls, &request{ctx: ctx, correlation: g.newID(), change: &cc, reply: make(
		chan response, 1)})
	return err
}

// TransferLeader requests standalone leadership transfer; managed replicas
// require granted transfer.
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

// Snapshot requests a complete application snapshot under a bounded wait.
func (g *Group) Snapshot(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := g.submit(bounded, g.snapshots, &request{ctx: bounded, snapshot: true, reply: make(chan response, 1)})
	return err
}

// ReportUnreachable queues a destination failure observation without blocking.
func (g *Group) ReportUnreachable(id uint64) {
	select {
	case g.unreachable <- id:
	default:
	}
}

// ReportSnapshot adapts a legacy destination-only report to its current offer.
// Legacy transports cannot distinguish their own delayed prior attempts; managed
// transfers always use the exact SnapshotAttempt API instead.
func (g *Group) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	g.snapshotReportMu.Lock()
	record := g.snapshotReports[id]
	var attempt SnapshotAttempt
	if record != nil {
		attempt = record.attempt
	}
	g.snapshotReportMu.Unlock()
	// Legacy _meta and standalone callers retain immediate native reports.
	g.recordSnapshotReport(attempt, status, snapshotLegacyReport)
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

// RaftStatus returns an owned copy of cached local Raft membership and
// progress.
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

// Status adapts cached Raft state to a local topology group observation.
func (g *Group) Status() topology.GroupStatus {
	s := g.RaftStatus()
	pt := tracker.ProgressTracker{Config: s.Config}
	result := topology.GroupStatus{
		GroupID:   g.opts.GroupID,
		HostNode:  g.opts.NodeName,
		Found:     true,
		LeaderID:  s.Lead,
		Term:      s.Term,
		Commit:    s.Commit,
		Applied:   s.Applied,
		ConfState: pt.ConfState(),
		Progress:  make(map[uint64]topology.ReplicaProgress),
	}
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

// Membership returns the standalone membership-control view used by safety
// coordination.
func (g *Group) Membership() membership.RaftGroup { return membershipView{g} }

// Running reports worker lifetime independently of leader/readiness state.
func (g *Group) Running() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.started && !g.stopped && g.ctx != nil && g.ctx.Err() == nil
}
