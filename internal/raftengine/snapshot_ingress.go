//revive:disable:max-public-structs // Keep the internal ingress contract together.
package raftengine

import (
	"context"
	"reflect"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const snapshotIngressHeaderLimit = 64 << 10

type snapshotPayloadMode uint8

const (
	snapshotHeaderOnly snapshotPayloadMode = iota
	snapshotComplete
)

// SnapshotInstallOutcome describes completion by the exact native owner.
type SnapshotInstallOutcome uint8

// Native completion outcomes never infer installation from a successful Step.
const (
	SnapshotInstallUnknown SnapshotInstallOutcome = iota
	SnapshotInstalled
	SnapshotSkipped
	SnapshotInstallFailed
)

// SnapshotIngress transfers immutable encoded and prevalidated decoded buffers.
// Done must be nonblocking. After submission only Done releases ownership,
// including when the submitting caller stops waiting.
type SnapshotIngress struct {
	Message raftpb.Message
	Decoded raftstate.Snapshot
	Done    func(SnapshotInstallOutcome, error)
}

// SnapshotBounds are monotonic lower bounds from one engine incarnation.
type SnapshotBounds struct {
	RunID        string
	Term, Commit uint64
}

// SnapshotPreflight records an owner-loop decision, not payload installation.
type SnapshotPreflight struct {
	Term, Commit  uint64
	NotNeeded     bool
	HeaderStepped bool
}

type snapshotPreflightReply struct {
	value SnapshotPreflight
	err   error
}
type ownedSnapshotTicket struct {
	ingress  SnapshotIngress
	once     sync.Once
	selected bool // loop-owned; true only for the exact selected Ready
}

func (t *ownedSnapshotTicket) finish(outcome SnapshotInstallOutcome, err error) {
	t.once.Do(func() {
		if t.ingress.Done != nil {
			t.ingress.Done(outcome, err)
		}
		// A stopped handle or completed write job must not retain the decoded
		// application buffer or the coordinator callback after ownership ends.
		t.ingress = SnapshotIngress{}
	})
}

// SnapshotBounds observes native progress without authorizing native Step.
func (g *Group) SnapshotBounds() SnapshotBounds {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snapshotBounds
}

// PrepareOwnedSnapshot decodes on the caller's worker, never the native loop.
// Cancellation is checked after decoding because JSON decoding cannot be joined
// until it actually returns. The caller holds both buffers until completion.
func (g *Group) PrepareOwnedSnapshot(ctx context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return raftstate.Snapshot{}, err
	}
	if err := g.validateOwnedSnapshotShape(m, snapshotComplete); err != nil {
		return raftstate.Snapshot{}, err
	}
	decoded, err := raftstate.DecodeRaftSnapshot(*m.Snapshot, g.opts.ClusterID, g.opts.GroupID)
	if err != nil {
		return raftstate.Snapshot{}, err
	}
	if err := validateAuthoritySnapshot(g.opts, decoded); err != nil {
		return raftstate.Snapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return raftstate.Snapshot{}, err
	}
	return decoded, nil
}

// PreflightSnapshot asks the exact native owner whether an empty-data header is
// provably obsolete. A busy boundary never steps it.
func (g *Group) PreflightSnapshot(ctx context.Context, header raftpb.Message) (SnapshotPreflight, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotPreflight{}, err
	}
	if err := g.validateOwnedSnapshotShape(header, snapshotHeaderOnly); err != nil {
		return SnapshotPreflight{}, err
	}
	owned, err := copyMessage(header)
	if err != nil {
		return SnapshotPreflight{}, err
	}
	req := &request{ctx: ctx, messages: []raftpb.Message{owned}, preflightReply: make(chan snapshotPreflightReply, 1)}
	if _, err := g.enqueueSnapshotIngress(ctx, req); err != nil {
		return SnapshotPreflight{}, err
	}
	select {
	case reply := <-req.preflightReply:
		return reply.value, reply.err
	case <-ctx.Done():
		return SnapshotPreflight{}, ctx.Err()
	case <-g.done:
		return SnapshotPreflight{}, g.exitError()
	}
}

// StepOwnedSnapshot accepts ownership even if the waiter is later canceled.
// PrepareOwnedSnapshot must have produced Decoded for this immutable Message.
func (g *Group) StepOwnedSnapshot(ctx context.Context, in SnapshotIngress) error {
	ticket := &ownedSnapshotTicket{ingress: in}
	if err := g.validateOwnedSnapshotShape(in.Message, snapshotComplete); err != nil {
		ticket.finish(SnapshotInstallFailed, err)
		return err
	}
	if in.Decoded.Schema != raftstate.Schema || in.Decoded.ClusterID != g.opts.ClusterID ||
		in.Decoded.GroupID != g.opts.GroupID || len(in.Decoded.Application) == 0 {
		ticket.finish(SnapshotInstallFailed, fault.ErrInvalid)
		return fault.ErrInvalid
	}
	req := &request{ctx: ctx, ownedSnapshot: ticket, reply: make(chan response, 1)}
	enqueued, err := g.enqueueSnapshotIngress(ctx, req)
	if err != nil {
		if !enqueued {
			ticket.finish(SnapshotInstallFailed, err)
		}
		return err
	}
	select {
	case reply := <-req.reply:
		return reply.err
	case <-ctx.Done():
		return ctx.Err()
	case <-g.done:
		return g.exitError()
	}
}

// enqueueSnapshotIngress serializes publication with the stopped fence. A
// nonblocking bounded queue avoids a late send after joined cleanup drains it.
func (g *Group) enqueueSnapshotIngress(ctx context.Context, req *request) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started {
		return false, fault.ErrReplicaNotReady
	}
	if g.stopping || g.stopped || g.ctx.Err() != nil {
		return false, fault.ErrStopped
	}
	select {
	case g.snapshotIngress <- req:
		return true, nil
	default:
		return false, fault.ErrOverloaded
	}
}

func (g *Group) validateOwnedSnapshotShape(m raftpb.Message, mode snapshotPayloadMode) error {
	if m.Type != raftpb.MsgSnap || m.Term == 0 || m.From == 0 || m.From == g.opts.ReplicaID ||
		m.To != g.opts.ReplicaID || m.Snapshot == nil || len(m.Entries) != 0 || len(m.Responses) != 0 {
		return fault.New(fault.Invalid, "invalid owned snapshot header", nil)
	}
	ss := *m.Snapshot
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || ss.Metadata.Term > m.Term ||
		mode == snapshotHeaderOnly && len(ss.Data) != 0 || mode == snapshotComplete && len(ss.Data) == 0 {
		return fault.New(fault.Invalid, "invalid owned snapshot boundary", nil)
	}
	ss.Data = nil
	header := m
	header.Snapshot = &ss
	if header.Size() > snapshotIngressHeaderLimit {
		return fault.ErrInvalid
	}
	// Reuse the complete-snapshot membership validation with a constant sentinel.
	ss.Data = []byte{1}
	return validateSnapshot(ss)
}

func (g *Group) snapshotBoundaryReady(state *loopState) bool {
	return g.pending == nil && state.readyTimer == nil && g.activeSnapshot == nil &&
		g.headerOnlyTicket == nil && !g.rn.HasReady()
}

func (g *Group) handleSnapshotPreflight(req *request, state *loopState) {
	reply := func(value SnapshotPreflight, err error) { req.preflightReply <- snapshotPreflightReply{value, err} }
	if err := req.ctx.Err(); err != nil {
		reply(SnapshotPreflight{}, err)
		return
	}
	header := req.messages[0]
	if err := g.validateOwnedSnapshotShape(header, snapshotHeaderOnly); err != nil {
		reply(SnapshotPreflight{}, err)
		return
	}
	if !g.snapshotBoundaryReady(state) {
		reply(SnapshotPreflight{}, fault.ErrOverloaded)
		return
	}
	s := g.rn.BasicStatus()
	result := SnapshotPreflight{Term: s.Term, Commit: s.Commit}
	if header.Term < s.Term || header.Snapshot.Metadata.Index <= s.Commit {
		if err := g.rn.Step(header); err != nil {
			reply(result, err)
			return
		}
		result.NotNeeded, result.HeaderStepped = true, true
		if g.rn.HasReady() {
			g.headerOnlyTicket = req
		}
	}
	reply(result, nil)
}

func (g *Group) handleOwnedSnapshot(req *request, state *loopState) {
	refuse := func(err error) { req.ownedSnapshot.finish(SnapshotInstallFailed, err); answer(req, Result{}, err) }
	if err := req.ctx.Err(); err != nil {
		refuse(err)
		return
	}
	if !g.snapshotBoundaryReady(state) {
		refuse(fault.ErrOverloaded)
		return
	}
	if err := g.rn.Step(req.ownedSnapshot.ingress.Message); err != nil {
		refuse(err)
		return
	}
	g.activeSnapshot = req.ownedSnapshot
	if !g.rn.HasReady() {
		g.finishOwnedSnapshot(SnapshotSkipped, nil)
	}
	answer(req, Result{}, nil)
}

func (g *Group) classifyOwnedSnapshotReady(rd *raft.Ready) error {
	if g.headerOnlyTicket != nil {
		if !raft.IsEmptySnap(rd.Snapshot) {
			return fault.New(fault.Invalid, "header-only Step selected snapshot", nil)
		}
		g.headerOnlyTicket = nil
	}
	ticket := g.activeSnapshot
	if ticket == nil {
		return nil
	}
	offered := ticket.ingress.Message.Snapshot
	exact := len(rd.Snapshot.Data) == len(offered.Data) && len(rd.Snapshot.Data) != 0 &&
		&rd.Snapshot.Data[0] == &offered.Data[0] && reflect.DeepEqual(rd.Snapshot.Metadata, offered.Metadata)
	if !exact {
		g.finishOwnedSnapshot(SnapshotSkipped, nil)
		return nil
	}
	if err := validateSnapshotAuthorityProgress(g.authority, ticket.ingress.Decoded.Authority); err != nil {
		return err
	}
	ticket.selected = true
	return nil
}

func (g *Group) finishOwnedSnapshot(outcome SnapshotInstallOutcome, err error) {
	ticket := g.activeSnapshot
	if ticket == nil {
		return
	}
	g.activeSnapshot = nil
	ticket.finish(outcome, err)
}

func (g *Group) finishSnapshotIngressAfterJoin() {
	err := g.exitError()
	for {
		select {
		case req := <-g.snapshotIngress:
			if req.ownedSnapshot != nil {
				req.ownedSnapshot.finish(SnapshotInstallFailed, err)
			}
		default:
			g.finishOwnedSnapshot(SnapshotInstallFailed, err)
			g.headerOnlyTicket = nil
			return
		}
	}
}
