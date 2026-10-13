package raftengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const readContextSize = 1 + 16 + 8 + 8

type readRound struct {
	context    []byte
	requests   []*readRequest
	lastIssued time.Time
	submitted  bool
}
type remoteReadRound struct {
	message   raftpb.Message
	submitted bool
}
type readState struct {
	term, leader   uint64
	role           raft.StateType
	active         *readRound
	next, applying []*readRequest
	remote         map[uint64]*remoteReadRound
}

func (g *Group) admitRead(req *readRequest) {
	if err := req.ctx.Err(); err != nil {
		g.finishRead(req, Result{}, err)
		return
	}
	g.readState.next = append(g.readState.next, req)
}
func (g *Group) pruneReadList(requests []*readRequest) []*readRequest {
	kept := requests[:0]
	for _, req := range requests {
		if err := req.ctx.Err(); err != nil {
			g.finishRead(req, Result{}, err)
		} else {
			kept = append(kept, req)
		}
	}
	clear(requests[len(kept):])
	return kept
}
func (g *Group) resetReadRounds() {
	status := g.rn.BasicStatus()
	s := &g.readState
	if status.Term != s.term || status.Lead != s.leader || status.RaftState != s.role {
		g.failReadRounds(fault.ErrUnavailable)
		s.term, s.leader, s.role = status.Term, status.Lead, status.RaftState
	}
	s.next = g.pruneReadList(s.next)
	s.applying = g.pruneReadList(s.applying)
	if s.active != nil {
		s.active.requests = g.pruneReadList(s.active.requests)
	}
}
func (g *Group) failReadRounds(err error) {
	s := &g.readState
	if s.active != nil {
		for _, req := range s.active.requests {
			g.finishRead(req, Result{}, err)
		}
	}
	for _, requests := range [][]*readRequest{s.next, s.applying} {
		for _, req := range requests {
			g.finishRead(req, Result{}, err)
		}
	}
	s.active, s.next, s.applying = nil, nil, nil
	clear(s.remote)
}
func (g *Group) currentTermCommitted() bool {
	status := g.rn.BasicStatus()
	term, err := g.ms.Term(status.Commit)
	return err == nil && status.Commit != 0 && term == status.Term
}
func (g *Group) newReadContext() []byte {
	token := make([]byte, readContextSize)
	token[0] = 1
	// nonce was constructed from a 16-byte crypto/rand seed.
	_, _ = hex.Decode(token[1:17], []byte(g.nonce))
	binary.BigEndian.PutUint64(token[17:25], g.sequence.Add(1))
	binary.BigEndian.PutUint64(token[25:33], g.opts.ReplicaID)
	return token
}
func (g *Group) issueReadRound(now time.Time) {
	s := &g.readState
	if s.leader == 0 {
		return
	}
	if s.role == raft.StateLeader && !g.currentTermCommitted() {
		return
	}
	if s.active == nil && len(s.next) > 0 {
		s.active = &readRound{context: g.newReadContext(), requests: s.next}
		s.next = nil // freeze cohort before ReadIndex; later arrivals need another round
	}
	if s.active != nil && !s.active.submitted {
		g.rn.ReadIndex(bytes.Clone(s.active.context))
		s.active.submitted, s.active.lastIssued = true, now
	}
	if s.role == raft.StateLeader {
		for _, pending := range s.remote {
			if !pending.submitted {
				_ = g.rn.Step(pending.message) // fixed shape, admitted source, current term
				pending.submitted = true
			}
		}
	}
}
func (g *Group) retryReadRound(now time.Time) {
	s := &g.readState
	if s.active != nil && s.active.submitted && s.role == raft.StateFollower && s.leader != 0 &&
		now.Sub(s.active.lastIssued) >= time.Duration(g.opts.Timing.ElectionTicks)*g.opts.Timing.TickInterval {
		g.rn.ReadIndex(bytes.Clone(s.active.context)) // same context, never more wrapper rounds
		s.active.lastIssued = now
	}
	g.issueReadRound(now)
}
func (g *Group) acceptReadStates(states []raft.ReadState) {
	s := &g.readState
	for _, state := range states {
		if s.active == nil || state.Index == 0 || !bytes.Equal(state.RequestCtx, s.active.context) {
			continue
		}
		for _, req := range s.active.requests {
			req.index = state.Index
			s.applying = append(s.applying, req)
		}
		s.active = nil
	}
}
func (g *Group) completeAppliedReads() {
	s := &g.readState
	pending := s.applying
	s.applying = nil
	for _, req := range pending {
		if req.index > g.applied {
			s.applying = append(s.applying, req)
			continue
		}
		if err := req.ctx.Err(); err != nil {
			g.finishRead(req, Result{}, err)
			continue
		}
		if err := g.ctx.Err(); err != nil {
			g.finishRead(req, Result{}, fault.ErrStopped)
			continue
		}
		if req.native {
			result, err := authorityResult(g.nativeState())
			g.finishRead(req, result, err)
			continue
		}
		if req.authorityKey != nil {
			result, err := g.readAuthorityReceipt(*req.authorityKey)
			g.finishRead(req, result, err)
			continue
		}
		if req.barrier {
			g.finishRead(req, Result{}, nil)
			continue
		}
		machine, ok := g.opts.Machine.(QueryMachine)
		if !ok || !machine.SupportsQuery() {
			g.finishRead(req, Result{}, fault.ErrUnsupported)
			continue
		}
		ctx, cancel := context.WithCancel(req.ctx)
		stop := context.AfterFunc(g.ctx, cancel)
		result, err := machine.Query(ctx, bytes.Clone(req.data))
		if err == nil {
			err = ctx.Err()
		}
		if len(result.Data) > req.maxResultBytes {
			err = fault.ErrOverloaded
		}
		// Copy before releasing callback ownership or allowing another machine call.
		g.finishRead(req, result, err)
		stop()
		cancel()
	}
}

func readMessageContext(msg raftpb.Message) ([]byte, bool) {
	if len(msg.Entries) != 1 {
		return nil, false
	}
	entry := msg.Entries[0]
	token := entry.Data
	if entry.Type != raftpb.EntryNormal || entry.Index != 0 || entry.Term != 0 || len(token) != readContextSize || token[0] != 1 ||
		binary.BigEndian.Uint64(token[17:25]) == 0 || binary.BigEndian.Uint64(token[25:33]) == 0 {
		return nil, false
	}
	if msg.LogTerm != 0 || msg.Commit != 0 || msg.Vote != 0 || msg.Reject || msg.RejectHint != 0 || len(msg.Context) != 0 || len(msg.Responses) != 0 || msg.Snapshot != nil {
		return nil, false
	}
	return token, true
}

// validateRemoteRead runs on the owner loop and admits at most one upstream
// round per source. Duplicate requests held before current-term commit never
// reach RawNode's unbounded pendingReadIndexMessages slice.
func (g *Group) validateRemoteRead(msg raftpb.Message) error {
	token, ok := readMessageContext(msg)
	if !ok || msg.Term != 0 || msg.Index != 0 || binary.BigEndian.Uint64(token[25:33]) != msg.From {
		return fault.ErrInvalid
	}
	s := &g.readState
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		return fault.ErrNotLeader
	}
	if !slices.Contains(g.cs.Voters, msg.From) && !slices.Contains(g.cs.Learners, msg.From) {
		return fault.ErrIdentityMismatch
	}
	if previous := s.remote[msg.From]; previous != nil {
		if bytes.Equal(previous.message.Entries[0].Data, token) {
			return nil
		}
		return fault.ErrOverloaded
	}
	if len(s.remote) >= g.opts.Reads.MaxPending {
		return fault.ErrOverloaded
	}
	if s.remote == nil {
		s.remote = make(map[uint64]*remoteReadRound)
	}
	// Step owns a deep message copy; retain that owned copy until response/reset.
	s.remote[msg.From] = &remoteReadRound{message: msg}
	g.issueReadRound(time.Now())
	return nil
}
func (g *Group) validateReadIndexResponse(msg raftpb.Message) error {
	token, ok := readMessageContext(msg)
	status := g.rn.BasicStatus()
	if !ok || msg.Index == 0 || binary.BigEndian.Uint64(token[25:33]) != g.opts.ReplicaID {
		return fault.ErrInvalid
	}
	if status.RaftState != raft.StateFollower || status.Lead == 0 || msg.From != status.Lead || msg.Term != status.Term {
		return fault.ErrUnavailable
	}
	if g.readState.active == nil || !bytes.Equal(g.readState.active.context, token) {
		return fault.ErrUnavailable
	}
	return nil
}
func (g *Group) releaseRemoteReads(messages []raftpb.Message) {
	for _, msg := range messages {
		if msg.Type != raftpb.MsgReadIndexResp || len(msg.Entries) != 1 {
			continue
		}
		pending := g.readState.remote[msg.To]
		if pending != nil && bytes.Equal(pending.message.Entries[0].Data, msg.Entries[0].Data) {
			delete(g.readState.remote, msg.To)
		}
	}
}
