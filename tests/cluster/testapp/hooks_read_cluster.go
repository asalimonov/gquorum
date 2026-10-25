package main

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"strconv"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type readCounters struct {
	DropReadResponse      bool     `json:"drop_read_response"`
	ReadResponsesDropped  uint64   `json:"read_responses_dropped"`
	DropAppend            bool     `json:"drop_append"`
	DropRaft              bool     `json:"drop_raft"`
	DropVoteRequestsFrom  []uint64 `json:"drop_vote_requests_from,omitempty"`
	VoteRequestsDropped   uint64   `json:"vote_requests_dropped"`
	HoldApply             bool     `json:"hold_apply"`
	HoldQuery             bool     `json:"hold_query"`
	ApplyCalls            uint64   `json:"apply_calls"`
	ApplyActive           uint64   `json:"apply_active"`
	ApplyCompleted        uint64   `json:"apply_completed"`
	AppliedIndex          uint64   `json:"applied_index"`
	QueryCalls            uint64   `json:"query_calls"`
	QueryActive           uint64   `json:"query_active"`
	QueryCompleted        uint64   `json:"query_completed"`
	AppendDropped         uint64   `json:"append_dropped"`
	RaftDropped           uint64   `json:"raft_dropped"`
	ReadRequestsSent      uint64   `json:"read_requests_sent"`
	ReadContextsSent      uint64   `json:"read_contexts_sent"`
	ReadResponsesReceived uint64   `json:"read_responses_received"`
	ReadResponseIndex     uint64   `json:"read_response_index"`
}
type readControl struct {
	counters                   readCounters
	applyRelease, queryRelease chan struct{}
	lastContext                []byte // one bounded diagnostic context, never quorum authority
}
type controlledMachine struct {
	*kvMachine
	hooks *clusterHooks
	key   walKey
}

func (a *application) clusterMachine(machine *kvMachine, key walKey) load.StateMachine {
	if a.clusterHooks == nil {
		return machine
	}
	h := a.clusterHooks
	h.mu.Lock()
	if h.reads[key] == nil {
		h.reads[key] = &readControl{}
	}
	h.mu.Unlock()
	return &controlledMachine{kvMachine: machine, hooks: h, key: key}
}

// Node calls application Close only after the machine owner joins. Removing
// these volatile controls then cannot strand a callback or affect a successor.
func (a *application) forgetReadControl(key walKey) {
	if a.clusterHooks != nil {
		a.clusterHooks.mu.Lock()
		delete(a.clusterHooks.reads, key)
		a.clusterHooks.mu.Unlock()
	}
}
func (m *controlledMachine) before(ctx context.Context, query bool) error {
	h := m.hooks
	h.mu.Lock()
	c := h.reads[m.key]
	release := c.applyRelease
	if query {
		c.counters.QueryCalls++
		c.counters.QueryActive++
		release = c.queryRelease
	} else {
		c.counters.ApplyCalls++
		c.counters.ApplyActive++
	}
	h.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}
func (m *controlledMachine) after(query bool, index uint64, err error) {
	h := m.hooks
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.reads[m.key]
	if query {
		c.counters.QueryActive--
		if err == nil {
			c.counters.QueryCompleted++
		}
	} else {
		c.counters.ApplyActive--
		if err == nil {
			c.counters.ApplyCompleted++
			c.counters.AppliedIndex = index
		}
	}
}
func (m *controlledMachine) Apply(ctx context.Context, command load.Command) (result load.Result, err error) {
	err = m.before(ctx, false)
	defer func() { m.after(false, command.Index, err) }()
	if err != nil {
		return load.Result{}, err
	}
	return m.kvMachine.Apply(ctx, command)
}
func (m *controlledMachine) Query(ctx context.Context, data []byte) (result load.Result, err error) {
	err = m.before(ctx, true)
	defer func() { m.after(true, 0, err) }()
	if err != nil {
		return load.Result{}, err
	}
	return m.kvMachine.Query(ctx, data)
}
func (h *clusterHooks) readMetrics(key walKey) readCounters {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c := h.reads[key]; c != nil {
		owned := c.counters
		owned.DropVoteRequestsFrom = slices.Clone(owned.DropVoteRequestsFrom)
		return owned
	}
	return readCounters{}
}
func (a *application) recordReadSend(batch load.RaftBatch) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.reads[walKey{batch.From.Replica.GroupID, batch.From.Replica.ReplicaID}]
	if c == nil {
		return
	}
	for _, msg := range batch.Messages {
		if msg.Type == raftpb.MsgReadIndex && len(msg.Entries) == 1 {
			c.counters.ReadRequestsSent++
			if !bytes.Equal(c.lastContext, msg.Entries[0].Data) {
				c.counters.ReadContextsSent++
				c.lastContext = bytes.Clone(msg.Entries[0].Data)
			}
		}
	}
}

// Fault drops preserve the exact destination. Remaining messages still pass
// Node's normal authenticated identity and Raft validation before application.
func (a *application) filterReadRaft(batch load.RaftBatch) (load.RaftBatch, error) {
	if a.clusterHooks == nil || batch.To.Node != a.currentNode().Identity() {
		return batch, nil
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.reads[walKey{batch.To.Replica.GroupID, batch.To.Replica.ReplicaID}]
	if c == nil {
		return batch, nil
	}
	kept := make([]raftpb.Message, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		dropVote := (msg.Type == raftpb.MsgVote || msg.Type == raftpb.MsgPreVote) && slices.Contains(c.counters.DropVoteRequestsFrom, msg.From)
		if c.counters.DropRaft || dropVote || c.counters.DropReadResponse && msg.Type == raftpb.MsgReadIndexResp || c.counters.DropAppend && (msg.Type == raftpb.MsgApp || msg.Type == raftpb.MsgSnap) {
			c.counters.RaftDropped++
			if dropVote {
				c.counters.VoteRequestsDropped++
			}
			if msg.Type == raftpb.MsgReadIndexResp {
				c.counters.ReadResponsesDropped++
			}
			if msg.Type == raftpb.MsgApp {
				c.counters.AppendDropped++
			}
			if msg.Type == raftpb.MsgSnap {
				return batch, fault.ErrUnavailable // a dropped snapshot cannot report delivery success
			}
			continue
		}
		kept = append(kept, msg)
	}
	batch.Messages = kept
	return batch, nil
}
func (a *application) recordReadReceive(batch load.RaftBatch) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if c := h.reads[walKey{batch.To.Replica.GroupID, batch.To.Replica.ReplicaID}]; c != nil {
		for _, msg := range batch.Messages {
			if msg.Type == raftpb.MsgReadIndexResp {
				c.counters.ReadResponsesReceived++
				c.counters.ReadResponseIndex = msg.Index
			}
		}
	}
}
func (a *application) registerReadRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mutate := func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		replica, err := strconv.ParseUint(r.PathValue("replica"), 10, 64)
		if err != nil || replica == 0 || topology.ValidateLoadID(group) != nil {
			httpError(w, fault.ErrInvalid)
			return
		}
		var body struct {
			Identity             topology.Identity `json:"identity"`
			DropReadResponse     bool              `json:"drop_read_response"`
			DropAppend           bool              `json:"drop_append"`
			DropRaft             bool              `json:"drop_raft"`
			DropVoteRequestsFrom []uint64          `json:"drop_vote_requests_from"`
			HoldApply            bool              `json:"hold_apply"`
			HoldQuery            bool              `json:"hold_query"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if body.Identity != a.currentNode().Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		seen := make(map[uint64]bool)
		if len(body.DropVoteRequestsFrom) > 128 {
			httpError(w, fault.ErrInvalid)
			return
		}
		for _, sender := range body.DropVoteRequestsFrom {
			if sender == 0 || seen[sender] {
				httpError(w, fault.ErrInvalid)
				return
			}
			seen[sender] = true
		}
		if r.Method == "PUT" {
			g, found := a.currentNode().LoadGroup(group)
			if !found || g.ReplicaID() != replica {
				httpError(w, fault.ErrIdentityMismatch)
				return
			}
		}
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		c := h.reads[walKey{group, replica}]
		if c == nil {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		if r.Method == "PUT" {
			if c.counters.DropAppend || c.counters.DropRaft || c.counters.DropReadResponse || len(c.counters.DropVoteRequestsFrom) != 0 || c.applyRelease != nil || c.queryRelease != nil || !(body.DropAppend || body.DropRaft || body.DropReadResponse || body.HoldApply || body.HoldQuery || len(body.DropVoteRequestsFrom) != 0) {
				httpError(w, fault.ErrInvalid)
				return
			}
			c.counters.DropReadResponse = body.DropReadResponse
			c.counters.DropVoteRequestsFrom = slices.Clone(body.DropVoteRequestsFrom)
			c.counters.DropAppend, c.counters.DropRaft = body.DropAppend, body.DropRaft
			c.counters.HoldApply, c.counters.HoldQuery = body.HoldApply, body.HoldQuery
			if body.HoldApply {
				c.applyRelease = make(chan struct{})
			}
			if body.HoldQuery {
				c.queryRelease = make(chan struct{})
			}
		} else {
			if c.applyRelease != nil {
				close(c.applyRelease)
				c.applyRelease = nil
			}
			if c.queryRelease != nil {
				close(c.queryRelease)
				c.queryRelease = nil
			}
			c.counters.DropReadResponse = false
			c.counters.DropVoteRequestsFrom = nil
			c.counters.DropAppend, c.counters.DropRaft, c.counters.HoldApply, c.counters.HoldQuery = false, false, false, false
		}
		jsonResponse(w, http.StatusOK, c.counters)
	}
	mux.HandleFunc("PUT /internal/test/reads/{group}/{replica}", protect(mutate))
	mux.HandleFunc("DELETE /internal/test/reads/{group}/{replica}", protect(mutate))
}
