package raftengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"slices"
	"time"
)

// Ordinary proposal forwarding follows Meridian's handleProposalRG: use
// RawNode.Propose on the local replica and await that replica's local apply.
// Retries stay on the owner loop; ambiguous transport outcomes are never retried.
func (g *Group) retryDroppedProposals(now time.Time) {
	for _, req := range g.props {
		if req.retryAt.IsZero() || now.Before(req.retryAt) {
			continue
		}
		req.retryAt = time.Time{}
		g.handleProposal(req)
	}
}

func validateForwardedProposal(o Options, msg raftpb.Message) error {
	if o.GroupID == topology.MetaGroupID || msg.Term != 0 || len(msg.Entries) != 1 {
		return fault.New(fault.Invalid, "only one ordinary load proposal can be forwarded", nil)
	}
	if msg.LogTerm != 0 || msg.Index != 0 || msg.Commit != 0 || msg.Vote != 0 || msg.Reject || msg.RejectHint != 0 || len(msg.Context) != 0 || len(msg.Responses) != 0 || msg.Snapshot != nil {
		return fault.New(fault.Invalid, "forwarded proposal contains consensus-control fields", nil)
	}
	e := msg.Entries[0]
	if e.Type != raftpb.EntryNormal || e.Term != 0 || e.Index != 0 || len(e.Data) == 0 {
		return fault.New(fault.Invalid, "invalid forwarded proposal entry", nil)
	}
	if uint64(len(e.Data)) > o.MaxUncommittedBytes {
		return fault.ErrOverloaded
	}
	value, err := raftstate.DecodeEntry(e.Data)
	if err != nil || value.Kind != raftstate.Application {
		return fault.New(fault.Invalid, "forwarded entry is not an application command", err)
	}
	return nil
}

// stepRemote runs on the RawNode loop. Checking leadership here prevents a
// stale leader from forwarding under somebody else's authenticated sender ID.
func (g *Group) stepRemote(msg raftpb.Message) error {
	g.resetReadRounds()
	if msg.Type == raftpb.MsgReadIndex {
		return g.validateRemoteRead(msg)
	}
	if msg.Type == raftpb.MsgReadIndexResp {
		if err := g.validateReadIndexResponse(msg); err != nil {
			return err
		}
	}
	if msg.Type == raftpb.MsgProp {
		if g.rn.BasicStatus().RaftState != raft.StateLeader {
			return fault.ErrNotLeader
		}
		if !slices.Contains(g.cs.Voters, msg.From) && !slices.Contains(g.cs.Learners, msg.From) {
			return fault.ErrIdentityMismatch
		}
		select {
		case <-g.ready:
		default:
			return fault.ErrReplicaNotReady
		}
		if g.WALStall() > time.Second {
			return fault.ErrOverloaded
		}
	}
	return g.rn.Step(msg)
}
