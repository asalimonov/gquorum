package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Step validates the sender header and admits one Raft message under RPC
// ownership.
func (g *Group) Step(parent context.Context, header *metapb.Header, msg raftpb.Message) error {
	ctxHandle, err := g.beginRPC(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer done()
	identity := g.currentIdentity()
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrUnavailable
	}
	if header == nil || header.ClusterId != identity.ClusterID || header.MemberId != msg.From ||
		msg.To != identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	name := g.memberNode(msg.From)
	if msg.Type == raftpb.MsgSnap {
		name, err = g.snapshotSender(ctx, msg, identity, name)
		if err != nil {
			return err
		}
	}
	if name == "" || name != header.NodeName {
		return fault.ErrIdentityMismatch
	}
	err = engine.Step(ctx, msg)
	if err == nil {
		g.acceptAddressHints(header)
	}
	return err
}

// Call validates and dispatches a private authenticated meta protocol request.
func (g *Group) Call(parent context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	owned, err := g.beginRPC(parent)
	if err != nil {
		return metatransport.Reply(err)
	}
	defer owned.Release()
	ctx := owned.Context
	if !retainedProtocolMethod(method) {
		if err := g.ensureRunning(ctx); err != nil {
			return metatransport.Reply(err)
		}
	}
	identity := g.currentIdentity()
	if header == nil || header.ClusterId != identity.ClusterID {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	call := metaCall{Method: method, Header: header, Data: data, Identity: identity}
	if reply := g.readProtocolCall(ctx, call); reply != nil {
		return reply
	}
	if method != "InstallFence" && method != "MetaControl" && (header.MemberId == 0 ||
		g.memberNode(header.MemberId) != header.NodeName) {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	if reply := g.controlProtocolCall(ctx, call); reply != nil {
		return reply
	}
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	return g.leaderProtocolCall(ctx, call)
}
func (g *Group) leaderReply(err error) *metapb.Reply {
	reply := metatransport.Reply(err)
	if engine := g.engine.Load(); engine != nil {
		reply.LeaderId = engine.RaftStatus().Lead
		name := g.memberNode(reply.LeaderId)
		g.mu.RLock()
		reply.LeaderAddress = g.addresses[name]
		g.mu.RUnlock()
	}
	return reply
}

func (g *Group) propose(ctx context.Context, proposal any) (metaengine.ApplyResult, error) {
	return g.submitProposal(ctx, proposal, ordinaryRuntime)
}

func (g *Group) submitProposal(ctx context.Context, proposal any,
	access runtimeAccess) (metaengine.ApplyResult, error) {
	if err := g.ensureRuntime(ctx, access); err != nil {
		return metaengine.ApplyResult{}, err
	}
	data, err := metaengine.Encode(proposal)
	if err != nil {
		return metaengine.ApplyResult{}, fault.New(fault.Invalid, "invalid meta proposal", err)
	}
	var encoded []byte
	if g.IsLeader() || access == shutdownRuntime && g.engine.Load() != nil && g.engine.Load().IsLeader() {
		result, err := g.engine.Load().
			Propose(ctx, raftengine.Proposal{Data: data})
		if err != nil {
			return metaengine.ApplyResult{}, err
		}
		encoded = result.Data
	} else {
		encoded, err = g.forwardProposal(ctx, data)
		if err != nil {
			return metaengine.ApplyResult{}, err
		}
	}
	var result metaengine.ApplyResult
	if err = json.Unmarshal(encoded, &result); err != nil {
		return result, fault.New(fault.Unreadable, "invalid committed meta result", err)
	}
	if !result.Accepted {
		code := fault.Invalid
		switch string(result.Code) {
		case "version_conflict":
			code = fault.VersionConflict
		case "overloaded":
			code = fault.Overloaded
		case "authority_pending":
			code = fault.AuthorityPending
		case "authority_fenced":
			code = fault.AuthorityFenced
		case "authorization", "superseded":
			code = fault.OperatorAuthorization
		default: // Keep the default fault classification for other result codes.
		}
		if result.Placement != nil {
			return result, &topology.PlacementError{Code: code, Report: result.Placement.Clone()}
		}
		return result, fault.New(code, result.Reason, nil)
	}
	return result, nil
}

func (g *Group) forwardProposal(ctx context.Context, data []byte) ([]byte, error) {
	engine := g.engine.Load()
	if engine == nil || engine.RaftStatus().Lead == 0 {
		return nil, fault.ErrUnavailable
	}
	address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
	if err != nil {
		return nil, err
	}
	reply, err := g.transport.CallAddress(ctx, address, "ProposeMeta", data)
	if err != nil {
		return nil, err
	}
	if err = metatransport.ReplyError(reply); err != nil {
		return nil, err
	}
	return reply.Data, nil
}
