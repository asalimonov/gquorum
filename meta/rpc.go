package meta

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func (g *Group) Step(ctx context.Context, header *metapb.Header, msg raftpb.Message) error {
	identity := g.currentIdentity()
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrUnavailable
	}
	if header == nil || header.ClusterId != identity.ClusterID || header.MemberId != msg.From || msg.To != identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	name := g.memberNode(msg.From)
	if msg.Type == raftpb.MsgSnap {
		if msg.Snapshot == nil {
			return fault.ErrInvalid
		}
		check := metaengine.NewMachine(g.rules, nil)
		if err := check.Restore(ctx, msg.Snapshot.Data); err != nil {
			return err
		}
		latest := check.Latest()
		if latest.Version > 0 && latest.ClusterID != identity.ClusterID {
			return fault.ErrIdentityMismatch
		}
		if name == "" {
			for _, doc := range []topology.Document{check.Target(), check.Active(), latest} {
				if meta := doc.MetaGroup(); meta != nil {
					for _, replica := range meta.Replicas {
						if replica.ReplicaID == msg.From {
							name = replica.NodeName
						}
					}
				}
			}
		}
	}
	if name == "" || name != header.NodeName {
		return fault.ErrIdentityMismatch
	}
	return engine.Step(ctx, msg)
}
func (g *Group) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	identity := g.currentIdentity()
	if header == nil || header.ClusterId != identity.ClusterID {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	if method == "IdentityPing" {
		// The caller verifies the returned exact process identity before revoking a lease.
		encoded, err := json.Marshal(identity)
		if err != nil {
			return metatransport.Reply(err)
		}
		return &metapb.Reply{Data: encoded}
	}
	if method == "MetaStatus" {
		engine := g.engine.Load()
		if engine == nil {
			return metatransport.Reply(fault.ErrUnavailable)
		}
		encoded, err := json.Marshal(engine.Status())
		if err != nil {
			return metatransport.Reply(err)
		}
		return &metapb.Reply{Data: encoded, LeaderId: engine.RaftStatus().Lead}
	}
	if header.MemberId == 0 || g.memberNode(header.MemberId) != header.NodeName {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	switch method {
	case "ProposeMeta":
		proposal, err := metaengine.DecodeProposal(data)
		if err != nil {
			return metatransport.Reply(fault.New(fault.Invalid, "invalid meta command envelope", err))
		}
		switch p := proposal.(type) {
		case *metaengine.BindAdmissionProposal:
			if p.Admission.NodeName != header.NodeName || p.Admission.MemberID != header.MemberId {
				return metatransport.Reply(fault.ErrIdentityMismatch)
			}
		case *metaengine.GrantSession:
			if p.NodeName != header.NodeName || g.activeMemberID(p.NodeName) != header.MemberId {
				return metatransport.Reply(fault.ErrIdentityMismatch)
			}
		case *metaengine.AcquireLock:
			session, ok := g.state().Session(p.SessionID)
			if !ok || session.NodeName != header.NodeName {
				return metatransport.Reply(fault.ErrIdentityMismatch)
			}
		case *metaengine.RevokeSession:
			session, ok := g.state().Session(p.SessionID)
			if ok && session.NodeName != header.NodeName {
				return metatransport.Reply(fault.ErrIdentityMismatch)
			}
		}
		engine := g.engine.Load()
		result, err := engine.Propose(ctx, raftengine.Proposal{Data: data})
		if err != nil {
			return g.leaderReply(err)
		}
		return &metapb.Reply{Data: result.Data}
	case "KeepAlive":
		var request struct {
			SessionID  string `json:"session_id"`
			InstanceID string `json:"instance_id"`
		}
		if err := json.Unmarshal(data, &request); err != nil {
			return metatransport.Reply(fault.ErrInvalid)
		}
		g.leaseGate.Lock()
		defer g.leaseGate.Unlock()
		session, ok := g.state().Session(request.SessionID)
		if !ok || session.NodeName != header.NodeName || session.InstanceID != request.InstanceID || g.activeMemberID(header.NodeName) != header.MemberId {
			return metatransport.Reply(fault.ErrIdentityMismatch)
		}
		now := time.Now()
		g.leases.refreshTTL(request.SessionID, now, time.Duration(session.TTLSeconds)*time.Second)
		encoded, _ := json.Marshal(struct {
			ExpiresIn int `json:"expires_in"`
		}{g.leases.remaining(request.SessionID, now)})
		return &metapb.Reply{Data: encoded}
	default:
		return metatransport.Reply(fault.ErrUnsupported)
	}
}
func (g *Group) leaderReply(err error) *metapb.Reply {
	reply := metatransport.Reply(err)
	if engine := g.engine.Load(); engine != nil {
		reply.LeaderId = engine.RaftStatus().Lead
	}
	return reply
}
func (g *Group) propose(ctx context.Context, proposal any) (metaengine.ApplyResult, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return metaengine.ApplyResult{}, err
	}
	data, err := metaengine.Encode(proposal)
	if err != nil {
		return metaengine.ApplyResult{}, fault.New(fault.Invalid, "invalid meta proposal", err)
	}
	var encoded []byte
	if g.IsLeader() {
		result, err := g.engine.Load().Propose(ctx, raftengine.Proposal{Data: data})
		if err != nil {
			return metaengine.ApplyResult{}, err
		}
		encoded = result.Data
	} else {
		engine := g.engine.Load()
		if engine == nil || engine.RaftStatus().Lead == 0 {
			return metaengine.ApplyResult{}, fault.ErrUnavailable
		}
		address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
		if err != nil {
			return metaengine.ApplyResult{}, err
		}
		reply, err := g.transport.CallAddress(ctx, address, "ProposeMeta", data)
		if err != nil {
			return metaengine.ApplyResult{}, err
		}
		if err = metatransport.ReplyError(reply); err != nil {
			return metaengine.ApplyResult{}, err
		}
		encoded = reply.Data
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
		case "authorization", "superseded":
			code = fault.ExecutionAuthorization
		}
		return result, fault.New(code, result.Reason, nil)
	}
	return result, nil
}
