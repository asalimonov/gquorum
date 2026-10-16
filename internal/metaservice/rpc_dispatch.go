package metaservice

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type metaCall struct {
	Method   string
	Header   *metapb.Header
	Data     []byte
	Identity topology.Identity
}

func retainedProtocolMethod(method string) bool {
	switch method {
	case "IdentityPing", "MetaStatus", "ParkedRead", "InstallFence", "MetaControl":
		return true
	default:
		return false
	}
}

// readProtocolCall runs before active-member authorization, preserving admission and retained authority reads.
func (g *Group) readProtocolCall(ctx context.Context, call metaCall) *metapb.Reply {
	switch call.Method {
	case "Join":
		return g.handleJoin(ctx, call.Header, call.Data)
	case "IdentityPing":
		if len(call.Data) > 0 {
			return g.authorityIdentityReply(ctx, call.Header, call.Data)
		}
		return g.identityPingReply(call.Identity)
	case "ParkedRead":
		return g.parkedReadReply()
	case "MetaStatus":
		return g.metaStatusReply(ctx)
	case "AuthorityRead":
		return g.authorityReadReply(ctx, call.Header, call.Data)
	default:
		return nil
	}
}

func (g *Group) controlProtocolCall(ctx context.Context, call metaCall) *metapb.Reply {
	switch call.Method {
	case "InstallFence":
		return g.installFenceReply(ctx, call.Header, call.Data)
	case "MetaControl":
		return g.metaControlReply(ctx, call.Header, call.Data)
	case "MetaReplicaMetrics":
		if len(call.Data) != 0 {
			return metatransport.Reply(fault.ErrInvalid)
		}
		return g.localReplicaMetrics(ctx, call.Header)
	default:
		return nil
	}
}

func (g *Group) leaderProtocolCall(ctx context.Context, call metaCall) *metapb.Reply {
	switch call.Method {
	case "JoinReady":
		return g.handleReady(ctx, call.Header, call.Data)
	case "ApplyVersion":
		return g.applyVersionReply(ctx, call.Data)
	case "ProposeMeta":
		return g.proposeMetaReply(ctx, call.Header, call.Data)
	case "KeepAlive":
		return g.keepAliveReply(call.Header, call.Data)
	default:
		return metatransport.Reply(fault.ErrUnsupported)
	}
}

func (g *Group) identityPingReply(identity topology.Identity) *metapb.Reply {
	if engine := g.engine.Load(); engine == nil || !engine.Running() {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	// The caller verifies the returned exact process identity before
	// revoking a lease.
	encoded, err := json.Marshal(identity)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) parkedReadReply() *metapb.Reply {
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	encoded, err := json.Marshal(g.parked.snapshot())
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) metaStatusReply(ctx context.Context) *metapb.Reply {
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	engine := g.engine.Load()
	if engine == nil {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	status, err := (groupControllers{g}).Status(ctx, topology.MetaGroupID)
	if err != nil {
		return g.leaderReply(err)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded, LeaderId: engine.RaftStatus().Lead}
}

func (g *Group) installFenceReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	var request load.FenceRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return metatransport.Reply(err)
	}
	if request.From != (topology.Identity{ClusterID: header.ClusterId, NodeName: header.NodeName,
		MemberID:   header.MemberId,
		InstanceID: header.InstanceId}) {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	result, err := g.InstallFence(ctx, request)
	if err != nil {
		return metatransport.Reply(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) metaControlReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	var envelope struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if envelope.Kind != "" {
		return g.cleanupControlReply(ctx, header, data, envelope.Kind)
	}
	var request load.ActionRequest
	if json.Unmarshal(data, &request) != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if request.From != (topology.Identity{ClusterID: header.ClusterId, NodeName: header.NodeName,
		MemberID:   header.MemberId,
		InstanceID: header.InstanceId}) {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	result, err := g.ApplyMetaAction(ctx, request)
	if err != nil {
		return g.leaderReply(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) applyVersionReply(ctx context.Context, data []byte) *metapb.Reply {
	request, err := decodeApplyVersionRequest(data)
	if err != nil {
		return metatransport.Reply(err)
	}
	var id topology.OperationID
	if request.Kind == "" { // Schema3 callers are removed with their persistence at Task78.
		var legacy topology.OperatorID
		legacy, err = g.applyLocal(ctx, request.Version)
		id = topology.OperationID(legacy)
	} else {
		id, err = g.applyOperationLocal(ctx, request)
	}
	if err != nil {
		return g.leaderReply(err)
	}
	encoded, _ := json.Marshal(id)
	return &metapb.Reply{Data: encoded}
}

func (g *Group) proposeMetaReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	proposal, err := metaengine.DecodeProposal(data)
	if err != nil {
		return metatransport.Reply(fault.New(fault.Invalid, "invalid meta command envelope", err))
	}
	if err := g.validateProposalCourier(proposal, header); err != nil {
		return metatransport.Reply(err)
	}
	engine := g.engine.Load()
	result, err := engine.Propose(ctx, raftengine.Proposal{Data: data})
	if err != nil {
		return g.leaderReply(err)
	}
	return &metapb.Reply{Data: result.Data}
}

func (g *Group) keepAliveReply(header *metapb.Header, data []byte) *metapb.Reply {
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
	if !ok || session.NodeName != header.NodeName || session.InstanceID != request.InstanceID ||
		session.InstanceID != header.InstanceId || g.eligibleMemberID(header.NodeName) != header.MemberId {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	now := time.Now()
	g.leases.refreshTTL(request.SessionID, now, time.Duration(session.TTLSeconds)*time.Second)
	encoded, _ := json.Marshal(struct {
		ExpiresIn int `json:"expires_in"`
	}{g.leases.remaining(request.SessionID, now)})
	return &metapb.Reply{Data: encoded}
}

func (g *Group) validateProposalCourier(proposal any, header *metapb.Header) error {
	switch p := proposal.(type) {
	case *metaengine.RecordCollectionProposal, *metaengine.RecordCleanupStatusProposal:
		return fault.ErrOperationAuthorization // Restricted reports enter through MetaControl only.
	case *metaengine.BindAdmissionProposal:
		if p.Admission.NodeName != header.NodeName || p.Admission.MemberID != header.MemberId {
			return fault.ErrIdentityMismatch
		}
	case *metaengine.GrantSession:
		return g.validateSessionCourier(p, header)
	case *metaengine.AcquireLock:
		session, ok := g.state().Session(p.SessionID)
		if !ok || session.NodeName != header.NodeName || session.InstanceID != header.InstanceId ||
			p.Key != "node/"+header.NodeName && strings.HasPrefix(p.Key, "node/") {
			return fault.ErrIdentityMismatch
		}
	case *metaengine.RevokeSession:
		session, ok := g.state().Session(p.SessionID)
		if ok && (session.NodeName != header.NodeName || session.InstanceID != header.InstanceId) {
			return fault.ErrIdentityMismatch
		}
	default: // Other proposals use their own replicated validation.
	}
	return nil
}

func (g *Group) validateSessionCourier(proposal *metaengine.GrantSession, header *metapb.Header) error {
	machine := g.state()
	identity := topology.Identity{ClusterID: machine.Target().ClusterID, NodeName: proposal.NodeName,
		MemberID: header.MemberId, InstanceID: proposal.InstanceID}
	if proposal.NodeName != header.NodeName || proposal.InstanceID == "" ||
		proposal.InstanceID != header.InstanceId || machine.EligibleMemberID(proposal.NodeName) != header.MemberId {
		return fault.ErrIdentityMismatch
	}
	_, err := sessionAdmissionBasis(machine, identity)
	return err
}
