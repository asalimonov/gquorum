package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const credentialHeader = "x-gquorum-example-credential"

type loadErrorDetails struct {
	Schema int                       `json:"schema"`
	Safety *membership.SafetyRefusal `json:"safety,omitempty"`
}

type loadClient struct {
	address string
	member  uint64
	conn    *grpc.ClientConn
	client  loadpb.LoadClient
}

func (a *application) header() *loadpb.Header {
	id := a.currentNode().Identity()
	return &loadpb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, InstanceId: id.InstanceID, MemberId: id.MemberID}
}
func (a *application) client(ctx context.Context, name string, member uint64) (*loadClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	address, err := a.loadAddress(ctx, name, member)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old := a.clients[name]; old != nil {
		if old.address == address {
			if member != 0 {
				old.member = member
			}
			return old, nil
		}
		_ = old.conn.Close()
	}
	if a.clients == nil {
		return nil, fault.ErrStopped
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		return nil, fault.New(fault.Unavailable, "open example load connection", err)
	}
	client := &loadClient{address: address, member: member, conn: conn, client: loadpb.NewLoadClient(conn)}
	a.clients[name] = client
	return client, nil
}

func (a *application) auth(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(credentialHeader)
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(a.cfg.Credential)) != 1 {
		return fault.New(fault.IdentityMismatch, "invalid example credential", nil)
	}
	return nil
}
func (a *application) validateHeader(ctx context.Context, h *loadpb.Header) error {
	if h == nil || h.ClusterId != a.clusterID() || h.NodeName == "" || h.InstanceId == "" || h.MemberId == 0 {
		return fault.ErrIdentityMismatch
	}
	target, err := a.currentCluster().Target(ctx)
	if err != nil {
		return err
	}
	if current := target.MemberID(h.NodeName); current > 0 {
		if current == h.MemberId {
			return nil
		}
		return fault.ErrIdentityMismatch
	}
	if previous, err := a.approvedRoute(ctx, func(doc topology.Document) bool {
		return doc.MemberID(h.NodeName) != 0
	}); err == nil {
		if previous.MemberID(h.NodeName) == h.MemberId {
			return nil
		}
		return fault.ErrIdentityMismatch
	}
	if target.Version == 0 {
		for _, p := range a.cfg.Peers {
			if p.Name == h.NodeName && p.MemberID == h.MemberId {
				return nil
			}
		}
	}
	return fault.ErrIdentityMismatch
}
func domainReply(err error) *loadpb.Reply {
	code := fault.Unavailable
	var typed *fault.Error
	if errors.As(err, &typed) {
		code = typed.Code
	} else {
		for _, candidate := range []fault.Code{fault.IdentityMismatch, fault.UnsafeMembership, fault.OperationAuthorization, fault.ReplicaNotReady, fault.NotLeader, fault.Invalid, fault.Stopped, fault.GroupRetired} {
			if errors.Is(err, fault.New(candidate, "", nil)) {
				code = candidate
				break
			}
		}
	}
	reply := &loadpb.Reply{ErrorCode: string(code), Message: err.Error()}
	var safety *membership.SafetyRefusal
	if code == fault.UnsafeMembership && errors.As(err, &safety) {
		// Keep the structured refusal across both remote and loopback calls.
		// The operator can then retry transient quorum/health loss while still
		// terminating permanent membership refusals. Error text is not a parser.
		reply.Data, _ = json.Marshal(loadErrorDetails{Schema: 1, Safety: safety})
	}
	return reply
}
func replyError(reply *loadpb.Reply) error {
	if reply == nil {
		return fault.ErrUnavailable
	}
	if reply.ErrorCode != "" {
		if reply.ErrorCode == string(fault.UnsafeMembership) && len(reply.Data) != 0 {
			var details loadErrorDetails
			if json.Unmarshal(reply.Data, &details) == nil && details.Schema == 1 && details.Safety != nil && details.Safety.GroupID != "" && details.Safety.ReplicaID != 0 && details.Safety.Reason != "" {
				return fault.New(fault.UnsafeMembership, reply.Message, details.Safety)
			}
		}
		return fault.New(fault.Code(reply.ErrorCode), reply.Message, nil)
	}
	return nil
}
func dataReply(data any, replica uint64) (*loadpb.Reply, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return domainReply(err), nil
	}
	return &loadpb.Reply{Data: encoded, ReplicaId: replica}, nil
}
func (a *application) callNode(ctx context.Context, name, method, group string, replica uint64, data any) (*loadpb.Reply, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	request := &loadpb.Request{Header: a.header(), Method: method, GroupId: group, ReplicaId: replica, Data: encoded}
	var reply *loadpb.Reply
	if name == a.cfg.NodeName {
		reply, err = a.receiveCall(ctx, request)
	} else {
		var member uint64
		switch payload := data.(type) {
		case load.ActionRequest:
			member = payload.To.Node.MemberID
		case load.ReplicaQuery:
			member = payload.To.Node.MemberID
		case load.FenceRequest:
			member = payload.To.MemberID
		}
		client, openErr := a.client(ctx, name, member)
		if openErr != nil {
			return nil, openErr
		}
		ctx = metadata.AppendToOutgoingContext(ctx, credentialHeader, a.cfg.Credential)
		reply, err = client.client.Call(ctx, request)
	}
	if err != nil {
		return nil, fault.New(fault.Unavailable, "example load RPC unavailable", err)
	}
	return reply, replyError(reply)
}
func (a *application) Call(ctx context.Context, request *loadpb.Request) (*loadpb.Reply, error) {
	if err := a.auth(ctx); err != nil {
		return domainReply(err), nil
	}
	return a.receiveCall(ctx, request)
}
func headerIdentity(h *loadpb.Header) topology.Identity {
	if h == nil {
		return topology.Identity{}
	}
	return topology.Identity{ClusterID: h.ClusterId, NodeName: h.NodeName, InstanceID: h.InstanceId, MemberID: h.MemberId}
}
func (a *application) receiveCall(ctx context.Context, request *loadpb.Request) (*loadpb.Reply, error) {
	if request == nil {
		return domainReply(fault.ErrInvalid), nil
	}
	owned, done, err := a.beginEffect(ctx)
	if err != nil {
		return domainReply(err), nil
	}
	defer done()
	ctx = owned
	if err := topology.ValidateLoadID(request.GroupId); err != nil {
		return domainReply(err), nil
	}
	switch request.Method {
	case "Fence":
		var fence load.FenceRequest
		if err := decodeJSON(request.Data, &fence); err != nil {
			return domainReply(err), nil
		}
		if headerIdentity(request.Header) != fence.From || fence.Resource.GroupID != request.GroupId || fence.ReplicaID != request.ReplicaId {
			return domainReply(fault.ErrIdentityMismatch), nil
		}
		if err := a.waitAuthorityFence(ctx, fence, false); err != nil {
			return domainReply(err), nil
		}
		result, err := a.currentNode().InstallLoadFence(ctx, fence)
		if err != nil {
			return domainReply(err), nil
		}
		a.captureAuthorityFence(result)
		if err := a.waitAuthorityFence(ctx, fence, true); err != nil {
			return domainReply(err), nil
		}
		return dataReply(result, request.ReplicaId)
	case "ReadReplica":
		var query load.ReplicaQuery
		if err := decodeJSON(request.Data, &query); err != nil {
			return domainReply(err), nil
		}
		if headerIdentity(request.Header) != query.From || query.To.Replica.GroupID != request.GroupId || query.To.Replica.ReplicaID != request.ReplicaId {
			return domainReply(fault.ErrIdentityMismatch), nil
		}
		result, err := a.currentNode().ReadLoadReplica(ctx, query)
		if err != nil {
			return domainReply(err), nil
		}
		return dataReply(result, request.ReplicaId)
	case "Action":
		var action load.ActionRequest
		if err := decodeJSON(request.Data, &action); err != nil {
			return domainReply(err), nil
		}
		if headerIdentity(request.Header) != action.From || action.To.Replica.GroupID != request.GroupId || action.To.Replica.ReplicaID != request.ReplicaId {
			return domainReply(fault.ErrIdentityMismatch), nil
		}
		a.captureAuthorityAction(action)
		// Fault injection delays/refuses delivery. Node still validates the complete
		// authorization immediately before performing any local effect.
		if err := a.beforeClusterEffect(ctx, action.Authorization); err != nil {
			return domainReply(err), nil
		}
		result, err := a.currentNode().ApplyLoadAction(ctx, action)
		a.afterClusterEffect(action.Authorization, err)
		if err == nil {
			a.captureAuthorityReceipt(result)
			err = a.waitAfterClusterEffect(ctx, action.Authorization)
		}
		if err != nil {
			return domainReply(err), nil
		}
		return dataReply(result, request.ReplicaId)
	}
	if err := a.validateHeader(ctx, request.Header); err != nil {
		return domainReply(err), nil
	}
	g, found := a.currentNode().LoadGroup(request.GroupId)
	if !found || !g.Running() {
		return domainReply(fault.ErrReplicaNotReady), nil
	}
	if request.ReplicaId != g.ReplicaID() {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	switch request.Method {
	case "Status":
		return dataReply(g.Status(), g.ReplicaID())
	case "Put":
		if _, err := a.localServing(ctx, request.GroupId); err != nil {
			return domainReply(err), nil
		}
		if !g.IsLeader() {
			return domainReply(fault.ErrNotLeader), nil
		}
		var put putCommand
		if err := decodeJSON(request.Data, &put); err != nil {
			return domainReply(err), nil
		}
		result, err := g.Propose(ctx, load.Proposal{RequestID: put.RequestID, Data: request.Data})
		if err != nil {
			return domainReply(err), nil
		}
		if result.Result.Code != "ok" {
			return domainReply(fault.New(fault.Code(result.Result.Code), "load proposal refused", nil)), nil
		}
		return dataReply(map[string]any{"request_id": put.RequestID, "key": put.Key, "value": put.Value, "commit": result.Index}, g.ReplicaID())
	case "Snapshot":
		if err := g.Snapshot(ctx); err != nil {
			return domainReply(err), nil
		}
		return &loadpb.Reply{ReplicaId: g.ReplicaID()}, nil
	default:
		return domainReply(fault.ErrUnsupported), nil
	}
}
func (a *application) beginEffect(parent context.Context) (context.Context, func(), error) {
	ctx, processDone, err := a.beginProcessEffect(parent)
	if err != nil {
		return nil, nil, err
	}
	a.generationMu.Lock()
	g := a.generation
	if g == nil {
		a.generationMu.Unlock()
		return ctx, processDone, nil
	}
	if !g.accepting {
		a.generationMu.Unlock()
		processDone()
		return nil, nil, fault.ErrStopped
	}
	g.effects.Add(1)
	a.generationMu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	joined := make(chan struct{})
	stop := context.AfterFunc(g.ctx, func() { cancel(); close(joined) })
	return ctx, func() {
		cancel()
		if !stop() {
			<-joined
		}
		g.effects.Done()
		processDone()
	}, nil
}

func (a *application) beginProcessEffect(parent context.Context) (context.Context, func(), error) {
	a.effectMu.Lock()
	select {
	case <-a.closing:
		a.effectMu.Unlock()
		return nil, nil, fault.ErrStopped
	default:
	}
	a.effectWG.Add(1)
	a.effectMu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	joined := make(chan struct{})
	stop := context.AfterFunc(a.workCtx, func() { cancel(); close(joined) })
	return ctx, func() {
		cancel()
		if !stop() {
			<-joined
		}
		a.effectWG.Done()
	}, nil
}

func (a *application) Deliver(ctx context.Context, frame *loadpb.Frame) (*loadpb.Reply, error) {
	owned, done, err := a.beginEffect(ctx)
	if err != nil {
		return domainReply(err), nil
	}
	defer done()
	ctx = owned

	if err := a.auth(ctx); err != nil {
		return domainReply(err), nil
	}
	if frame == nil {
		return domainReply(fault.ErrInvalid), nil
	}
	var batch load.RaftBatch
	if err := decodeJSON(frame.RaftMessage, &batch); err != nil {
		return domainReply(err), nil
	}
	if headerIdentity(frame.Header) != batch.From.Node || frame.GroupId != batch.To.Replica.GroupID || frame.ToReplicaId != batch.To.Replica.ReplicaID {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	if len(batch.Messages) == 0 {
		return domainReply(fault.ErrInvalid), nil
	}
	if batch.To.Node != a.currentNode().Identity() {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	batch, err = a.filterReadRaft(batch)
	if err != nil {
		return domainReply(err), nil
	}
	if len(batch.Messages) == 0 {
		return &loadpb.Reply{}, nil // authenticated fault drop; no effect acknowledgement
	}
	if err := a.currentNode().ReceiveLoadRaft(ctx, batch); err != nil {
		return domainReply(err), nil
	}
	a.recordReadReceive(batch)
	for _, message := range batch.Messages {
		if message.Type == raftpb.MsgSnap {
			a.receivedClusterSnapshot(frame.GroupId)
		}
	}
	return &loadpb.Reply{}, nil
}

type loadTransport struct {
	app     *application
	binding *fixtureLoadBinding
}

func (t loadTransport) SendRaft(ctx context.Context, batch load.RaftBatch) error {
	t.app.recordReadSend(batch)
	if t.binding != nil {
		return t.binding.transport.SendRaft(ctx, batch)
	}
	client, err := t.app.client(ctx, batch.To.Node.NodeName, batch.To.Node.MemberID)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	from := batch.From.Node
	header := &loadpb.Header{ClusterId: from.ClusterID, NodeName: from.NodeName, InstanceId: from.InstanceID, MemberId: from.MemberID}
	rpcCtx := metadata.AppendToOutgoingContext(ctx, credentialHeader, t.app.cfg.Credential)
	reply, err := client.client.Deliver(rpcCtx, &loadpb.Frame{Header: header, GroupId: batch.To.Replica.GroupID, ToReplicaId: batch.To.Replica.ReplicaID, RaftMessage: encoded})
	if err != nil {
		return err
	}
	if err := replyError(reply); err != nil {
		return err
	}
	for _, message := range batch.Messages {
		if message.Type == raftpb.MsgSnap {
			return t.app.currentNode().ReportLoadDelivery(ctx, load.DeliveryReport{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID, Kind: load.DeliverySnapshotSucceeded})
		}
	}
	return nil
}
func (t loadTransport) ReadReplica(ctx context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	var result load.ReplicaStatus
	reply, err := t.app.callNode(ctx, query.To.Node.NodeName, "ReadReplica", query.To.Replica.GroupID, query.To.Replica.ReplicaID, query)
	if err == nil {
		err = decodeJSON(reply.Data, &result)
	}
	return result, err
}
func (t loadTransport) ApplyAction(ctx context.Context, action load.ActionRequest) (load.ActionResult, error) {
	var result load.ActionResult
	reply, err := t.app.callNode(ctx, action.To.Node.NodeName, "Action", action.To.Replica.GroupID, action.To.Replica.ReplicaID, action)
	if err == nil {
		err = decodeJSON(reply.Data, &result)
	}
	return result, err
}

func (t loadTransport) InstallFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	var result load.FenceResult
	reply, err := t.app.callNode(ctx, request.To.NodeName, "Fence", request.Resource.GroupID, request.ReplicaID, request)
	if err == nil {
		err = decodeJSON(reply.Data, &result)
	}
	return result, err
}
