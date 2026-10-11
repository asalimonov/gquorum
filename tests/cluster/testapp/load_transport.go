package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const credentialHeader = "x-gquorum-example-credential"

type loadClient struct {
	address string
	conn    *grpc.ClientConn
	client  loadpb.LoadClient
}

func (a *application) header() *loadpb.Header {
	id := a.meta.Identity()
	return &loadpb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, InstanceId: id.InstanceID, MemberId: id.MemberID}
}
func (a *application) client(ctx context.Context, name string) (*loadClient, error) {
	address, err := a.address(ctx, name, loadAddressLabel)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old := a.clients[name]; old != nil {
		if old.address == address {
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
	client := &loadClient{address: address, conn: conn, client: loadpb.NewLoadClient(conn)}
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
	if h == nil || h.ClusterId != a.cfg.ClusterID || h.NodeName == "" || h.InstanceId == "" || h.MemberId == 0 {
		return fault.ErrIdentityMismatch
	}
	target, active, err := a.documents(ctx)
	if err != nil {
		return err
	}
	if current := target.MemberID(h.NodeName); current > 0 {
		if current == h.MemberId {
			return nil
		}
		return fault.ErrIdentityMismatch
	}
	if current := active.MemberID(h.NodeName); current > 0 {
		if current == h.MemberId {
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
		for _, candidate := range []fault.Code{fault.IdentityMismatch, fault.UnsafeMembership, fault.OperatorAuthorization, fault.ReplicaNotReady, fault.NotLeader, fault.Invalid, fault.Stopped, fault.GroupRetired} {
			if errors.Is(err, fault.New(candidate, "", nil)) {
				code = candidate
				break
			}
		}
	}
	return &loadpb.Reply{ErrorCode: string(code), Message: err.Error()}
}
func replyError(reply *loadpb.Reply) error {
	if reply == nil {
		return fault.ErrUnavailable
	}
	if reply.ErrorCode != "" {
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
		client, openErr := a.client(ctx, name)
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
func (a *application) receiveCall(ctx context.Context, request *loadpb.Request) (*loadpb.Reply, error) {
	if request == nil {
		return domainReply(fault.ErrInvalid), nil
	}
	if err := topology.ValidateLoadID(request.GroupId); err != nil {
		return domainReply(err), nil
	}
	if request.Method == "Effect" {
		owned, done, err := a.beginEffect(ctx)
		if err != nil {
			return domainReply(err), nil
		}
		defer done()
		ctx = owned
		var approved topology.AuthorizedStep
		if err := decodeJSON(request.Data, &approved); err != nil {
			return domainReply(err), nil
		}
		// Check required version first so a lagging receiver reports retryable
		// unavailability instead of confusing a fresh member with a foreign one.
		if err := a.meta.Authorize(ctx, approved); err != nil {
			return domainReply(err), nil
		}
		if err := a.validateHeader(ctx, request.Header); err != nil {
			return domainReply(err), nil
		}
		if approved.GroupID != request.GroupId {
			return domainReply(fault.ErrOperatorAuthorization), nil
		}
		if err := a.receiveEffect(ctx, approved, request.ReplicaId); err != nil {
			return domainReply(err), nil
		}
		return &loadpb.Reply{}, nil
	}
	if err := a.validateHeader(ctx, request.Header); err != nil {
		return domainReply(err), nil
	}
	g, found := a.runtime.Load(request.GroupId)
	if request.Method == "Status" {
		if !found {
			return dataReply(topology.GroupStatus{GroupID: request.GroupId, HostNode: a.cfg.NodeName}, 0)
		}
		return dataReply(g.Status(), g.ReplicaID())
	}
	if !found || !g.Running() {
		return domainReply(fault.ErrReplicaNotReady), nil
	}
	if request.ReplicaId != g.ReplicaID() {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	switch request.Method {
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
		if result.Code != "ok" {
			return domainReply(fault.New(fault.Code(result.Code), "load proposal refused", nil)), nil
		}
		return dataReply(map[string]any{"request_id": put.RequestID, "key": put.Key, "value": put.Value, "commit": g.Status().Commit}, g.ReplicaID())
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

func contains(cs raftpb.ConfState, id uint64) bool {
	for _, ids := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		for _, candidate := range ids {
			if candidate == id {
				return true
			}
		}
	}
	return false
}
func (a *application) Deliver(ctx context.Context, frame *loadpb.Frame) (*loadpb.Reply, error) {
	if err := a.auth(ctx); err != nil {
		return domainReply(err), nil
	}
	if frame == nil {
		return domainReply(fault.ErrInvalid), nil
	}
	if err := a.validateHeader(ctx, frame.Header); err != nil {
		return domainReply(err), nil
	}
	if err := topology.ValidateLoadID(frame.GroupId); err != nil {
		return domainReply(err), nil
	}
	g, found := a.runtime.Load(frame.GroupId)
	if !found || !g.Running() {
		return domainReply(fault.ErrReplicaNotReady), nil
	}
	var message raftpb.Message
	if err := message.Unmarshal(frame.RaftMessage); err != nil {
		return domainReply(fault.ErrInvalid), nil
	}
	if frame.ToReplicaId != g.ReplicaID() || message.To != g.ReplicaID() || message.From == 0 {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	owner, err := a.owner(ctx, frame.GroupId, message.From)
	if err != nil || owner != frame.Header.NodeName {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	cs := g.Status().ConfState
	if len(cs.Voters)+len(cs.VotersOutgoing)+len(cs.Learners)+len(cs.LearnersNext) > 0 && !contains(cs, message.From) {
		return domainReply(fault.ErrIdentityMismatch), nil
	}
	if err := g.Step(ctx, message); err != nil {
		return domainReply(err), nil
	}
	if message.Type == raftpb.MsgSnap {
		a.receivedClusterSnapshot(frame.GroupId)
	}
	return &loadpb.Reply{}, nil
}

type loadTransport struct {
	app     *application
	group   string
	replica uint64
}

func (t loadTransport) Send(ctx context.Context, messages []raftpb.Message) error {
	// Independent peers share a bounded batch pool, so a dead destination does
	// not prevent delivery to healthy voters. Each RPC honors the engine context.
	var wg sync.WaitGroup
	slots := make(chan struct{}, 8)
	for _, message := range messages {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			t.report(message, false)
			continue
		}
		wg.Go(func() {
			defer func() { <-slots }()
			name, err := t.app.owner(ctx, t.group, message.To)
			if err == nil {
				var client *loadClient
				client, err = t.app.client(ctx, name)
				if err == nil {
					var encoded []byte
					encoded, err = message.Marshal()
					if err == nil {
						var reply *loadpb.Reply
						rpcCtx := metadata.AppendToOutgoingContext(ctx, credentialHeader, t.app.cfg.Credential)
						reply, err = client.client.Deliver(rpcCtx, &loadpb.Frame{Header: t.app.header(), GroupId: t.group, ToReplicaId: message.To, RaftMessage: encoded})
						if err == nil {
							err = replyError(reply)
						}
					}
				}
			}
			t.report(message, err == nil)
		})
	}
	wg.Wait()
	return nil
}
func (t loadTransport) report(message raftpb.Message, success bool) {
	if group, found := t.app.runtime.Load(t.group); found && group.ReplicaID() == t.replica {
		if !success {
			group.ReportUnreachable(message.To)
		}
		if message.Type == raftpb.MsgSnap {
			state := raft.SnapshotFailure
			if success {
				state = raft.SnapshotFinish
			}
			group.ReportSnapshot(message.To, state)
		}
	}
}
