package metatransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	Header                                               *metapb.Header
	Listener                                             net.Listener
	TLS, ClientTLS                                       *tls.Config
	AllowPlaintext                                       bool
	Credential                                           []byte
	Handler                                              Handler
	Resolve                                              func(context.Context, uint64) (string, error)
	SendTimeout, SnapshotTimeout, RetryInitial, RetryMax time.Duration
	MaxSnapshotBytes                                     int
	OnUnreachable                                        func(uint64)
	OnSnapshot                                           func(uint64, raft.SnapshotStatus)
}
type Transport struct {
	opts             Options
	mu               sync.Mutex
	ctx              context.Context
	cancel           context.CancelFunc
	server           *grpc.Server
	peers            map[uint64]*peer
	started, stopped bool
	wg               sync.WaitGroup
	done             chan struct{}
	serveErr         error
}
type peer struct {
	id     uint64
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan raftpb.Message
	retire chan struct{}
	until  atomic.Int64
}

func New(o Options) (*Transport, error) {
	if o.Header == nil || o.Header.ClusterId == "" || o.Header.NodeName == "" || o.Listener == nil || o.Handler == nil || o.Resolve == nil || len(o.Credential) == 0 {
		return nil, fault.New(fault.Invalid, "meta transport requires identity, listener, handler, resolver and credential", nil)
	}
	if o.TLS == nil && !o.AllowPlaintext || o.ClientTLS == nil && !o.AllowPlaintext {
		return nil, fault.New(fault.Invalid, "plaintext transport must be explicit", nil)
	}
	if o.SendTimeout <= 0 || o.SnapshotTimeout <= 0 || o.RetryInitial <= 0 || o.RetryMax < o.RetryInitial || o.MaxSnapshotBytes <= 0 {
		return nil, fault.ErrInvalid
	}
	o.Credential = bytes.Clone(o.Credential)
	o.Header = proto.Clone(o.Header).(*metapb.Header)
	if o.TLS != nil {
		o.TLS = o.TLS.Clone()
	}
	if o.ClientTLS != nil {
		o.ClientTLS = o.ClientTLS.Clone()
	}
	options := []grpc.ServerOption{grpc.MaxRecvMsgSize(snapshotChunkBytes * 2)}
	if o.TLS != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(o.TLS)))
	}
	srv := grpc.NewServer(options...)
	metapb.RegisterMetaServer(srv, &server{handler: o.Handler, clusterID: o.Header.ClusterId, credential: o.Credential, maxSnapshot: o.MaxSnapshotBytes})
	return &Transport{opts: o, server: srv, peers: make(map[uint64]*peer), done: make(chan struct{})}, nil
}
func (t *Transport) Start(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started || t.stopped {
		return fault.ErrStopped
	}
	t.started = true
	t.ctx, t.cancel = context.WithCancel(parent)
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		err := t.server.Serve(t.opts.Listener)
		t.mu.Lock()
		if !t.stopped {
			t.serveErr = err
		}
		t.mu.Unlock()
	}()
	return nil
}
func (t *Transport) Stop(ctx context.Context) error {
	t.mu.Lock()
	if !t.stopped {
		t.stopped = true
		if t.cancel != nil {
			t.cancel()
		}
		t.server.Stop()
		_ = t.opts.Listener.Close()
		// All Add calls hold this mutex; no worker can be added after Wait starts.
		go func() { t.wg.Wait(); close(t.done) }()
	}
	t.mu.Unlock()
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "meta network workers have not stopped", ctx.Err())
	}
}
func (t *Transport) Error() error { t.mu.Lock(); defer t.mu.Unlock(); return t.serveErr }
func (t *Transport) SetMemberID(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.opts.Header.MemberId = id
}
func (t *Transport) Header() *metapb.Header {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.opts.Header
	return proto.Clone(h).(*metapb.Header)
}

// Prune retires removed peers after their already admitted messages drain. It
// must not cancel the final commit carrying a removal. Stop still cancels and
// joins every retiring worker immediately; this never changes Raft membership.
func (t *Transport) Prune(ids map[uint64]bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, p := range t.peers {
		if !ids[id] {
			close(p.retire)
			delete(t.peers, id)
		}
	}
}
func (t *Transport) Send(ctx context.Context, messages []raftpb.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started || t.stopped {
		return fault.ErrStopped
	}
	for _, msg := range messages {
		if msg.From != t.opts.Header.MemberId || msg.To == 0 {
			return fault.ErrIdentityMismatch
		}
		p := t.peers[msg.To]
		if p == nil {
			peerCtx, cancel := context.WithCancel(t.ctx)
			p = &peer{id: msg.To, ctx: peerCtx, cancel: cancel, queue: make(chan raftpb.Message, 128), retire: make(chan struct{})}
			t.peers[msg.To] = p
			t.wg.Add(1)
			go t.runPeer(p)
		}
		// The engine has already transferred ownership of these messages to Send.
		if time.Now().UnixNano() < p.until.Load() {
			t.fail(msg)
			continue
		}
		select {
		case p.queue <- msg:
		default:
			t.fail(msg)
		}
	}
	return nil
}
func (t *Transport) fail(msg raftpb.Message) {
	if t.opts.OnUnreachable != nil {
		t.opts.OnUnreachable(msg.To)
	}
	if msg.Type == raftpb.MsgSnap && t.opts.OnSnapshot != nil {
		t.opts.OnSnapshot(msg.To, raft.SnapshotFailure)
	}
}
func (t *Transport) connection(address string) (*grpc.ClientConn, error) {
	var credential credentials.TransportCredentials
	if t.opts.ClientTLS != nil {
		credential = credentials.NewTLS(t.opts.ClientTLS.Clone())
	} else {
		credential = insecure.NewCredentials()
	}
	return grpc.NewClient(address, grpc.WithTransportCredentials(credential), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
}
func (t *Transport) authenticated(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, credentialKey, hex.EncodeToString(t.opts.Credential))
}
func (t *Transport) runPeer(p *peer) {
	defer t.wg.Done()
	defer p.cancel()
	var conn *grpc.ClientConn
	var address string
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
		for {
			select {
			case msg := <-p.queue:
				t.fail(msg)
			default:
				return
			}
		}
	}()
	backoff := t.opts.RetryInitial
	retire := p.retire
	retiring := false
	for {
		if retiring && len(p.queue) == 0 {
			return
		}
		select {
		case <-p.ctx.Done():
			return
		case <-retire:
			retiring, retire = true, nil
			continue
		case msg := <-p.queue:
			timeout := t.opts.SendTimeout
			if msg.Type == raftpb.MsgSnap {
				timeout = t.opts.SnapshotTimeout
			}
			ctx, cancel := context.WithTimeout(p.ctx, timeout)
			resolved, err := t.opts.Resolve(ctx, p.id)
			if err == nil && (conn == nil || address != resolved) {
				if conn != nil {
					_ = conn.Close()
				}
				conn, err = t.connection(resolved)
				address = resolved
			}
			if err == nil {
				err = t.sendMessage(t.authenticated(ctx), metapb.NewMetaClient(conn), msg)
			}
			cancel()
			if err == nil {
				backoff = t.opts.RetryInitial
				if msg.Type == raftpb.MsgSnap && t.opts.OnSnapshot != nil {
					t.opts.OnSnapshot(msg.To, raft.SnapshotFinish)
				}
				continue
			}
			jitterLimit := min(backoff/4, t.opts.RetryMax-backoff)
			delay := backoff + time.Duration(rand.Int64N(max(1, int64(jitterLimit))))
			p.until.Store(time.Now().Add(delay).UnixNano())
			t.fail(msg)
			// Failed and buffered messages are discarded; a later Raft tick sends fresh traffic.
			for {
				select {
				case queued := <-p.queue:
					t.fail(queued)
				default:
					goto drained
				}
			}
		drained:
			timer := time.NewTimer(delay)
			select {
			case <-p.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff >= t.opts.RetryMax/2 {
				backoff = t.opts.RetryMax
			} else {
				backoff *= 2
			}
		}
	}
}
func (t *Transport) sendMessage(ctx context.Context, client metapb.MetaClient, msg raftpb.Message) error {
	h := t.Header()
	if msg.Type != raftpb.MsgSnap {
		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		stream, err := client.StreamMessages(ctx)
		if err != nil {
			return err
		}
		if err = stream.Send(&metapb.RaftMessage{Header: h, ToMemberId: msg.To, RaftMessage: data}); err != nil {
			return err
		}
		reply, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		return ReplyError(reply)
	}
	if msg.Snapshot == nil || len(msg.Snapshot.Data) == 0 || len(msg.Snapshot.Data) > t.opts.MaxSnapshotBytes {
		return fault.ErrInvalid
	}
	snapshot := *msg.Snapshot
	data := snapshot.Data
	snapshot.Data = nil
	msg.Snapshot = &snapshot
	encoded, err := msg.Marshal()
	if err != nil {
		return err
	}
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return err
	}
	for offset := 0; offset < len(data); {
		end := min(offset+snapshotChunkBytes, len(data))
		chunk := &metapb.SnapshotChunk{Header: h, ToMemberId: msg.To, Data: data[offset:end], Final: end == len(data)}
		if offset == 0 {
			chunk.RaftMessage = encoded
		}
		if err = stream.Send(chunk); err != nil {
			return err
		}
		offset = end
	}
	reply, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	return ReplyError(reply)
}

// CallAddress invokes a control RPC with a caller-owned deadline. The temporary
// connection is always closed, including cancellation and authentication errors.
func (t *Transport) CallAddress(ctx context.Context, address, method string, data []byte) (*metapb.Reply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	running := t.started && !t.stopped
	parent := t.ctx
	if running {
		t.wg.Add(1)
	}
	t.mu.Unlock()
	if !running {
		return nil, fault.ErrStopped
	}
	defer t.wg.Done()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(parent, cancel)
	defer stop()
	conn, err := t.connection(address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := metapb.NewMetaClient(conn)
	req := &metapb.MetaRequest{Header: t.Header(), Data: bytes.Clone(data)}
	callCtx = t.authenticated(callCtx)
	switch method {
	case "Join":
		return client.Join(callCtx, req)
	case "JoinReady":
		return client.JoinReady(callCtx, req)
	case "KeepAlive":
		return client.KeepAlive(callCtx, req)
	case "IdentityPing":
		return client.IdentityPing(callCtx, req)
	case "MetaStatus":
		return client.MetaStatus(callCtx, req)
	case "MetaControl":
		return client.MetaControl(callCtx, req)
	case "ProposeMeta":
		return client.ProposeMeta(callCtx, req)
	case "ApplyVersion":
		return client.ApplyVersion(callCtx, req)
	case "ParkedRead":
		return client.ParkedRead(callCtx, req)
	default:
		return nil, fmt.Errorf("%w: unknown meta RPC", fault.ErrInvalid)
	}
}

func (t *Transport) SetPeerAddresses(peers []*metapb.PeerAddress) {
	t.mu.Lock()
	defer t.mu.Unlock()
	header := proto.Clone(t.opts.Header).(*metapb.Header)
	header.Peers = nil
	for _, p := range peers {
		header.Peers = append(header.Peers, proto.Clone(p).(*metapb.PeerAddress))
	}
	t.opts.Header = header
}
