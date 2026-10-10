package metatransport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testHandler struct {
	received chan raftpb.Message
	block    bool
	calls    chan string
}

func (h *testHandler) Step(ctx context.Context, header *metapb.Header, msg raftpb.Message) error {
	if h.block {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case h.received <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *testHandler) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if h.calls != nil {
		select {
		case h.calls <- method:
		case <-ctx.Done():
			return Reply(ctx.Err())
		}
	}
	if h.block {
		<-ctx.Done()
		return Reply(ctx.Err())
	}
	return &metapb.Reply{Data: bytes.Clone(data)}
}
func testTransport(t *testing.T, id uint64, h *testHandler, resolve func(context.Context, uint64) (string, error), modify func(*Options)) *Transport {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Header: &metapb.Header{ClusterId: "cluster", NodeName: "node", MemberId: id}, Listener: listener, AllowPlaintext: true, Credential: []byte("test-cluster-secret"), Handler: h, Resolve: resolve, SendTimeout: 100 * time.Millisecond, SnapshotTimeout: time.Second, RetryInitial: 20 * time.Millisecond, RetryMax: 100 * time.Millisecond, MaxSnapshotBytes: 2 << 20}
	if modify != nil {
		modify(&opts)
	}
	transport, err := New(opts)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err = transport.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := transport.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return transport
}
func nowhere(context.Context, uint64) (string, error) { return "", fault.ErrUnavailable }
func recvMessage(t *testing.T, ch <-chan raftpb.Message) raftpb.Message {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("message not delivered")
		return raftpb.Message{}
	}
}
func TestMetaTransportMessagesAndChunkedSnapshotPreserveTerms(t *testing.T) {
	receiver := &testHandler{received: make(chan raftpb.Message, 8)}
	remote := testTransport(t, 2, receiver, nowhere, nil)
	reports := make(chan raft.SnapshotStatus, 8)
	local := testTransport(t, 1, &testHandler{received: make(chan raftpb.Message, 8)}, func(context.Context, uint64) (string, error) { return remote.opts.Listener.Addr().String(), nil }, func(o *Options) { o.OnSnapshot = func(_ uint64, s raft.SnapshotStatus) { reports <- s } })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := local.Send(ctx, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 7, Commit: 6}}); err != nil {
		t.Fatal(err)
	}
	if msg := recvMessage(t, receiver.received); msg.Term != 7 || msg.Commit != 6 {
		t.Fatal(msg)
	}
	data := bytes.Repeat([]byte("full machine state"), 40000)
	snapshot := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 99, Term: 5, ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
	if err := local.Send(ctx, []raftpb.Message{{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 8, Snapshot: &snapshot}}); err != nil {
		t.Fatal(err)
	}
	msg := recvMessage(t, receiver.received)
	if msg.Term != 8 || msg.Snapshot.Metadata.Term != 5 || msg.Snapshot.Metadata.Index != 99 || !bytes.Equal(msg.Snapshot.Data, data) || len(msg.Snapshot.Metadata.ConfState.Learners) != 1 {
		t.Fatal("snapshot framing changed state or terms")
	}
	select {
	case s := <-reports:
		if s != raft.SnapshotFinish {
			t.Fatal(s)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func TestMetaTransportCredentialAndClusterIsolation(t *testing.T) {
	receiver := &testHandler{received: make(chan raftpb.Message, 1)}
	remote := testTransport(t, 2, receiver, nowhere, nil)
	tests := []struct {
		name   string
		change func(*Options)
		grpc   codes.Code
		fault  error
	}{
		{name: "credential", change: func(o *Options) { o.Credential = []byte("different") }, grpc: codes.Unauthenticated},
		{name: "cluster", change: func(o *Options) { o.Header.ClusterId = "other" }, fault: fault.ErrIdentityMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			local := testTransport(t, 1, &testHandler{received: make(chan raftpb.Message, 1)}, nowhere, tc.change)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			reply, err := local.CallAddress(ctx, remote.opts.Listener.Addr().String(), "MetaStatus", nil)
			if tc.grpc != codes.OK {
				if status.Code(err) != tc.grpc {
					t.Fatal(err)
				}
			} else if !errors.Is(ReplyError(reply), tc.fault) {
				t.Fatal(reply, err)
			}
		})
	}
	select {
	case <-receiver.received:
		t.Fatal("unauthorized traffic reached handler")
	default:
	}
}
func TestMetaTransportPeerBackoffDoesNotBlockOtherPeer(t *testing.T) {
	blocked := testTransport(t, 2, &testHandler{block: true}, nowhere, nil)
	h := &testHandler{received: make(chan raftpb.Message, 32)}
	healthy := testTransport(t, 3, h, nowhere, nil)
	failures := make(chan uint64, 256)
	local := testTransport(t, 1, &testHandler{received: make(chan raftpb.Message, 1)}, func(_ context.Context, id uint64) (string, error) {
		if id == 2 {
			return blocked.opts.Listener.Addr().String(), nil
		}
		return healthy.opts.Listener.Addr().String(), nil
	}, func(o *Options) {
		o.OnUnreachable = func(id uint64) {
			select {
			case failures <- id:
			default:
			}
		}
	})
	for i := range 8 {
		if err := local.Send(context.Background(), []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: uint64(i + 1)}, {Type: raftpb.MsgHeartbeat, From: 1, To: 3, Term: uint64(i + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		msg := recvMessage(t, h.received)
		if msg.To != 3 {
			t.Fatal(msg)
		}
	}
	select {
	case id := <-failures:
		if id != 2 {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("failed peer was not reported")
	}
	local.Prune(map[uint64]bool{3: true})
	local.mu.Lock()
	_, present := local.peers[2]
	local.mu.Unlock()
	if present {
		t.Fatal("retired peer queue retained")
	}
}
func TestMetaTransportStopCancelsAndJoinsControlCall(t *testing.T) {
	h := &testHandler{block: true, calls: make(chan string, 1)}
	remote := testTransport(t, 2, h, nowhere, nil)
	local := testTransport(t, 1, &testHandler{received: make(chan raftpb.Message, 1)}, nowhere, nil)
	done := make(chan error, 1)
	go func() {
		_, err := local.CallAddress(context.Background(), remote.opts.Listener.Addr().String(), "IdentityPing", nil)
		done <- err
	}()
	select {
	case <-h.calls:
	case <-time.After(time.Second):
		t.Fatal("call did not arrive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := local.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked call succeeded")
		}
	default:
		t.Fatal("Stop returned before control call finished")
	}
	if err := local.Send(ctx, nil); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
func TestMetaTransportRefusesMissingSnapshotFinalAndWrongMessageDomain(t *testing.T) {
	h := &testHandler{received: make(chan raftpb.Message, 2)}
	remote := testTransport(t, 2, h, nowhere, nil)
	local := testTransport(t, 1, &testHandler{received: make(chan raftpb.Message, 1)}, nowhere, nil)
	conn, err := local.connection(remote.opts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = local.authenticated(ctx)
	client := metapb.NewMetaClient(conn)
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}
	data, _ := (&raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: &snapshot}).Marshal()
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&metapb.SnapshotChunk{Header: local.Header(), ToMemberId: 2, RaftMessage: data, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	reply, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ReplyError(reply), fault.ErrInvalid) {
		t.Fatal(reply)
	}
	messages, err := client.StreamMessages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = messages.Send(&metapb.RaftMessage{Header: local.Header(), ToMemberId: 2, RaftMessage: data}); err != nil {
		t.Fatal(err)
	}
	reply, err = messages.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ReplyError(reply), fault.ErrIdentityMismatch) {
		t.Fatal(reply)
	}
	select {
	case <-h.received:
		t.Fatal("invalid framing delivered")
	default:
	}
}
