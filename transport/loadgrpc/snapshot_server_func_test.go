//go:build functional

package loadgrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type snapshotRPCStats struct {
	ends     atomic.Int64
	payloads atomic.Int64
	end      chan struct{}
}
type snapshotRPCStatsKey struct{}

func (s *snapshotRPCStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, snapshotRPCStatsKey{}, info.FullMethodName == loadpb.Load_TransferSnapshot_FullMethodName)
}
func (s *snapshotRPCStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if selected, _ := ctx.Value(snapshotRPCStatsKey{}).(bool); !selected {
		return
	}
	switch event.(type) {
	case *stats.End:
		s.ends.Add(1)
		select {
		case s.end <- struct{}{}:
		default:
		}
	case *stats.OutPayload:
		s.payloads.Add(1)
	}
}
func (*snapshotRPCStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (*snapshotRPCStats) HandleConn(context.Context, stats.ConnStats) {}

type snapshotRPCService struct {
	options     snapshot.Options
	mu          sync.Mutex
	ack         snapshot.Ack
	generation  uint64
	signals     map[uint64]chan struct{}
	begins      atomic.Int64
	writes      atomic.Int64
	detaches    atomic.Int64
	begin       func(context.Context, snapshot.BeginRequest) error
	write       func(context.Context, snapshot.Chunk) error
	lastRequest snapshot.BeginRequest
	closeFinish bool
	finish      func(context.Context, snapshot.Session) error
	notNeeded   bool
}

func (s *snapshotRPCService) Options() snapshot.Options { return s.options }
func (s *snapshotRPCService) Begin(ctx context.Context, r snapshot.BeginRequest) (snapshot.Ack, error) {
	s.begins.Add(1)
	if s.begin != nil {
		if err := s.begin(ctx, r); err != nil {
			return snapshot.Ack{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, signal := range s.signals {
		select {
		case <-signal:
		default:
			close(signal)
		}
	}
	s.generation++
	s.signals[s.generation] = make(chan struct{})
	next := uint64(0)
	if r.Resume {
		next = s.ack.NextOffset
	}
	s.lastRequest = r
	if s.notNeeded {
		return snapshot.Ack{Session: snapshot.Session{Key: r.Manifest.Key},
			State: snapshot.DeliveryNotNeeded, RemainingTotal: s.options.TotalTimeout,
			ProgressTimeout: s.options.ProgressTimeout, ReceiverRunID: "receiver-run"}, nil
	}
	if r.Resume && s.ack.State == snapshot.DeliveryAccepted {
		return s.ack, nil
	}
	s.ack = snapshot.Ack{Session: snapshot.Session{Key: r.Manifest.Key, Attachment: s.generation}, NextOffset: next,
		ChunkBytes: min(r.Manifest.ChunkBytes, r.MaxChunkBytes, uint32(s.options.ChunkBytes)), State: snapshot.DeliveryReceiving,
		RemainingTotal: s.options.TotalTimeout, ProgressTimeout: s.options.ProgressTimeout, ReceiverRunID: strings.Repeat("r", 512)}
	return s.ack, nil
}
func (s *snapshotRPCService) Write(ctx context.Context, c snapshot.Chunk) (snapshot.Ack, error) {
	s.writes.Add(1)
	if s.write != nil {
		if err := s.write(ctx, c); err != nil {
			return snapshot.Ack{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ack.NextOffset = max(s.ack.NextOffset, c.Offset+uint64(len(c.Data)))
	return s.ack, nil
}
func (s *snapshotRPCService) Finish(ctx context.Context, session snapshot.Session) (snapshot.Ack, error) {
	if s.finish != nil {
		if err := s.finish(ctx, session); err != nil {
			return snapshot.Ack{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ack.State = snapshot.DeliveryAccepted
	if s.closeFinish {
		s.closeLocked(s.ack.Session)
	}
	return s.ack, nil
}
func (s *snapshotRPCService) closeLocked(session snapshot.Session) {
	if signal := s.signals[session.Attachment]; signal != nil {
		select {
		case <-signal:
		default:
			close(signal)
		}
	}
}
func (s *snapshotRPCService) Detach(_ context.Context, session snapshot.Session) error {
	s.detaches.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(session)
	return nil
}
func (s *snapshotRPCService) Abort(ctx context.Context, session snapshot.Session) error {
	return s.Detach(ctx, session)
}
func (s *snapshotRPCService) SessionDone(session snapshot.Session) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.signals[session.Attachment]
}
func (s *snapshotRPCService) List(snapshot.Query) snapshot.Page { return snapshot.Page{} }
func (s *snapshotRPCService) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.signals {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

type snapshotRPCIngress struct {
	*ingressDouble
	snapshots snapshot.Service
}

func (n *snapshotRPCIngress) LoadSnapshots() snapshot.Service { return n.snapshots }

type snapshotRPCFixture struct {
	transport *Transport
	service   *snapshotRPCService
	client    loadpb.LoadClient
	conn      *grpc.ClientConn
	stats     *snapshotRPCStats
	open      *loadpb.SnapshotOpen
	listener  *bufconn.Listener
}

func newSnapshotRPCFixture(t *testing.T, options snapshot.Options) *snapshotRPCFixture {
	return newSnapshotRPCFixtureWith(t, options, nil)
}
func newSnapshotRPCFixtureWith(t *testing.T, options snapshot.Options, interceptor grpc.StreamServerInterceptor) *snapshotRPCFixture {
	t.Helper()
	options, err := options.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	transport, err := New(Options{Resolve: func(context.Context, topology.Identity) (string, error) { return "unused", nil },
		Authenticate: func(context.Context, topology.Identity) error { return nil },
		DialOptions:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, MaxConcurrentCalls: 1})
	if err != nil {
		t.Fatal(err)
	}
	local := &ingressDouble{id: testSnapshotIdentity("receiver", 2),
		read: func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
			return load.ReplicaStatus{Peer: q.To, Found: true}, nil
		}}
	service := &snapshotRPCService{options: options, signals: make(map[uint64]chan struct{})}
	if err = transport.Bind(&snapshotRPCIngress{local, service}); err != nil {
		t.Fatal(err)
	}
	observed := &snapshotRPCStats{end: make(chan struct{}, 32)}
	serverOptions := []grpc.ServerOption{grpc.StatsHandler(observed)}
	if interceptor != nil {
		serverOptions = append(serverOptions, grpc.StreamInterceptor(interceptor))
	}
	server := grpc.NewServer(serverOptions...)
	transport.Register(server)
	serverDone := make(chan struct{})
	go func() { defer close(serverDone); _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///snapshot", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	from := replicaPeer(testSnapshotIdentity("sender", 1), 1)
	to := replicaPeer(local.id, 2)
	header, err := (&raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("bytes"))
	open := &loadpb.SnapshotOpen{Version: 1, From: encodeRaftPeer(from), To: encodeRaftPeer(to), DeliveryId: 1,
		NativeHeader: header, TotalBytes: 5, Sha256: sum[:], ChunkBytes: 4, RemainingTotalNanos: uint64(options.TotalTimeout), ProgressTimeoutNanos: uint64(options.ProgressTimeout)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := transport.Close(ctx); err != nil {
			t.Error(err)
		}
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		<-serverDone
	})
	return &snapshotRPCFixture{transport, service, loadpb.NewLoadClient(conn), conn, observed, open, listener}
}
func testSnapshotIdentity(name string, id uint64) topology.Identity {
	return topology.Identity{ClusterID: "cluster", NodeName: name, MemberID: id, InstanceID: name + "-process"}
}
func openSnapshotRPC(t *testing.T, f *snapshotRPCFixture) (loadpb.Load_TransferSnapshotClient, context.CancelFunc, *loadpb.SnapshotAck) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	stream, err := f.client.TransferSnapshot(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: f.open}}); err != nil {
		cancel()
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return stream, cancel, ack
}
func waitSnapshotRPC(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-timer.C:
			t.Fatal("snapshot RPC barrier timeout")
		}
	}
}
func snapshotHandlersIdle(t *Transport) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotPreOpen == 0 && t.snapshotHandlers == 0
}

func TestSnapshotRPC_HalfOpenCancellationAndExactlyOneCompletion(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"session", "close", "deadline", "empty"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			options := snapshot.Options{ProgressTimeout: 60 * time.Millisecond, TotalTimeout: time.Second}
			f := newSnapshotRPCFixture(t, options)
			var cancel context.CancelFunc
			if reason == "empty" {
				ctx, release := context.WithTimeout(t.Context(), time.Second)
				cancel = release
				if _, err := f.client.TransferSnapshot(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				_, release, _ := openSnapshotRPC(t, f)
				cancel = release
			}
			defer cancel()
			if reason == "session" {
				f.service.stop()
			}
			if reason == "close" {
				ctx, release := context.WithTimeout(t.Context(), time.Second)
				defer release()
				if err := f.transport.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
			if f.service.writes.Load() != 0 {
				t.Fatal("late mutation")
			}
		})
	}
}
func TestSnapshotRPC_ReplacementAbortAndFinalAck(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	old, cancel, first := openSnapshotRPC(t, f)
	defer cancel()
	f.open.Resume = true
	current, cancelCurrent, second := openSnapshotRPC(t, f)
	defer cancelCurrent()
	if second.Attachment <= first.Attachment {
		t.Fatal("attachment not fenced")
	}
	if _, err := old.Recv(); err == nil {
		t.Fatal("old half-open RPC remained live")
	}
	if err := current.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Abort{Abort: &loadpb.SnapshotAbort{Attachment: second.Attachment}}}); err != nil {
		t.Fatal(err)
	}
	ack, err := current.Recv()
	if err != nil || ack.Failure == nil {
		t.Fatal(ack, err)
	}
	waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 2 && snapshotHandlersIdle(f.transport) })
}
func TestSnapshotRPC_FinishWritesAckEvenAfterNativeCleanup(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	f.service.closeFinish = true
	stream, cancel, initial := openSnapshotRPC(t, f)
	defer cancel()
	for offset := 0; offset < 5; {
		n := min(4, 5-offset)
		data := []byte("bytes")[offset : offset+n]
		sum := sha256.Sum256(data)
		err := stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{
			Attachment: initial.Attachment, Offset: uint64(offset), Data: data, Sha256: sum[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		ack, err := stream.Recv()
		if err != nil || ack.NextOffset != uint64(offset+n) {
			t.Fatal(ack, err)
		}
		offset += n
	}
	if err := stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Finish{Finish: &loadpb.SnapshotFinish{Attachment: initial.Attachment}}}); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || ack.State != string(snapshot.DeliveryAccepted) {
		t.Fatal(ack, err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
	f.service.mu.Lock()
	request := f.service.lastRequest
	f.service.mu.Unlock()
	if request.MaxChunkBytes == 0 || request.Manifest.ChunkBytes != f.open.ChunkBytes {
		t.Fatal(request)
	}
}
func TestSnapshotRPC_RejectsInvalidFramesBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"before-open", "version", "second-open", "attachment"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotRPCFixture(t, snapshot.Options{})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			stream, err := f.client.TransferSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "before-open" {
				_ = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Finish{Finish: &loadpb.SnapshotFinish{Attachment: 1}}})
			} else {
				if kind == "version" {
					f.open.Version++
				}
				_ = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: f.open}})
				if kind != "version" {
					ack, err := stream.Recv()
					if err != nil {
						t.Fatal(err)
					}
					frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: f.open}}
					if kind == "attachment" {
						frame.Body = &loadpb.SnapshotFrame_Finish{Finish: &loadpb.SnapshotFinish{Attachment: ack.Attachment + 1}}
					}
					_ = stream.Send(frame)
				}
			}
			ack, err := stream.Recv()
			if err == nil && ack.Failure == nil {
				t.Fatal(ack)
			}
			waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
			if f.service.writes.Load() != 0 {
				t.Fatal("invalid frame dispatched")
			}
		})
	}
}
func TestSnapshotRPC_BlockedSendJoinsAfterHandlerReturn(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	stream, cancel, ack := openSnapshotRPC(t, f)
	defer cancel()
	sendingDone := make(chan struct{})
	go func() {
		defer close(sendingDone)
		for range 10000 {
			if err := stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{Attachment: ack.Attachment, Data: []byte("byte"), Sha256: func() []byte { sum := sha256.Sum256([]byte("byte")); return sum[:] }()}}}); err != nil {
				return
			}
		}
	}()
	waitSnapshotRPC(t, func() bool { return f.service.writes.Load() > 200 })
	// No client Recv occurs after admission. Server outbound stream flow control
	// fills; SendMsg cannot return until handler return cancels actual stream I/O.
	time.Sleep(20 * time.Millisecond)
	before := f.service.writes.Load()
	time.Sleep(20 * time.Millisecond)
	if after := f.service.writes.Load(); after != before {
		t.Fatalf("send not blocked: %d -> %d", before, after)
	}
	f.service.stop()
	waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
	cancel()
	select {
	case <-sendingDone:
	case <-time.After(time.Second):
		t.Fatal("client sender did not join")
	}
}
func TestSnapshotRPC_PreOpenQuotaDoesNotConsumeAuthenticatedOrOrdinarySlots(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	active, cancel, ack := openSnapshotRPC(t, f)
	defer cancel()
	ctx, cancelEmpty := context.WithCancel(t.Context())
	defer cancelEmpty()
	for range snapshotPreOpenMinimum {
		if _, err := f.client.TransferSnapshot(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitSnapshotRPC(t, func() bool {
		f.transport.mu.Lock()
		defer f.transport.mu.Unlock()
		return f.transport.snapshotPreOpen == snapshotPreOpenMinimum
	})
	request, err := envelope(load.ReplicaQuery{From: testSnapshotIdentity("sender", 1),
		To: replicaPeer(testSnapshotIdentity("receiver", 2), 2)}, controlLimit, defaultMaxMessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := f.client.ReadReplica(t.Context(), request)
	if err != nil || reply.ErrorCode != "" {
		t.Fatal(reply, err)
	}
	sum := sha256.Sum256([]byte("byte"))
	if err = active.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{Attachment: ack.Attachment, Data: []byte("byte"), Sha256: sum[:]}}}); err != nil {
		t.Fatal(err)
	}
	if reply, err := active.Recv(); err != nil || reply.NextOffset != 4 {
		t.Fatal(reply, err)
	}
	over, err := f.client.TransferSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = over.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	cancelEmpty()
	cancel()
	waitSnapshotRPC(t, func() bool { return snapshotHandlersIdle(f.transport) })
}

func TestSnapshotRPC_CanceledCallbacksRetainQuotaUntilActualReturn(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"authenticate", "write"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotRPCFixture(t, snapshot.Options{})
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			block := func() { close(entered); <-release }
			if kind == "authenticate" {
				f.transport.options.Authenticate = func(context.Context, topology.Identity) error { block(); return nil }
			} else {
				f.service.write = func(context.Context, snapshot.Chunk) error { block(); return nil }
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			stream, err := f.client.TransferSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: f.open}}); err != nil {
				t.Fatal(err)
			}
			if kind == "write" {
				ack, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				data := []byte("byte")
				sum := sha256.Sum256(data)
				if err = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{Attachment: ack.Attachment, Data: data, Sha256: sum[:]}}}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stopCtx, stopCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			err = f.transport.Close(stopCtx)
			stopCancel()
			if !errors.Is(err, context.DeadlineExceeded) || snapshotHandlersIdle(f.transport) {
				t.Fatal("callback ownership released early", err)
			}
			once.Do(func() { close(release) })
			if err = f.transport.Close(ctx); err != nil {
				t.Fatal(err)
			}
			waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
		})
	}
}

func TestSnapshotRPC_FrameCapacityKeepsOriginalManifest(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	f.transport.options.MaxMessageBytes = 1024
	f.open.ChunkBytes = 4096
	_, cancel, ack := openSnapshotRPC(t, f)
	defer cancel()
	f.service.mu.Lock()
	request := f.service.lastRequest
	f.service.mu.Unlock()
	if request.Manifest.ChunkBytes != 4096 || request.MaxChunkBytes == 0 || request.MaxChunkBytes >= 1024 || ack.ChunkBytes != request.MaxChunkBytes {
		t.Fatal(request, ack)
	}
}
