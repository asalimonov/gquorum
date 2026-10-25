//go:build functional

package loadgrpc

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type ingressDouble struct {
	id      topology.Identity
	receive func(context.Context, load.RaftBatch) error
	read    func(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
	action  func(context.Context, load.ActionRequest) (load.ActionResult, error)
	fence   func(context.Context, load.FenceRequest) (load.FenceResult, error)
	report  func(context.Context, load.DeliveryReport) error
}

func (n *ingressDouble) Identity() topology.Identity { return n.id }
func (n *ingressDouble) ReceiveLoadRaft(ctx context.Context, b load.RaftBatch) error {
	return n.receive(ctx, b)
}
func (n *ingressDouble) ReadLoadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return n.read(ctx, q)
}
func (n *ingressDouble) ApplyLoadAction(ctx context.Context, a load.ActionRequest) (load.ActionResult, error) {
	return n.action(ctx, a)
}
func (n *ingressDouble) InstallLoadFence(ctx context.Context, f load.FenceRequest) (load.FenceResult, error) {
	return n.fence(ctx, f)
}
func (n *ingressDouble) ReportLoadDelivery(ctx context.Context, d load.DeliveryReport) error {
	return n.report(ctx, d)
}

func newProtocolPair(t *testing.T, limit int) (*Transport, *Transport, *ingressDouble, *ingressDouble) {
	t.Helper()
	return newProtocolPairWith(t, limit, func(s *grpc.Server, receiver *Transport) { receiver.Register(s) })
}

// newProtocolPairWith lets a test register another server implementation.
func newProtocolPairWith(t *testing.T, limit int, register func(*grpc.Server, *Transport)) (*Transport, *Transport, *ingressDouble, *ingressDouble) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	token, _ := NewSharedToken("private-test-credential", false)
	options := Options{Resolve: func(context.Context, topology.Identity) (string, error) { return "passthrough:///test-peer", nil }, Authenticate: token.Authenticate, MaxMessageBytes: limit, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(token), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) })}}
	receiver, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	local := &ingressDouble{id: topology.Identity{ClusterID: "c", NodeName: "a", MemberID: 1, InstanceID: "a1"}}
	remote := &ingressDouble{id: topology.Identity{ClusterID: "c", NodeName: "b", MemberID: 2, InstanceID: "b1"}}
	if err := sender.Bind(local); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Bind(remote); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(limit), grpc.MaxSendMsgSize(limit))
	register(server, receiver)
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sender.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := receiver.Close(ctx); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-stopped
	})
	return sender, receiver, local, remote
}
func replicaPeer(id topology.Identity, rid uint64) load.Peer {
	return load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "g", ReplicaID: rid}}
}

func TestAuthenticatedProtocolPreservesPublicValues(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 16<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	query := load.ReplicaQuery{From: local.id, To: replicaPeer(remote.id, 7), Native: true}
	expected := load.ReplicaStatus{Peer: query.To, Found: true, Native: &topology.NativeState{GroupID: "g"}, LocalState: "ready"}
	remote.read = func(_ context.Context, got load.ReplicaQuery) (load.ReplicaStatus, error) {
		if got != query {
			t.Error("query changed")
		}
		return expected, nil
	}
	actual, err := sender.ReadReplica(ctx, query)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("native reply lost: %+v %v", actual, err)
	}
	action := load.ActionRequest{From: local.id, To: query.To}
	refusal := &membership.SafetyRefusal{GroupID: "g", ReplicaID: 7, Operation: "remove", Reason: "quorum", LiveVoters: 1, Quorum: 2, HealthKnown: true}
	remote.action = func(_ context.Context, got load.ActionRequest) (load.ActionResult, error) {
		if !reflect.DeepEqual(action, got) {
			t.Error("action changed")
		}
		return load.ActionResult{}, refusal
	}
	_, err = sender.ApplyAction(ctx, action)
	var decoded *membership.SafetyRefusal
	if !errors.As(err, &decoded) || !reflect.DeepEqual(decoded, refusal) {
		t.Fatalf("lost safety error: %v", err)
	}
	remote.action = func(_ context.Context, got load.ActionRequest) (load.ActionResult, error) {
		return load.ActionResult{From: got.To, Authorization: got.Authorization, LocalTarget: got.LocalTarget}, nil
	}
	result, err := sender.ApplyAction(ctx, action)
	if err != nil || result.From != query.To {
		t.Fatal(result, err)
	}
	fence := load.FenceRequest{From: local.id, To: remote.id, ReplicaID: 7}
	remote.fence = func(_ context.Context, got load.FenceRequest) (load.FenceResult, error) {
		if !reflect.DeepEqual(got, fence) {
			t.Error("fence changed")
		}
		return load.FenceResult{From: got.To}, nil
	}
	fresult, err := sender.InstallFence(ctx, fence)
	if err != nil || fresult.From != remote.id {
		t.Fatal(fresult, err)
	}
}

func TestSnapshotReportsOnlyCompleteAcceptance(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 1500)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	batch := load.RaftBatch{From: replicaPeer(local.id, 1), To: replicaPeer(remote.id, 2), DeliveryID: 91}
	for i := range 10 {
		batch.Messages = append(batch.Messages, raftpb.Message{From: 1, To: 2, Type: raftpb.MsgApp, Index: uint64(i + 1), Context: make([]byte, 300)})
	}
	batch.Messages = append(batch.Messages, raftpb.Message{From: 1, To: 2, Type: raftpb.MsgSnap})
	var mu sync.Mutex
	var messages []raftpb.Message
	fail := false
	remote.receive = func(_ context.Context, part load.RaftBatch) error {
		mu.Lock()
		defer mu.Unlock()
		if part.From != batch.From || part.To != batch.To || part.DeliveryID != 91 {
			t.Error("batch identity changed")
		}
		if fail && len(messages) > 0 {
			return fault.ErrOverloaded
		}
		messages = append(messages, part.Messages...)
		return nil
	}
	var reports []load.DeliveryReport
	local.report = func(_ context.Context, r load.DeliveryReport) error {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, r)
		if r.Kind == load.DeliverySnapshotSucceeded && !sameRaftMessages(messages, batch.Messages) {
			t.Error("success before complete acceptance")
		}
		return nil
	}
	if err := sender.SendRaft(ctx, batch); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(reports) != 1 || reports[0].Kind != load.DeliverySnapshotSucceeded || reports[0].DeliveryID != 91 || reports[0].From != batch.From || reports[0].To != batch.To {
		t.Fatal("incorrect success report", reports)
	}
	messages = nil
	reports = nil
	fail = true
	mu.Unlock()
	if err := sender.SendRaft(ctx, batch); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatalf("expected partial delivery failure: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 1 || reports[0].Kind != load.DeliverySnapshotFailed || len(messages) >= len(batch.Messages) {
		t.Fatal("partial snapshot reported as accepted", reports)
	}
}

func TestAuthenticationAndExactRecipientBeforeIngress(t *testing.T) {
	t.Parallel()
	sender, receiver, local, remote := newProtocolPair(t, 4096)
	var calls atomic.Int32
	remote.read = func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
		calls.Add(1)
		return load.ReplicaStatus{Peer: q.To}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := load.ReplicaQuery{From: local.id, To: replicaPeer(remote.id, 2)}
	request, err := envelope(q, 4096, 4096)
	if err != nil {
		t.Fatal(err)
	}
	// Direct protocol dispatch supplies no authenticated metadata; Node is never called.
	_, err = (&serverAdapter{transport: receiver}).ReadReplica(ctx, request)
	if status.Code(err) != codes.Unauthenticated || calls.Load() != 0 {
		t.Fatal("unauthenticated ingress", err)
	}
	q.To.Node.InstanceID = "old-process"
	if _, err := sender.ReadReplica(ctx, q); !errors.Is(err, fault.ErrIdentityMismatch) || calls.Load() != 0 {
		t.Fatal("stale recipient dispatched", err)
	}
	q.To.Node = remote.id
	q.From.InstanceID = "forged-local"
	if _, err := sender.ReadReplica(ctx, q); !errors.Is(err, fault.ErrIdentityMismatch) || calls.Load() != 0 {
		t.Fatal("foreign local sender dispatched", err)
	}
	if err := sender.Bind(local); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("rebind permitted", err)
	}
}

func TestCloseRetainsIgnoredCancellationOwnership(t *testing.T) {
	t.Parallel()
	_, receiver, local, remote := newProtocolPair(t, 4096)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	receiver.options.Authenticate = func(ctx context.Context, _ topology.Identity) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}
	q := load.ReplicaQuery{From: local.id, To: replicaPeer(remote.id, 2)}
	request, _ := envelope(q, 4096, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&serverAdapter{transport: receiver}).ReadReplica(context.Background(), request)
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := receiver.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ignored callback was not joined: %v", err)
	}
	<-canceled
	if _, err := receiver.begin(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("new work accepted", err)
	}
	select {
	case <-receiver.done:
		t.Fatal("released unresolved owner")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := receiver.Close(ctx2); err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestConnectionAndCallBounds(t *testing.T) {
	t.Parallel()
	sender, _, local, _ := newProtocolPair(t, 4096)
	sender.options.MaxConnections = 1
	sender.options.MaxConcurrentCalls = 1
	sender.options.Resolve = func(_ context.Context, id topology.Identity) (string, error) {
		return "passthrough:///" + id.NodeName, nil
	}
	ctx, err := sender.begin(context.Background())
	if err != nil || ctx.Node != local {
		t.Fatal(err)
	}
	defer ctx.Release()
	if _, err := sender.begin(context.Background()); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("call capacity ignored", err)
	}
	release, err := sender.client(ctx.Context, topology.Identity{NodeName: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.client(ctx.Context, topology.Identity{NodeName: "second"}); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("busy connection evicted", err)
	}
	release.Release()
	release2, err := sender.client(ctx.Context, topology.Identity{NodeName: "second"})
	if err != nil {
		t.Fatal(err)
	}
	release2.Release()
	sender.mu.Lock()
	count := len(sender.clients)
	sender.mu.Unlock()
	if count != 1 {
		t.Fatal("unbounded connection cache")
	}
}

// The generated protocol remains internal; application integration only needs
// the public adapter and the caller's grpc.Server.
var _ loadpb.LoadServer = (*serverAdapter)(nil)

func (*ingressDouble) LoadSnapshots() snapshot.Service { return unsupportedSnapshotService{} }

type unsupportedSnapshotService struct{}

func (unsupportedSnapshotService) Options() snapshot.Options {
	options, _ := (snapshot.Options{}).Resolve()
	return options
}
func (unsupportedSnapshotService) Begin(context.Context, snapshot.BeginRequest) (snapshot.Ack, error) {
	return snapshot.Ack{}, snapshotWireError(snapshot.ReasonUnsupported, nil)
}
func (unsupportedSnapshotService) Write(context.Context, snapshot.Chunk) (snapshot.Ack, error) {
	return snapshot.Ack{}, snapshotWireError(snapshot.ReasonUnsupported, nil)
}
func (unsupportedSnapshotService) Finish(context.Context, snapshot.Session) (snapshot.Ack, error) {
	return snapshot.Ack{}, snapshotWireError(snapshot.ReasonUnsupported, nil)
}
func (unsupportedSnapshotService) Detach(context.Context, snapshot.Session) error {
	return snapshotWireError(snapshot.ReasonUnsupported, nil)
}
func (unsupportedSnapshotService) Abort(context.Context, snapshot.Session) error {
	return snapshotWireError(snapshot.ReasonUnsupported, nil)
}
func (unsupportedSnapshotService) SessionDone(snapshot.Session) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (unsupportedSnapshotService) List(snapshot.Query) snapshot.Page { return snapshot.Page{} }
