package main

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Each Node generation owns a separately bound production adapter. The borrowed
// load listener remains the same; RPC dispatch captures one exact implementation.
type fixtureLoadBinding struct {
	snapshots      *fixtureSnapshotService
	transport      *loadgrpc.Transport
	descriptor     *grpc.ServiceDesc
	implementation any
}

func (a *application) prepareLoadBinding(options *gquorum.NodeOptions) (*fixtureLoadBinding, error) {
	credential, err := loadgrpc.NewSharedToken(a.cfg.Credential, false)
	if err != nil {
		return nil, err
	}
	transport, err := loadgrpc.New(loadgrpc.Options{
		Resolve: func(ctx context.Context, id topology.Identity) (string, error) {
			return a.loadAddress(ctx, id.NodeName, id.MemberID)
		},
		Authenticate: credential.Authenticate, MaxMessageBytes: 64 << 20,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(credential)}})
	if err != nil {
		return nil, err
	}
	binding := &fixtureLoadBinding{transport: transport}
	options.Loads.Transport = loadTransport{app: a, binding: binding}
	return binding, nil
}
func (b *fixtureLoadBinding) bind(a *application, n gquorum.Node) error {
	b.snapshots = &fixtureSnapshotService{Service: n.LoadSnapshots(), app: a, receipts: make(map[snapshot.Key]fixtureSnapshotReceipt), faults: fixtureSnapshotFaults{write: a.snapshotWrite, sendAck: a.snapshotWireAck}}
	ingress := &fixtureLoadIngress{Node: n, app: a, snapshots: b.snapshots}
	if err := b.transport.Bind(ingress); err != nil {
		return err
	}
	b.transport.Register(b)
	return nil
}
func (b *fixtureLoadBinding) RegisterService(desc *grpc.ServiceDesc, implementation any) {
	b.descriptor, b.implementation = desc, implementation
}
func (a *application) productionLoadService() (*fixtureLoadBinding, error) {
	a.generationMu.RLock()
	defer a.generationMu.RUnlock()
	g := a.generation
	if g == nil || !g.accepting || g.production == nil {
		return nil, status.Error(codes.Unavailable, "load generation stopped")
	}
	return g.production, nil
}
func (a *application) registerProductionLoad(server *grpc.Server, b *fixtureLoadBinding) {
	desc := *b.descriptor
	desc.Methods = append([]grpc.MethodDesc(nil), desc.Methods...)
	for i, method := range desc.Methods {
		desc.Methods[i].Handler = func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			implementation, err := a.productionLoadService()
			if err != nil {
				return nil, err
			}
			return method.Handler(implementation.implementation, ctx, decode, interceptor)
		}
	}
	desc.Streams = append([]grpc.StreamDesc(nil), desc.Streams...)
	for i, stream := range desc.Streams {
		desc.Streams[i].Handler = func(_ any, rpc grpc.ServerStream) error {
			implementation, err := a.productionLoadService()
			if err != nil {
				return err
			}
			if stream.StreamName == "TransferSnapshot" {
				call := &fixtureSnapshotCall{}
				rpc = &fixtureSnapshotStream{ServerStream: rpc, ctx: context.WithValue(rpc.Context(), fixtureSnapshotContextKey{}, call),
					call: call, service: implementation.snapshots}
			}
			return stream.Handler(implementation.implementation, rpc)
		}
	}
	// Handler closures capture the current generation at dispatch. No original
	// implementation is retained by the listener after that generation retires.
	server.RegisterService(&desc, nil)
}

type fixtureLoadIngress struct {
	gquorum.Node
	app       *application
	snapshots snapshot.Service
}

func (n *fixtureLoadIngress) LoadSnapshots() snapshot.Service { return n.snapshots }
func (n *fixtureLoadIngress) ReceiveLoadRaft(ctx context.Context, batch load.RaftBatch) error {
	filtered, err := n.app.filterReadRaft(batch)
	if err != nil {
		return err
	}
	if len(filtered.Messages) == 0 {
		return nil
	}
	if err = n.Node.ReceiveLoadRaft(ctx, filtered); err != nil {
		return err
	}
	n.app.recordReadReceive(filtered)
	for _, message := range filtered.Messages {
		if message.Type == raftpb.MsgSnap {
			n.app.receivedClusterSnapshot(batch.To.Replica.GroupID)
		}
	}
	return nil
}

type fixtureSnapshotReceipt struct {
	verified uint64
	accepted bool
}

// These fixture-owned seams support authenticated chunk/Finish/reply faults.
// They never change library semantics or substitute a successful transfer.
type fixtureSnapshotFaults struct {
	write   func(context.Context, snapshot.Chunk) (snapshot.Chunk, error)
	finish  func(context.Context, snapshot.Session) error
	reply   func(context.Context, snapshot.Ack) error
	sendAck func(context.Context, fixtureSnapshotWireAck) error
}
type fixtureSnapshotService struct {
	notNeeded    uint64
	receiptOrder []snapshot.Key
	snapshot.Service
	app      *application
	mu       sync.Mutex
	receipts map[snapshot.Key]fixtureSnapshotReceipt
	faults   fixtureSnapshotFaults
}

func (s *fixtureSnapshotService) Begin(ctx context.Context, request snapshot.BeginRequest) (snapshot.Ack, error) {
	if call, ok := ctx.Value(fixtureSnapshotContextKey{}).(*fixtureSnapshotCall); ok {
		call.mu.Lock()
		call.key = request.Manifest.Key
		call.mu.Unlock()
	}
	var message raftpb.Message
	if err := message.Unmarshal(request.Manifest.NativeHeader); err != nil {
		return snapshot.Ack{}, err
	}
	_, err := s.app.filterReadRaft(load.RaftBatch{From: request.Manifest.Key.From, To: request.Manifest.Key.To,
		DeliveryID: request.Manifest.Key.DeliveryID, Messages: []raftpb.Message{message}})
	if err != nil {
		return snapshot.Ack{}, err
	}
	ack, err := s.Service.Begin(ctx, request)
	if err == nil {
		err = s.recordReply(ctx, ack)
	}
	return ack, err
}
func (s *fixtureSnapshotService) Write(ctx context.Context, chunk snapshot.Chunk) (snapshot.Ack, error) {
	s.mu.Lock()
	hook := s.faults.write
	s.mu.Unlock()
	value := chunk
	if hook != nil {
		var err error
		value, err = hook(ctx, chunk)
		if err != nil {
			return snapshot.Ack{}, err
		}
	}
	ack, err := s.Service.Write(ctx, value)
	if err == nil {
		err = s.recordReply(ctx, ack)
	}
	return ack, err
}
func (s *fixtureSnapshotService) Finish(ctx context.Context, session snapshot.Session) (snapshot.Ack, error) {
	s.mu.Lock()
	hook := s.faults.finish
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, session); err != nil {
			return snapshot.Ack{}, err
		}
	}
	if err := s.app.snapshotPhase(ctx, session.Key.To.Node, walKey{session.Key.To.Replica.GroupID, session.Key.To.Replica.ReplicaID}, "before_finish", 0); err != nil {
		return snapshot.Ack{}, err
	}
	ack, err := s.Service.Finish(ctx, session)
	if err == nil {
		err = s.app.snapshotPhase(ctx, session.Key.To.Node, walKey{session.Key.To.Replica.GroupID, session.Key.To.Replica.ReplicaID}, "after_handoff", 0)
	}
	if err == nil {
		err = s.recordReply(ctx, ack)
	}
	return ack, err
}
func (s *fixtureSnapshotService) recordReply(ctx context.Context, ack snapshot.Ack) error {
	s.mu.Lock()
	record, exists := s.receipts[ack.Session.Key]
	if !exists {
		if len(s.receiptOrder) >= 512 {
			delete(s.receipts, s.receiptOrder[0])
			s.receiptOrder = s.receiptOrder[1:]
		}
		s.receiptOrder = append(s.receiptOrder, ack.Session.Key)
	}
	if ack.State == snapshot.DeliveryNotNeeded {
		s.notNeeded++
	}
	record.verified = max(record.verified, ack.NextOffset)
	accepted := ack.State == snapshot.DeliveryAccepted && !record.accepted
	record.accepted = record.accepted || accepted
	s.receipts[ack.Session.Key] = record
	hook := s.faults.reply
	s.mu.Unlock()
	if accepted {
		s.app.receivedClusterSnapshot(ack.Session.Key.To.Replica.GroupID)
	}
	if hook != nil {
		return hook(ctx, ack)
	}
	return nil
}
func (t loadTransport) SendSnapshot(ctx context.Context, send snapshot.Send) (snapshot.Ack, error) {
	if t.binding == nil {
		return snapshot.Ack{}, &snapshot.TransferError{Reason: snapshot.ReasonUnsupported, Cause: fault.ErrUnsupported}
	}
	t.app.recordSnapshotOffer(send)
	return t.binding.transport.SendSnapshot(ctx, send)
}

type fixtureSnapshotContextKey struct{}
type fixtureSnapshotCall struct {
	mu  sync.Mutex
	key snapshot.Key
}
type fixtureSnapshotWireAck struct {
	Key                  snapshot.Key
	Attachment, Verified uint64
	State                snapshot.DeliveryState
}
type fixtureSnapshotStream struct {
	grpc.ServerStream
	ctx     context.Context
	call    *fixtureSnapshotCall
	service *fixtureSnapshotService
}

func (s *fixtureSnapshotStream) Context() context.Context { return s.ctx }
func (s *fixtureSnapshotStream) SendMsg(message any) error {
	// Scalar accessors avoid importing the adapter's internal generated schema.
	if ack, ok := message.(interface {
		GetState() string
		GetNextOffset() uint64
		GetAttachment() uint64
	}); ok {
		s.call.mu.Lock()
		key := s.call.key
		s.call.mu.Unlock()
		service := s.service
		if service != nil {
			service.mu.Lock()
			hook := service.faults.sendAck
			service.mu.Unlock()
			if hook != nil {
				if err := hook(s.ctx, fixtureSnapshotWireAck{Key: key, Attachment: ack.GetAttachment(),
					Verified: ack.GetNextOffset(), State: snapshot.DeliveryState(ack.GetState())}); err != nil {
					return err
				}
			}
		}
	}
	return s.ServerStream.SendMsg(message)
}
