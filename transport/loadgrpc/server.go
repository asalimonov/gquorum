package loadgrpc

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverAdapter struct {
	loadpb.UnimplementedLoadServer
	transport *Transport
}

type requestHandler[T any] struct {
	bodyLimit int
	route     func(T) (topology.Identity, topology.Identity)
	dispatch  func(context.Context, Ingress, T) (any, error)
}

func receive[T any](parent context.Context, s *serverAdapter, request *loadpb.Envelope,
	handler requestHandler[T]) (*loadpb.Reply, error) {
	t := s.transport
	ctx, err := t.begin(parent)
	if err != nil {
		return encodeReply(nil, err, t.options.MaxMessageBytes)
	}
	defer ctx.Release()
	var input T
	if err := decodeEnvelope(request, &input, handler.bodyLimit, t.options.MaxMessageBytes); err != nil {
		return nil, err
	}
	return handler.handle(ctx.Context, t, ctx.Node, input)
}

func (h requestHandler[T]) handle(ctx context.Context, t *Transport, node Ingress, input T) (*loadpb.Reply, error) {
	from, to := h.route(input)
	if err := t.authenticateSender(ctx, from); err != nil {
		return nil, err
	}
	if from.ClusterID != to.ClusterID || !validIdentity(from) || !validIdentity(to) || to != node.Identity() {
		return encodeReply(nil, fault.ErrIdentityMismatch, t.options.MaxMessageBytes)
	}
	value, err := h.dispatch(ctx, node, input)
	return encodeReply(value, err, t.options.MaxMessageBytes)
}

func (t *Transport) authenticateSender(ctx context.Context, from topology.Identity) error {
	if err := t.options.Authenticate(ctx, from); err != nil {
		return status.Error(codes.Unauthenticated, "load authentication failed")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (s *serverAdapter) SendRaft(ctx context.Context, request *loadpb.Envelope) (*loadpb.Reply, error) {
	return receive(ctx, s, request, requestHandler[load.RaftBatch]{bodyLimit: s.transport.options.MaxMessageBytes,
		route: func(b load.RaftBatch) (topology.Identity, topology.Identity) { return b.From.Node, b.To.Node },
		dispatch: func(ctx context.Context, n Ingress, b load.RaftBatch) (any, error) {
			return struct{}{}, n.ReceiveLoadRaft(ctx, b)
		}})
}

func (s *serverAdapter) ReadReplica(ctx context.Context, request *loadpb.Envelope) (*loadpb.Reply, error) {
	return receive(ctx, s, request, requestHandler[load.ReplicaQuery]{bodyLimit: controlLimit,
		route: func(q load.ReplicaQuery) (topology.Identity, topology.Identity) { return q.From, q.To.Node },
		dispatch: func(ctx context.Context, n Ingress, q load.ReplicaQuery) (any, error) {
			return n.ReadLoadReplica(ctx, q)
		}})
}

func (s *serverAdapter) ApplyAction(ctx context.Context, request *loadpb.Envelope) (*loadpb.Reply, error) {
	return receive(ctx, s, request, requestHandler[load.ActionRequest]{bodyLimit: controlLimit,
		route: func(a load.ActionRequest) (topology.Identity, topology.Identity) { return a.From, a.To.Node },
		dispatch: func(ctx context.Context, n Ingress, a load.ActionRequest) (any, error) {
			return n.ApplyLoadAction(ctx, a)
		}})
}

func (s *serverAdapter) InstallFence(ctx context.Context, request *loadpb.Envelope) (*loadpb.Reply, error) {
	return receive(ctx, s, request, requestHandler[load.FenceRequest]{bodyLimit: controlLimit,
		route: func(f load.FenceRequest) (topology.Identity, topology.Identity) { return f.From, f.To },
		dispatch: func(ctx context.Context, n Ingress, f load.FenceRequest) (any, error) {
			return n.InstallLoadFence(ctx, f)
		}})
}
