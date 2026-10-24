package loadgrpc

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

type rpcExchange struct {
	method        string
	input, output any
	limit         int
}

type callRoute struct {
	from, to topology.Identity
}

func invoke(ctx context.Context, conn *grpc.ClientConn, exchange rpcExchange) error {
	reply := new(loadpb.Reply)
	if err := conn.Invoke(ctx, exchange.method, exchange.input, reply); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return transportError(err)
	}
	return decodeReply(reply, exchange.output, exchange.limit)
}

func (t *Transport) call(parent context.Context, route callRoute, exchange rpcExchange) error {
	ctx, err := t.begin(parent)
	if err != nil {
		return err
	}
	defer ctx.Release()
	if err := checkRoute(ctx.Node.Identity(), route.from, route.to); err != nil {
		return err
	}
	request, err := envelope(exchange.input, controlLimit, t.options.MaxMessageBytes)
	if err != nil {
		return err
	}
	return t.invokeOnRoute(ctx.Context, route.to, rpcExchange{method: exchange.method,
		input: request, output: exchange.output,
		limit: t.options.MaxMessageBytes})
}

func (t *Transport) invokeOnRoute(ctx context.Context, to topology.Identity, exchange rpcExchange) error {
	conn, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	defer conn.Release()
	return invoke(ctx, conn.Conn, exchange)
}

// SendRaft delivers ordered subsets with unchanged endpoints and delivery ID.
// Batches without a snapshot are written to the route's stream and return once
// sent; a later refusal is reported as an unreachable delivery. Snapshot
// batches use a unary call, and success is reported only after every subset
// is accepted. Returning an error also lets Node's sender report failure if its
// context has already expired and no explicit delivery report can be accepted.
func (t *Transport) SendRaft(parent context.Context, batch load.RaftBatch) error {
	ctx, err := t.begin(parent)
	if err != nil {
		return err
	}
	defer ctx.Release()
	if err := checkRoute(ctx.Node.Identity(), batch.From.Node, batch.To.Node); err != nil {
		return err
	}
	outcome := t.sendBatchFrames(ctx.Context, batch)
	return outcome.report(ctx.Context, ctx.Node, batch)
}

type sendOutcome struct {
	snapshot bool
	err      error
}

func hasSnapshot(messages []raftpb.Message) bool {
	snapshot := false
	for _, m := range messages {
		snapshot = snapshot || m.Type == raftpb.MsgSnap
	}
	return snapshot
}

func (t *Transport) sendBatchFrames(ctx context.Context, batch load.RaftBatch) sendOutcome {
	outcome := sendOutcome{snapshot: hasSnapshot(batch.Messages)}
	frames, err := encodeRaftFrames(batch, t.options.MaxMessageBytes)
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.err = t.sendFrames(ctx, batch, frames, outcome)
	return outcome
}

func (t *Transport) sendFrames(ctx context.Context, batch load.RaftBatch, frames []*loadpb.RaftFrame,
	outcome sendOutcome) error {
	if !outcome.snapshot {
		return t.sendStream(ctx, batch, frames)
	}
	return t.sendUnary(ctx, batch.To.Node, frames)
}

func (o sendOutcome) deliveryKind() load.DeliveryKind {
	if !o.snapshot {
		return load.DeliveryUnreachable
	}
	if o.err == nil {
		return load.DeliverySnapshotSucceeded
	}
	return load.DeliverySnapshotFailed
}

func (o sendOutcome) report(ctx context.Context, node Ingress, batch load.RaftBatch) error {
	if !o.snapshot && o.err == nil {
		return nil
	}
	reportErr := node.ReportLoadDelivery(ctx,
		load.DeliveryReport{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID, Kind: o.deliveryKind()})
	if o.err == nil {
		return reportErr
	}
	return o.err
}

func (t *Transport) sendUnary(ctx context.Context, to topology.Identity, frames []*loadpb.RaftFrame) error {
	conn, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, frame := range frames {
		exchange := rpcExchange{method: loadpb.Load_SendRaft_FullMethodName, input: frame,
			limit: t.options.MaxMessageBytes}
		if err := invoke(ctx, conn.Conn, exchange); err != nil {
			return err
		}
	}
	return nil
}

// ReadReplica preserves diagnostic status and quorum-confirmed native evidence.
func (t *Transport) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	var result load.ReplicaStatus
	err := t.call(ctx, callRoute{from: q.From, to: q.To.Node},
		rpcExchange{method: loadpb.Load_ReadReplica_FullMethodName, input: q, output: &result})
	if err == nil && result.Peer != q.To {
		err = fault.ErrIdentityMismatch
	}
	return result, err
}

// ApplyAction carries the complete immutable grant and returns exact receipts.
func (t *Transport) ApplyAction(ctx context.Context, a load.ActionRequest) (load.ActionResult, error) {
	var result load.ActionResult
	err := t.call(ctx, callRoute{from: a.From, to: a.To.Node},
		rpcExchange{method: loadpb.Load_ApplyAction_FullMethodName, input: a, output: &result})
	if err == nil && (result.From != a.To || result.Authorization != a.Authorization ||
		result.LocalTarget != a.LocalTarget) {
		err = fault.ErrIdentityMismatch
	}
	return result, err
}

// InstallFence preserves the complete fence request and durable evidence.
func (t *Transport) InstallFence(ctx context.Context, f load.FenceRequest) (load.FenceResult, error) {
	var result load.FenceResult
	err := t.call(ctx, callRoute{from: f.From, to: f.To},
		rpcExchange{method: loadpb.Load_InstallFence_FullMethodName, input: f, output: &result})
	if err == nil && result.From != f.To {
		err = fault.ErrIdentityMismatch
	}
	return result, err
}
