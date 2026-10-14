package loadgrpc

import (
	"context"
	"errors"

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
	frames, err := splitBatch(batch, t.options.MaxMessageBytes)
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.err = t.sendFrames(ctx, batch, frames, outcome)
	return outcome
}

func (t *Transport) sendFrames(ctx context.Context, batch load.RaftBatch, frames []*loadpb.Envelope,
	outcome sendOutcome) error {
	streamed := false
	var err error
	if !outcome.snapshot {
		streamed, err = t.sendStream(ctx, batch, frames)
	}
	if !streamed {
		err = t.sendUnary(ctx, batch.To.Node, frames)
	}
	return err
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

func (t *Transport) sendUnary(ctx context.Context, to topology.Identity, frames []*loadpb.Envelope) error {
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

func splitBatch(batch load.RaftBatch, limit int) ([]*loadpb.Envelope, error) {
	if err := validateBatch(batch); err != nil {
		return nil, err
	}
	return splitValidBatch(batch, limit)
}

func validateBatch(batch load.RaftBatch) error {
	if len(batch.Messages) == 0 || batch.DeliveryID == 0 || batch.From.Replica.ReplicaID == 0 ||
		batch.To.Replica.ReplicaID == 0 || batch.From.Replica.GroupID != batch.To.Replica.GroupID ||
		topology.ValidateLoadID(batch.To.Replica.GroupID) != nil {
		return fault.ErrInvalid
	}
	for _, msg := range batch.Messages {
		if msg.From != batch.From.Replica.ReplicaID || msg.To != batch.To.Replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
	}
	return nil
}

func splitValidBatch(batch load.RaftBatch, limit int) ([]*loadpb.Envelope, error) {
	var frames []*loadpb.Envelope
	for start := 0; start < len(batch.Messages); {
		chunk, err := findBatchChunk(batch, start, limit)
		if err != nil {
			return nil, err
		}
		frames = append(frames, chunk.frame)
		start += chunk.count
	}
	return frames, nil
}

type batchChunk struct {
	frame *loadpb.Envelope
	count int
}

type batchSearch struct {
	lo, hi int
	best   batchChunk
}

// Preflight every message before sending. Binary search keeps the original
// order and never truncates an oversized message.
func findBatchChunk(batch load.RaftBatch, start, limit int) (batchChunk, error) {
	search := batchSearch{lo: 1, hi: len(batch.Messages) - start}
	for search.lo <= search.hi {
		if err := search.try(batch, start, limit); err != nil {
			return batchChunk{}, err
		}
	}
	if search.best.frame == nil {
		return batchChunk{}, fault.New(fault.Overloaded, "single Raft message exceeds load frame limit", nil)
	}
	return search.best, nil
}

func encodeBatchChunk(batch load.RaftBatch, start, count, limit int) (batchChunk, error) {
	part := batch
	part.Messages = batch.Messages[start : start+count]
	frame, err := envelope(part, limit, limit)
	return batchChunk{frame: frame, count: count}, err
}

func (s *batchSearch) try(batch load.RaftBatch, start, limit int) error {
	count := s.lo + (s.hi-s.lo)/2
	chunk, err := encodeBatchChunk(batch, start, count, limit)
	if err == nil {
		s.best = chunk
		s.lo = count + 1
	} else if errors.Is(err, fault.ErrOverloaded) {
		s.hi = count - 1
	} else {
		return err
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
