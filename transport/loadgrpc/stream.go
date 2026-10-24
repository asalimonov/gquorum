package loadgrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Raft batches without snapshots travel on a long-lived stream per exact
// sender/recipient replica pair. The sender does not wait for each batch to be
// stepped, so a link is no longer limited to one batch per round trip. The
// recipient steps frames in order and answers only refusals, which the sender
// reports as unreachable deliveries. Snapshots keep the unary call, whose
// completion is reported explicitly.
const defaultStreamIdle = 2 * time.Minute

type streamKey struct{ from, to load.Peer }

type raftStream struct {
	key     streamKey
	address string
	stream  loadpb.Load_StreamRaftClient
	cancel  context.CancelFunc
	release func()
	mu      sync.Mutex // serializes Send; a gRPC stream allows one sender
	// lastUsed (Unix nanoseconds) selects idle streams.
	lastUsed atomic.Int64
	// lastDelivery is reported unreachable when the stream breaks, so Raft
	// resends promptly.
	lastDelivery    atomic.Uint64
	broken, retired atomic.Bool
}

// sendStream writes ordinary traffic without a unary downgrade.
// TODO(not planned): Header compression and cross-batch coalescing remain deferred.
func (t *Transport) sendStream(ctx context.Context, batch load.RaftBatch, frames []*loadpb.RaftFrame) error {
	for attempt := 0; ; attempt++ {
		s, err := t.raftStream(ctx, batch)
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.broken.Load() || s.retired.Load() {
			s.mu.Unlock()
			if attempt == 0 {
				continue // replaced while this caller waited; open a fresh stream once
			}
			return fault.New(fault.Unavailable, "load stream closed", nil)
		}
		err = s.sendFramesLocked(ctx, batch.DeliveryID, frames)
		s.mu.Unlock()
		if err != nil {
			s.broken.Store(true)
			s.cancel()
			return transportError(err)
		}
		return nil
	}
}

func (s *raftStream) sendFramesLocked(ctx context.Context, delivery uint64, frames []*loadpb.RaftFrame) error {
	// A send blocked by flow control past the caller's deadline breaks the
	// stream rather than leaving the caller waiting.
	var err error
	stop := context.AfterFunc(ctx, s.cancel)
	// Publish before Send: the receive goroutine can fail while Send is still
	// buffering the first frame. Its report must identify that delivery.
	s.lastDelivery.Store(delivery)
	for _, frame := range frames {
		if err = s.stream.Send(frame); err != nil {
			break
		}
	}
	stopped := stop()
	if !stopped && err == nil {
		err = ctx.Err()
	}
	if err == nil && s.broken.Load() {
		err = fault.New(fault.Unavailable, "load stream closed during send", nil)
	}
	if err == nil {
		s.lastUsed.Store(time.Now().UnixNano())
	}
	return err
}

// raftStream returns the open stream for the batch's route, opening one when
// needed.
func (t *Transport) raftStream(ctx context.Context, batch load.RaftBatch) (*raftStream, error) {
	key := streamKey{from: batch.From, to: batch.To}
	address, err := t.resolve(ctx, batch.To.Node)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, fault.ErrStopped
	}
	cached := t.streams[key]
	reusable := cached != nil && !cached.broken.Load()
	if reusable && cached.address == address {
		t.mu.Unlock()
		return cached, nil
	}

	t.mu.Unlock()
	s, err := t.openRaftStream(key, address)
	if err != nil {
		return nil, err
	}
	return t.publishRaftStream(s)
}

func (t *Transport) openRaftStream(key streamKey, address string) (*raftStream, error) {
	conn, err := t.connect(address)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(t.ctx)
	stream, err := loadpb.NewLoadClient(conn.Conn).StreamRaft(streamCtx)
	if err != nil {
		cancel()
		conn.Release()
		return nil, transportError(err)
	}
	s := &raftStream{key: key, address: address, stream: stream, cancel: cancel, release: conn.Release}
	s.lastUsed.Store(time.Now().UnixNano())
	return s, nil
}

func (t *Transport) publishRaftStream(s *raftStream) (*raftStream, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		s.cancel()
		s.release()
		return nil, fault.ErrStopped
	}
	if old := t.streams[s.key]; old != nil {
		broken := old.broken.Load()
		if !broken && old.address == s.address {
			t.mu.Unlock()
			s.cancel()
			s.release()
			return old, nil // another sender opened it first
		}
		old.retired.Store(true)
		old.cancel()
	}
	t.streams[s.key] = s
	t.wg.Add(1)
	if !t.janitor {
		t.janitor = true
		t.wg.Add(1)
		go t.retireIdleStreams()
	}
	t.mu.Unlock()
	go t.readRefusals(s)
	return s, nil
}

// readRefusals owns the stream's receive side and its connection reference.
func (t *Transport) readRefusals(s *raftStream) {
	defer t.wg.Done()
	defer s.release()
	defer s.cancel()
	for {
		refusal, err := s.stream.Recv()
		if err != nil {
			s.broken.Store(true)
			t.mu.Lock()
			if t.streams[s.key] == s {
				delete(t.streams, s.key)
			}

			t.mu.Unlock()
			if !s.retired.Load() && t.ctx.Err() == nil {
				// Frames written to a broken stream may be lost.
				t.reportUnreachable(s, s.lastDelivery.Load())
			}
			return
		}
		t.reportUnreachable(s, refusal.DeliveryId)
	}
}

func (t *Transport) reportUnreachable(s *raftStream, delivery uint64) {
	t.mu.Lock()
	node := t.node
	t.mu.Unlock()
	if node == nil || delivery == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(t.ctx, t.options.CallTimeout)
	defer cancel()
	_ = node.ReportLoadDelivery(ctx,
		load.DeliveryReport{From: s.key.from, To: s.key.to, DeliveryID: delivery, Kind: load.DeliveryUnreachable})
}

// retireIdleStreams closes streams of routes that stopped sending, such as a
// replaced replica's, and ends with the transport.
func (t *Transport) retireIdleStreams() {
	defer t.wg.Done()
	tick := time.NewTicker(t.streamIdle / 2)
	defer tick.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
		}
		cutoffNs := time.Now().Add(-t.streamIdle).UnixNano()
		t.mu.Lock()
		for key, s := range t.streams {
			if s.lastUsed.Load() < cutoffNs {
				delete(t.streams, key)
				s.retired.Store(true)
				s.cancel()
			}
		}
		t.mu.Unlock()
	}
}

// StreamRaft steps each frame in arrival order and replies only to refusals.
// Frames that cannot be attributed to a delivery end the stream.
func (s *serverAdapter) StreamRaft(stream loadpb.Load_StreamRaftServer) error {
	t := s.transport
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		refused, err := s.receiveStreamed(stream.Context(), request)
		if err != nil {
			return err
		}
		if refused == nil {
			continue
		}
		reply := &loadpb.StreamRefusal{
			DeliveryId:   request.DeliveryId,
			ErrorCode:    string(fault.Unavailable),
			ErrorMessage: refused.Error(),
		}
		var typed *fault.Error
		if errors.As(refused, &typed) {
			reply.ErrorCode, reply.ErrorMessage = string(typed.Code), typed.Message
		}
		if proto.Size(reply) > t.options.MaxMessageBytes {
			return status.Error(codes.ResourceExhausted, "Raft refusal exceeds limit")
		}
		if err := stream.Send(reply); err != nil {
			return err
		}
	}
}

// receiveStreamed applies the unary ingress checks to one frame. A returned
// error ends the stream; a refusal is reported for this delivery only.
func (s *serverAdapter) receiveStreamed(parent context.Context, frame *loadpb.RaftFrame) (refused, terminal error) {
	t := s.transport
	batch, err := decodeRaftHeader(frame, t.options.MaxMessageBytes)
	if err != nil {
		return nil, err
	}
	ctx, err := t.begin(parent)
	if errors.Is(err, fault.ErrOverloaded) {
		return err, nil
	}
	if err != nil {
		if parent.Err() != nil {
			return nil, status.FromContextError(parent.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "load transport is not serving")
	}
	defer ctx.Release()
	if err := t.authenticateSender(ctx.Context, batch.From.Node); err != nil {
		return nil, err
	}
	if batch.To.Node != ctx.Node.Identity() {
		return fault.ErrIdentityMismatch, nil
	}
	if err := decodeRaftMessages(ctx.Context, frame, &batch); err != nil {
		if errors.Is(err, fault.ErrIdentityMismatch) {
			return err, nil
		}
		return nil, raftReceiveError(err)
	}
	if err := ctx.Node.ReceiveLoadRaft(ctx.Context, batch); err != nil {
		if parent.Err() != nil {
			return nil, status.FromContextError(parent.Err()).Err()
		}
		return err, nil
	}
	return nil, nil
}
