package loadgrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const snapshotOpenTimeout = 3 * time.Second

type snapshotServerSession struct {
	handler              *snapshotHandler
	request              snapshot.BeginRequest
	session              snapshot.Session
	done                 <-chan struct{}
	deadline, progressAt time.Time
	progressTimeout      time.Duration
	next                 uint64
	completed            bool
	receiverRun          string
	chunkBytes           uint32
}

func (s *serverAdapter) TransferSnapshot(stream loadpb.Load_TransferSnapshotServer) error {
	h, err := s.transport.beginSnapshotHandler()
	if err != nil {
		return snapshotRPCError(err)
	}
	h.start(stream)
	defer h.finish()
	ctx, cancel := context.WithCancel(stream.Context())
	stop := context.AfterFunc(s.transport.ctx, cancel)
	defer func() { stop(); cancel() }()
	if h.service == nil {
		return status.Error(codes.Unimplemented, "snapshot service unavailable")
	}
	return s.serveSnapshot(ctx, h)
}

func (s *serverAdapter) serveSnapshot(ctx context.Context, h *snapshotHandler) error {
	pre, cancel := context.WithTimeout(ctx, min(snapshotOpenTimeout, h.options.ProgressTimeout, h.options.TotalTimeout))
	request, err := s.awaitSnapshotOpen(pre, h)
	cancel()
	if err != nil {
		return snapshotRPCError(err)
	}
	if err = h.authenticatePermit(); err != nil {
		return snapshotRPCError(err)
	}
	now := time.Now()
	session := &snapshotServerSession{handler: h, request: request,
		deadline:   now.Add(min(request.RemainingTotal, h.options.TotalTimeout)),
		progressAt: now, progressTimeout: min(request.ProgressTimeout, h.options.ProgressTimeout)}
	bounded, release := context.WithDeadline(ctx, session.deadline)
	defer release()
	ack, err := h.service.Begin(bounded, request)
	if err != nil {
		ack.Session.Key = request.Manifest.Key
		return snapshotRPCError(h.writeAck(bounded, nil, ack, err))
	}
	session.session = ack.Session
	if ack.State != snapshot.DeliveryNotNeeded {
		// Detach is exact-generation and runs only after the active mutation returned.
		defer func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx),
				min(snapshotOpenTimeout, h.options.ProgressTimeout))
			defer cancel()
			_ = h.service.Detach(cleanup, ack.Session)
		}()
	}
	if err = session.observeAck(ack); err != nil {
		return snapshotRPCError(err)
	}
	if ack.State != snapshot.DeliveryNotNeeded && ack.State != snapshot.DeliveryAccepted {
		session.done = h.service.SessionDone(ack.Session)
	}
	if err = h.writeAck(bounded, session.done, ack, nil); err != nil {
		return snapshotRPCError(err)
	}
	if ack.State == snapshot.DeliveryNotNeeded || ack.State == snapshot.DeliveryAccepted {
		return nil
	}
	return snapshotRPCError(session.run(bounded))
}
func (s *serverAdapter) awaitSnapshotOpen(ctx context.Context, h *snapshotHandler) (snapshot.BeginRequest, error) {
	select {
	case <-ctx.Done():
		return snapshot.BeginRequest{}, ctx.Err()
	case received := <-h.frames:
		if received.err != nil {
			return snapshot.BeginRequest{}, received.err
		}
		if received.frame == nil || received.frame.GetOpen() == nil {
			return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonMalformed, nil)
		}
		return s.receiveSnapshotOpen(ctx, received.frame.GetOpen())
	}
}
func (s *serverAdapter) receiveSnapshotOpen(
	ctx context.Context, open *loadpb.SnapshotOpen,
) (snapshot.BeginRequest, error) {
	t := s.transport
	req, err := decodeSnapshotOpen(open, t.options.MaxMessageBytes)
	if err != nil {
		return snapshot.BeginRequest{}, err
	}
	started := time.Now()
	authCtx, cancel := context.WithTimeout(ctx, min(req.RemainingTotal, req.ProgressTimeout))
	defer cancel()
	if err = t.authenticateSender(authCtx, req.Manifest.Key.From.Node); err != nil {
		return snapshot.BeginRequest{}, err
	}
	t.mu.Lock()
	node := t.node
	t.mu.Unlock()
	if req.Manifest.Key.To.Node != node.Identity() {
		return snapshot.BeginRequest{}, fault.ErrIdentityMismatch
	}
	req.RemainingTotal -= time.Since(started)
	if req.RemainingTotal <= 0 {
		return snapshot.BeginRequest{}, context.DeadlineExceeded
	}
	capacity, err := snapshotChunkCapacity(t.options.MaxMessageBytes, t.options.MaxMessageBytes)
	if err != nil {
		return snapshot.BeginRequest{}, err
	}
	if capacity <= 0 || uint64(capacity) > math.MaxUint32 {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	req.MaxChunkBytes = uint32(capacity)
	return req, nil
}
func (ss *snapshotServerSession) run(ctx context.Context) error {
	for {
		due := ss.deadline
		if ss.next < ss.request.Manifest.TotalBytes {
			due = minSnapshotTime(due, ss.progressAt.Add(ss.progressTimeout))
		}
		phase, cancel := context.WithDeadline(ctx, due)
		err := ss.receiveFrame(phase)
		cancel()
		if err != nil {
			return err
		}
		if ss.completed {
			return nil
		}
	}
}
func (ss *snapshotServerSession) receiveFrame(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ss.done:
		return context.Canceled
	case r := <-ss.handler.frames:
		if r.err != nil {
			return r.err
		}
		select {
		case <-ss.done:
			return context.Canceled
		default:
		}
		return ss.handleFrame(ctx, r.frame)
	}
}
func (ss *snapshotServerSession) handleFrame(ctx context.Context, frame *loadpb.SnapshotFrame) error {
	result, err := ss.dispatch(ctx, frame)
	ack := result.ack
	if err != nil {
		ack.Session = ss.session
	} else {
		err = ss.observeAck(ack)
	}
	// Native cleanup can close SessionDone after successful Finish. This exact
	// final reply still observes RPC/total cancellation, without another mutation.
	done := ss.done
	if result.terminal {
		done = nil
	}
	if writeErr := ss.handler.writeAck(ctx, done, ack, err); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return err
	}
	ss.completed = result.terminal
	return nil
}

type snapshotDispatched struct {
	ack      snapshot.Ack
	terminal bool
}

func (ss *snapshotServerSession) dispatch(
	ctx context.Context, frame *loadpb.SnapshotFrame,
) (snapshotDispatched, error) {
	if frame == nil || proto.Size(frame) > ss.handler.t.options.MaxMessageBytes {
		return snapshotDispatched{}, snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	service := ss.handler.service
	switch body := frame.Body.(type) {
	case *loadpb.SnapshotFrame_Data:
		d := body.Data
		if d == nil || d.Attachment != ss.session.Attachment || len(d.Sha256) != sha256.Size ||
			uint64(len(d.Data)) > uint64(ss.chunkBytes) {
			break
		}
		chunk := snapshot.Chunk{Session: ss.session, Ordinal: d.Ordinal, Offset: d.Offset, Data: d.Data}
		copy(chunk.SHA256[:], d.Sha256)
		ack, err := service.Write(ctx, chunk)
		return snapshotDispatched{ack: ack}, err
	case *loadpb.SnapshotFrame_Finish:
		if body.Finish == nil || body.Finish.Attachment != ss.session.Attachment {
			break
		}
		ack, err := service.Finish(ctx, ss.session)
		return snapshotDispatched{ack: ack, terminal: true}, err
	case *loadpb.SnapshotFrame_Abort:
		if body.Abort == nil || body.Abort.Attachment != ss.session.Attachment {
			break
		}
		err := service.Abort(ctx, ss.session)
		if err == nil {
			err = snapshotWireError(snapshot.ReasonStopped, nil)
		}
		return snapshotDispatched{terminal: true}, err
	}
	return snapshotDispatched{}, snapshotWireError(snapshot.ReasonMalformed, nil)
}
func (ss *snapshotServerSession) observeAck(ack snapshot.Ack) error {
	if ack.Session.Key != ss.request.Manifest.Key || ack.NextOffset < ss.next ||
		ack.NextOffset > ss.request.Manifest.TotalBytes ||
		ack.RemainingTotal <= 0 || ack.ProgressTimeout <= 0 {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if ss.receiverRun != "" && (ack.ReceiverRunID != ss.receiverRun ||
		ack.ChunkBytes != ss.chunkBytes || ack.Session != ss.session) {
		return snapshotWireError(snapshot.ReasonRestartRequired, nil)
	}
	ss.receiverRun, ss.chunkBytes = ack.ReceiverRunID, ack.ChunkBytes
	if ack.ChunkBytes > ss.request.MaxChunkBytes {
		return snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	now := time.Now()
	ss.deadline = minSnapshotTime(ss.deadline, now.Add(ack.RemainingTotal))
	ss.progressTimeout = min(ss.progressTimeout, ack.ProgressTimeout)
	if ack.NextOffset > ss.next {
		ss.progressAt = now
	}
	ss.next = ack.NextOffset
	return nil
}
func minSnapshotTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
func snapshotRPCError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return status.FromContextError(err).Err()
	}
	code := codes.Unavailable
	var classified *fault.Error
	if errors.As(err, &classified) {
		switch classified.Code {
		case fault.Overloaded:
			code = codes.ResourceExhausted
		case fault.Invalid:
			code = codes.InvalidArgument
		case fault.IdentityMismatch:
			code = codes.PermissionDenied
		case fault.Unsupported:
			code = codes.Unimplemented
		default:
		}
	}
	return status.Error(code, snapshotBoundedText(err.Error()))
}
