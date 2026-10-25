package loadgrpc

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotReconnectInitial = 100 * time.Millisecond
	snapshotReconnectMaximum = 2 * time.Second
	snapshotJitterDivisor    = 4
)

type snapshotClientState struct {
	t                              *Transport
	send                           snapshot.Send
	options                        snapshot.Options
	mu                             sync.Mutex
	deadline, lastProgress         time.Time
	progressTimeout                time.Duration
	ack                            snapshot.Ack
	receiverRun                    string
	chunk                          uint32
	resume, admitted, finishIssued bool
	highestSent, attemptSent       uint64
	stream                         loadpb.Load_TransferSnapshotClient
	wake                           chan struct{}
	clockWake                      chan struct{}
}

var _ snapshot.Transport = (*Transport)(nil)

// SendSnapshot transfers immutable encoded bytes on an independent bulk connection.
// It joins all attempt workers before returning and never reports delivery itself.
func (t *Transport) SendSnapshot(parent context.Context, send snapshot.Send) (snapshot.Ack, error) {
	state, err := t.prepareSnapshotClient(parent, send)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer t.releaseSnapshotSend()
	ctx, cancel := context.WithCancelCause(parent)
	stopAdapter := context.AfterFunc(t.ctx, func() { cancel(fault.ErrStopped) })
	clockDone := make(chan struct{})
	go func() { defer close(clockDone); state.monitor(ctx, cancel) }()
	defer func() { cancel(nil); stopAdapter(); <-clockDone }()
	delay := snapshotReconnectInitial
	for {
		ack, err := t.sendSnapshotAttempt(ctx, state)
		if ctx.Err() != nil {
			return snapshot.Ack{}, context.Cause(ctx)
		}
		if err == nil {
			return ack, nil
		}
		err = classifySnapshotRPCError(err)
		if !retrySnapshotError(err) {
			return snapshot.Ack{}, err
		}
		//nolint:gosec // Retry jitter is scheduling, not security-sensitive randomness.
		timer := time.NewTimer(delay - time.Duration(rand.Int64N(int64(delay/snapshotJitterDivisor))))
		select {
		case <-ctx.Done():
			timer.Stop()
			return snapshot.Ack{}, context.Cause(ctx)
		case <-timer.C:
		}
		delay = min(snapshotReconnectMaximum, 2*delay)
		state.resume = true
	}
}
func (t *Transport) prepareSnapshotClient(parent context.Context, send snapshot.Send) (*snapshotClientState, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if err := validateSnapshotManifest(send.Manifest); err != nil {
		return nil, err
	}
	if uint64(len(send.Data)) != send.Manifest.TotalBytes {
		return nil, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	t.mu.Lock()
	node, closed := t.node, t.closed
	t.mu.Unlock()
	if closed {
		return nil, fault.ErrStopped
	}
	if node == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if err := checkRoute(node.Identity(), send.Manifest.Key.From.Node, send.Manifest.Key.To.Node); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fault.ErrStopped
	}
	if send.Manifest.TotalBytes > t.snapshotOptions.MaxSnapshotBytes {
		return nil, snapshotWireLimit(snapshot.ReasonSizeLimit,
			send.Manifest.TotalBytes, t.snapshotOptions.MaxSnapshotBytes)
	}
	if t.snapshotSending >= t.snapshotOptions.MaxOutgoing {
		return nil, snapshotWireError(snapshot.ReasonCapacity, nil)
	}
	now := time.Now()
	state := &snapshotClientState{t: t, send: send, options: t.snapshotOptions,
		deadline:     now.Add(t.snapshotOptions.TotalTimeout),
		lastProgress: now, progressTimeout: t.snapshotOptions.ProgressTimeout,
		wake: make(chan struct{}, 1), clockWake: make(chan struct{}, 1)}
	if due, ok := parent.Deadline(); ok {
		state.deadline = minSnapshotTime(state.deadline, due)
	}
	open, err := encodeSnapshotOpen(send, time.Until(state.deadline), t.snapshotOptions.ProgressTimeout, false)
	if err != nil {
		return nil, err
	}
	if proto.Size(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: open}}) > t.options.MaxMessageBytes {
		return nil, snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	t.snapshotSending++
	t.wg.Add(1)
	return state, nil
}
func (t *Transport) releaseSnapshotSend() {
	t.mu.Lock()
	t.snapshotSending--
	t.mu.Unlock()
	t.wg.Done()
}

//revive:disable-next-line:function-result-limit The target requires a stream and an explicit owned release.
func (t *Transport) openSnapshotStream(
	ctx context.Context, to load.Peer,
) (loadpb.Load_TransferSnapshotClient, func(), error) {
	ref, err := t.snapshotClient(ctx, to.Node)
	if err != nil {
		return nil, nil, err
	}
	child, cancel := context.WithCancel(ctx)
	stream, err := loadpb.NewLoadClient(ref.Conn).TransferSnapshot(child)
	if err != nil {
		cancel()
		ref.Release()
		return nil, nil, err
	}
	return stream, func() { cancel(); ref.Release() }, nil
}
func (t *Transport) sendSnapshotAttempt(ctx context.Context, state *snapshotClientState) (snapshot.Ack, error) {
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, release, err := t.openSnapshotStream(attempt, state.send.Manifest.Key.To)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer release()
	state.stream = stream
	state.mu.Lock()
	remaining, progress := time.Until(state.deadline), state.progressTimeout
	timeoutErr := state.timeoutLocked(time.Now())
	state.mu.Unlock()
	if timeoutErr != nil {
		return snapshot.Ack{}, timeoutErr
	}
	open, err := encodeSnapshotOpen(state.send, remaining, progress, state.resume)
	if err != nil {
		return snapshot.Ack{}, err
	}
	if err = stream.Send(&loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: open}}); err != nil {
		return snapshot.Ack{}, err
	}
	reply, err := stream.Recv()
	if err != nil {
		return snapshot.Ack{}, err
	}
	ack, err := state.observe(reply, snapshotAckOpening)
	if err != nil {
		return ack, err
	}
	if ack.State == snapshot.DeliveryAccepted || ack.State == snapshot.DeliveryNotNeeded {
		return ack, nil
	}
	state.mu.Lock()
	state.attemptSent = ack.NextOffset
	state.mu.Unlock()
	sending, receiving := make(chan error, 1), make(chan error, 1)
	go func() { sending <- state.sendChunks(attempt) }()
	go func() { receiving <- state.receiveAcks(attempt) }()
	select {
	case err = <-sending:
		if err != nil {
			cancel()
			<-receiving
		} else {
			err = <-receiving
		}
	case err = <-receiving:
		cancel()
		<-sending
	}
	// Both workers returned before the stream/connection may be replaced.
	state.mu.Lock()
	ack = state.ack
	state.mu.Unlock()
	return ack, err
}
func (s *snapshotClientState) receiveAcks(ctx context.Context) error {
	for {
		reply, err := s.stream.Recv()
		if err != nil {
			return err
		}
		ack, err := s.observe(reply, snapshotAckStreaming)
		if err != nil {
			return err
		}
		if ack.State == snapshot.DeliveryAccepted {
			return nil
		}
		if err = ctx.Err(); err != nil {
			return err
		}
	}
}
func classifySnapshotRPCError(err error) error {
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		return err
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		return snapshotWireError(snapshot.ReasonUnsupported, err)
	case codes.ResourceExhausted:
		if strings.Contains(status.Convert(err).Message(), "message larger than max") {
			return snapshotWireError(snapshot.ReasonFrameLimit, err)
		}
		return snapshotWireError(snapshot.ReasonCapacity, err)
	case codes.InvalidArgument:
		return snapshotWireError(snapshot.ReasonMalformed, err)
	case codes.Unauthenticated, codes.PermissionDenied:
		return snapshotWireError(snapshot.ReasonIdentity, err)
	default:
		return err
	}
}
func retrySnapshotError(err error) bool {
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		return false
	}
	var classified *fault.Error
	if errors.As(err, &classified) {
		return classified.Code == fault.Unavailable
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
