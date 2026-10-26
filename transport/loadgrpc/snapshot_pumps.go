package loadgrpc

import (
	"context"
	"reflect"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/protobuf/proto"
)

const snapshotPreOpenMinimum = 8

type snapshotReceived struct {
	frame *loadpb.SnapshotFrame
	err   error
}
type snapshotReply struct {
	ack    *loadpb.SnapshotAck
	result chan error
}

// One lifetime owns its handler, pumps and quota until their actual returns.
// Handler return must precede a join of blocked gRPC Recv/Send calls.
type snapshotHandler struct {
	t             *Transport
	service       snapshot.Service
	options       snapshot.Options
	authenticated bool
	stop          chan struct{}
	returned      chan struct{}
	frames        chan snapshotReceived
	replies       chan snapshotReply
	pumps         sync.WaitGroup
}

func (t *Transport) beginSnapshotHandler() (*snapshotHandler, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fault.ErrStopped
	}
	if t.node == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if t.snapshotPreOpen >= t.snapshotPreLimit {
		return nil, fault.ErrOverloaded
	}
	t.snapshotPreOpen++
	t.wg.Add(1)
	return &snapshotHandler{t: t, service: t.snapshotService, options: t.snapshotOptions,
		stop: make(chan struct{}), returned: make(chan struct{}),
		frames: make(chan snapshotReceived, 1), replies: make(chan snapshotReply, 1)}, nil
}

func (h *snapshotHandler) authenticatePermit() error {
	h.t.mu.Lock()
	defer h.t.mu.Unlock()
	if h.t.closed {
		return fault.ErrStopped
	}
	if h.t.snapshotHandlers >= 2*h.options.MaxIncoming {
		return fault.ErrOverloaded
	}
	h.t.snapshotHandlers++
	h.t.snapshotPreOpen--
	h.authenticated = true
	return nil
}
func (h *snapshotHandler) release() {
	h.t.mu.Lock()
	if h.authenticated {
		h.t.snapshotHandlers--
	} else {
		h.t.snapshotPreOpen--
	}
	h.t.mu.Unlock()
	h.t.wg.Done()
}
func (h *snapshotHandler) start(stream loadpb.Load_TransferSnapshotServer) {
	h.pumps.Add(2)
	go func() { defer h.pumps.Done(); h.receive(stream) }()
	go func() { defer h.pumps.Done(); h.send(stream) }()
	go func() { <-h.returned; h.pumps.Wait(); h.release() }()
}
func (h *snapshotHandler) finish() { close(h.stop); close(h.returned) }
func (h *snapshotHandler) receive(stream loadpb.Load_TransferSnapshotServer) {
	for {
		frame, err := stream.Recv()
		select {
		case h.frames <- snapshotReceived{frame, err}:
		case <-h.stop:
			return
		}
		if err != nil {
			return
		}
		select {
		case <-h.stop:
			return
		default:
		}
	}
}
func (h *snapshotHandler) send(stream loadpb.Load_TransferSnapshotServer) {
	for {
		select {
		case <-h.stop:
			return
		case reply := <-h.replies:
			select {
			case <-h.stop:
				return
			default:
			}
			err := stream.Send(reply.ack)
			reply.result <- err
			if err != nil {
				return
			}
		}
	}
}
func (h *snapshotHandler) writeAck(ctx context.Context, done <-chan struct{}, ack snapshot.Ack, cause error) error {
	reply, err := encodeSnapshotAck(ack, cause)
	if err != nil {
		return err
	}
	if size := proto.Size(reply); size > h.t.options.MaxMessageBytes {
		return snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	request := snapshotReply{ack: reply, result: make(chan error, 1)}
	select {
	case h.replies <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return context.Canceled
	}
	select {
	case err = <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return context.Canceled
	}
}

func nilSnapshotService(service snapshot.Service) bool {
	if service == nil {
		return true
	}
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
