//go:build functional

package loadgrpc

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBinaryIngressValidationOrder(t *testing.T) {
	t.Parallel()
	for _, streaming := range []bool{false, true} {
		name := "unary"
		if streaming {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, receiver, local, remote := newProtocolPair(t, 4096)
			var calls, auth atomic.Int32
			remote.receive = func(context.Context, load.RaftBatch) error { calls.Add(1); return nil }
			adapter := &serverAdapter{transport: receiver}
			frames, err := encodeRaftFrames(appendBatch(local, remote, 7), 4096)
			if err != nil {
				t.Fatal(err)
			}
			frame := frames[0]
			valid := frame.Messages[0]
			invoke := func() error {
				if streaming {
					refused, terminal := adapter.receiveStreamed(context.Background(), frame)
					if terminal != nil {
						return terminal
					}
					return refused
				}
				reply, err := adapter.SendRaft(context.Background(), frame)
				if err != nil {
					return err
				}
				return decodeReply(reply, nil, 4096)
			}
			// Authentication wins over corrupt native bytes, after a valid outer header.
			receiver.options.Authenticate = func(context.Context, topology.Identity) error { auth.Add(1); return errors.New("bad credential") }
			frame.Messages[0] = []byte{0xff}
			if err := invoke(); status.Code(err) != codes.Unauthenticated || auth.Load() != 1 || calls.Load() != 0 {
				t.Fatal("native decode before authentication", err)
			}
			receiver.options.Authenticate = func(context.Context, topology.Identity) error { auth.Add(1); return nil }
			for _, bad := range [][]byte{nil, {0xff}} {
				frame.Messages[0] = bad
				if err := invoke(); status.Code(err) != codes.InvalidArgument || calls.Load() != 0 {
					t.Fatal("invalid native ingress", err)
				}
			}
			wrong := raftpb.Message{From: 1, To: 3}
			frame.Messages[0], err = wrong.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if err := invoke(); !errors.Is(err, fault.ErrIdentityMismatch) || calls.Load() != 0 {
				t.Fatal("native identity refusal", err)
			}
			frame.Messages[0] = valid
			if err := invoke(); err != nil || calls.Load() != 1 {
				t.Fatal("valid frame rejected", err)
			}
			// Local identity is re-read for each frame, never cached at stream open.
			remote.id.InstanceID = "replacement"
			if err := invoke(); !errors.Is(err, fault.ErrIdentityMismatch) || calls.Load() != 1 {
				t.Fatal("stale recipient admitted", err)
			}
			frame.To.InstanceId = remote.id.InstanceID
			if err := invoke(); err != nil || calls.Load() != 2 {
				t.Fatal("current recipient refused", err)
			}
			receiver.options.Authenticate = func(ctx context.Context, _ topology.Identity) error { <-ctx.Done(); return ctx.Err() }
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if streaming {
				_, err = adapter.receiveStreamed(ctx, frame)
			} else {
				_, err = adapter.SendRaft(ctx, frame)
			}
			if status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation misclassified", err)
			}
		})
	}
}

func TestOversizedStreamRefusalTerminates(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 4096)
	remote.receive = func(context.Context, load.RaftBatch) error {
		return fault.New(fault.Unavailable, strings.Repeat("x", 8192), nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := sender.client(ctx, remote.id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	stream, err := loadpb.NewLoadClient(conn.Conn).StreamRaft(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := encodeRaftFrames(appendBatch(local, remote, 91), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(frames[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized refusal escaped bound", err)
	}
}

type blockedRaftSend struct {
	grpc.ClientStream
	ctx     context.Context
	entered chan struct{}
}

func (s *blockedRaftSend) Send(*loadpb.RaftFrame) error {
	close(s.entered)
	<-s.ctx.Done()
	return s.ctx.Err()
}
func (*blockedRaftSend) Recv() (*loadpb.StreamRefusal, error) {
	return nil, errors.New("unexpected receive")
}

func TestBlockedRaftSendCanceledAndJoined(t *testing.T) {
	t.Parallel()
	streamCtx, breakStream := context.WithCancel(context.Background())
	defer breakStream()
	blocked := &blockedRaftSend{ctx: streamCtx, entered: make(chan struct{})}
	s := &raftStream{stream: blocked, cancel: breakStream}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.sendFramesLocked(ctx, 37, []*loadpb.RaftFrame{{Version: raftWireVersion}}) }()
	<-blocked.entered
	if s.lastDelivery.Load() != 37 {
		t.Error("first blocked send has no delivery identity")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked send did not join")
	}
}
