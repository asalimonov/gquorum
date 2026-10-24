//go:build functional

package metatransport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestMetaTransportQueuedFramesShareRoundTrip(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var mu sync.Mutex
	streams := make(map[context.Context]bool)
	received := make(chan raftpb.Message, 32)
	remote := testTransport(t, 2, &testHandler{stepHook: func(ctx context.Context, msg raftpb.Message) error {
		mu.Lock()
		streams[ctx] = true
		mu.Unlock()
		if msg.Index == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		received <- msg
		return nil
	}}, nowhere, nil)
	local := testTransport(t, 1, &testHandler{}, func(context.Context, uint64) (string, error) {
		return remote.opts.Listener.Addr().String(), nil
	}, func(o *Options) { o.SendTimeout = 2 * time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := local.Send(ctx, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Index: 1}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var queued []raftpb.Message
	for index := uint64(2); index <= 21; index++ {
		queued = append(queued, raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Index: index})
	}
	if err := local.Send(ctx, queued); err != nil {
		t.Fatal(err)
	}
	unblock()
	for index := uint64(1); index <= 21; index++ {
		if msg := recvMessage(t, received); msg.Index != index {
			t.Fatalf("message order: got %d want %d", msg.Index, index)
		}
	}
	mu.Lock()
	count := len(streams)
	mu.Unlock()
	if count != 2 {
		t.Fatalf("queued messages consumed %d RPC round trips, want first + one queued batch", count)
	}
}

func TestMetaTransportBatchKeepsSnapshotAckBeforeFailedSuffix(t *testing.T) {
	t.Parallel()
	received := make(chan raftpb.Message, 8)
	remote := testTransport(t, 2, &testHandler{stepHook: func(_ context.Context, msg raftpb.Message) error {
		received <- msg
		if msg.Index == 4 {
			return fault.ErrInvalid
		}
		return nil
	}}, nowhere, nil)
	reports := make(chan raft.SnapshotStatus, 8)
	local := testTransport(t, 1, &testHandler{}, func(context.Context, uint64) (string, error) {
		return remote.opts.Listener.Addr().String(), nil
	}, func(o *Options) {
		o.SendTimeout = time.Second
		o.OnSnapshot = func(_ uint64, status raft.SnapshotStatus) { reports <- status }
	})
	messages := []raftpb.Message{
		{Type: raftpb.MsgApp, From: 1, To: 2, Index: 1},
		{Type: raftpb.MsgApp, From: 1, To: 2, Index: 2},
		{Type: raftpb.MsgSnap, From: 1, To: 2, Index: 3, Snapshot: &raftpb.Snapshot{
			Data: []byte("complete-state"), Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 1,
				ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}},
		}},
		{Type: raftpb.MsgApp, From: 1, To: 2, Index: 4},
		{Type: raftpb.MsgApp, From: 1, To: 2, Index: 5},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var connection peerConnection
	defer connection.close()
	unsent, err := local.sendPeerMessages(&peer{id: 2, ctx: ctx}, &connection, messages)
	if !errors.Is(err, fault.ErrInvalid) || len(unsent) != 2 || unsent[0].Index != 4 || unsent[1].Index != 5 {
		t.Fatalf("wrong failed suffix: %v %v", unsent, err)
	}
	for _, msg := range unsent {
		local.fail(msg)
	}
	for index := uint64(1); index <= 4; index++ {
		if msg := recvMessage(t, received); msg.Index != index {
			t.Fatalf("snapshot crossed message order: got %d want %d", msg.Index, index)
		}
	}
	select {
	case msg := <-received:
		t.Fatal("frame after refusal was applied", msg.Index)
	default:
	}
	if len(reports) != 1 || <-reports != raft.SnapshotFinish {
		t.Fatal("acknowledged snapshot lost or reported failed")
	}
}
