//go:build functional

package loadgrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func appendBatch(local, remote *ingressDouble, delivery uint64) load.RaftBatch {
	return load.RaftBatch{From: replicaPeer(local.id, 1), To: replicaPeer(remote.id, 2), DeliveryID: delivery,
		Messages: []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Index: delivery}}}
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStreamDeliversInOrderWithoutWaitingForSteps(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 4096)
	release := make(chan struct{})
	var mu sync.Mutex
	var got []uint64
	remote.receive = func(_ context.Context, b load.RaftBatch) error {
		if b.DeliveryID == 1 {
			<-release // the recipient is still stepping the first batch
		}
		mu.Lock()
		defer mu.Unlock()
		got = append(got, b.DeliveryID)
		return nil
	}
	local.report = func(context.Context, load.DeliveryReport) error {
		t.Error("accepted batch reported")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for id := uint64(1); id <= 5; id++ {
		if err := sender.SendRaft(ctx, appendBatch(local, remote, id)); err != nil {
			t.Fatal(id, err)
		}
	}
	close(release)
	waitFor(t, "all batches", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 5 })
	mu.Lock()
	defer mu.Unlock()
	for i, id := range got {
		if id != uint64(i+1) {
			t.Fatal("stream reordered batches", got)
		}
	}
}

func TestStreamRefusalIsReportedAsUnreachableDelivery(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 4096)
	remote.receive = func(_ context.Context, b load.RaftBatch) error {
		if b.DeliveryID == 2 {
			return fault.ErrReplicaNotReady
		}
		return nil
	}
	reports := make(chan load.DeliveryReport, 4)
	local.report = func(_ context.Context, r load.DeliveryReport) error { reports <- r; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for id := uint64(1); id <= 3; id++ {
		if err := sender.SendRaft(ctx, appendBatch(local, remote, id)); err != nil {
			t.Fatal("streamed send waited for the refusal", id, err)
		}
	}
	select {
	case r := <-reports:
		want := load.DeliveryReport{From: replicaPeer(local.id, 1), To: replicaPeer(remote.id, 2), DeliveryID: 2, Kind: load.DeliveryUnreachable}
		if r != want {
			t.Fatal("wrong refusal report", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refusal was not reported")
	}
	if err := sender.SendRaft(ctx, appendBatch(local, remote, 4)); err != nil {
		t.Fatal("a refusal broke the stream", err)
	}
	select {
	case r := <-reports:
		t.Fatal("accepted batch reported", r)
	case <-time.After(50 * time.Millisecond):
	}
}

// missingStreamServer represents an incompatible recipient without the method.
type missingStreamServer struct {
	*serverAdapter
	unary atomic.Int32
}

func (*missingStreamServer) StreamRaft(loadpb.Load_StreamRaftServer) error {
	return status.Error(codes.Unimplemented, "method StreamRaft not implemented")
}

func (s *missingStreamServer) SendRaft(ctx context.Context, frame *loadpb.RaftFrame) (*loadpb.Reply, error) {
	s.unary.Add(1)
	return s.serverAdapter.SendRaft(ctx, frame)
}

func TestMissingStreamDoesNotFallBack(t *testing.T) {
	t.Parallel()
	var server *missingStreamServer
	sender, _, local, remote := newProtocolPairWith(t, 4096, func(s *grpc.Server, receiver *Transport) {
		server = &missingStreamServer{serverAdapter: &serverAdapter{transport: receiver}}
		loadpb.RegisterLoadServer(s, server)
	})
	var received atomic.Int32
	remote.receive = func(context.Context, load.RaftBatch) error { received.Add(1); return nil }
	reports := make(chan load.DeliveryReport, 16)
	local.report = func(_ context.Context, r load.DeliveryReport) error { reports <- r; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for id := uint64(1); id <= 2; id++ {
		batch := appendBatch(local, remote, id)
		// The error can arrive asynchronously after Send buffered the frame.
		_ = sender.SendRaft(ctx, batch)
		for {
			select {
			case r := <-reports:
				if r.From != batch.From || r.To != batch.To || r.Kind != load.DeliveryUnreachable {
					t.Fatal("incorrect failure identity", r)
				}
				if r.DeliveryID == id {
					goto reported
				}
			case <-ctx.Done():
				t.Fatal("missing stream failure was not reported", ctx.Err())
			}
		}
	reported:
	}
	if err := sender.Close(ctx); err != nil {
		t.Fatal("stream was not joined", err)
	}
	if server.unary.Load() != 0 || received.Load() != 0 {
		t.Fatal("incompatible stream fell back to unary")
	}
}

func TestIdleStreamsAreRetired(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPair(t, 4096)
	sender.mu.Lock()
	sender.streamIdle = 40 * time.Millisecond
	sender.mu.Unlock()
	var mu sync.Mutex
	received := 0
	remote.receive = func(context.Context, load.RaftBatch) error { mu.Lock(); received++; mu.Unlock(); return nil }
	local.report = func(_ context.Context, r load.DeliveryReport) error {
		t.Error("retired idle stream reported a loss", r)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sender.SendRaft(ctx, appendBatch(local, remote, 1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "idle retirement", func() bool { sender.mu.Lock(); defer sender.mu.Unlock(); return len(sender.streams) == 0 })
	if err := sender.SendRaft(ctx, appendBatch(local, remote, 2)); err != nil {
		t.Fatal("route could not reopen its stream", err)
	}
	waitFor(t, "both batches", func() bool { mu.Lock(); defer mu.Unlock(); return received == 2 })
	if err := sender.Close(ctx); err != nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Close did not join stream work", err)
	}
}
