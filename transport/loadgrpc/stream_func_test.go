//go:build functional

package loadgrpc

import (
	"context"
	"errors"
	"sync"
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

// olderServer is a recipient from a release without StreamRaft.
type olderServer struct{ *serverAdapter }

func (olderServer) StreamRaft(loadpb.Load_StreamRaftServer) error {
	return status.Error(codes.Unimplemented, "method StreamRaft not implemented")
}

func TestOlderRecipientReceivesUnaryCalls(t *testing.T) {
	t.Parallel()
	sender, _, local, remote := newProtocolPairWith(t, 4096, func(s *grpc.Server, receiver *Transport) {
		loadpb.RegisterLoadServer(s, olderServer{&serverAdapter{transport: receiver}})
	})
	var mu sync.Mutex
	var got []uint64
	remote.receive = func(_ context.Context, b load.RaftBatch) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, b.DeliveryID)
		return nil
	}
	reports := make(chan load.DeliveryReport, 4)
	local.report = func(_ context.Context, r load.DeliveryReport) error { reports <- r; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The first batch is written to a stream the recipient refuses; it is lost
	// and reported unreachable, so Raft resends it.
	if err := sender.SendRaft(ctx, appendBatch(local, remote, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-reports:
		if r.DeliveryID != 1 || r.Kind != load.DeliveryUnreachable {
			t.Fatal(r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lost streamed batch was not reported")
	}
	waitFor(t, "unary fallback", func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		return len(sender.unaryOnly) == 1
	})
	if err := sender.SendRaft(ctx, appendBatch(local, remote, 2)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != 2 {
		t.Fatal("unary fallback did not deliver", got)
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
