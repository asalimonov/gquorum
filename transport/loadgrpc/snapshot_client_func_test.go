//go:build functional

package loadgrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type snapshotClientStats struct{ data, finish atomic.Int64 }

func (s *snapshotClientStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (s *snapshotClientStats) HandleRPC(_ context.Context, event stats.RPCStats) {
	if sent, ok := event.(*stats.OutPayload); ok {
		if frame, ok := sent.Payload.(*loadpb.SnapshotFrame); ok {
			if frame.GetData() != nil {
				s.data.Add(1)
			}
			if frame.GetFinish() != nil {
				s.finish.Add(1)
			}
		}
	}
}
func (*snapshotClientStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (*snapshotClientStats) HandleConn(context.Context, stats.ConnStats) {}
func newSnapshotRPCSender(t *testing.T, f *snapshotRPCFixture, policy snapshot.Options) (*Transport, *snapshotClientStats) {
	t.Helper()
	policy, err := policy.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	observed := new(snapshotClientStats)
	source, err := New(Options{Resolve: func(context.Context, topology.Identity) (string, error) { return f.conn.Target(), nil },
		Authenticate: func(context.Context, topology.Identity) error { return nil },
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(observed),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return f.listener.DialContext(ctx) })}})
	if err != nil {
		t.Fatal(err)
	}
	node := &snapshotRPCIngress{ingressDouble: &ingressDouble{id: testSnapshotIdentity("sender", 1)}, snapshots: &snapshotRPCService{options: policy}}
	if err = source.Bind(node); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := source.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return source, observed
}
func snapshotRPCSend(t *testing.T, f *snapshotRPCFixture, data []byte, chunk uint32) snapshot.Send {
	t.Helper()
	request, err := decodeSnapshotOpen(f.open, defaultMaxMessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	m := request.Manifest
	m.TotalBytes = uint64(len(data))
	m.SHA256 = sha256.Sum256(data)
	m.ChunkBytes = chunk
	return snapshot.Send{Manifest: m, Data: data}
}
func requireSnapshotClientReason(t *testing.T, err error, reason string) {
	t.Helper()
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) || detail.Reason != reason {
		t.Fatalf("wanted %s, got %v", reason, err)
	}
}
func TestSnapshotRPC_ClientTransfersWithNegotiatedBoundsAndJoinedCallbacks(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{ChunkBytes: 2, ProgressTimeout: time.Second})
	source, observed := newSnapshotRPCSender(t, f, snapshot.Options{ChunkBytes: 4})
	send := snapshotRPCSend(t, f, []byte("bytes"), 4)
	var progress []snapshot.Progress
	send.ReportProgress = func(p snapshot.Progress) { progress = append(progress, p) }
	ack, err := source.SendSnapshot(t.Context(), send)
	if err != nil || ack.State != snapshot.DeliveryAccepted || ack.NextOffset != 5 || ack.ChunkBytes != 2 {
		t.Fatal(ack, err)
	}
	if observed.data.Load() != 3 || observed.finish.Load() != 1 {
		t.Fatal("wrong data/finish counts")
	}
	if len(progress) != 5 || progress[0].VerifiedBytes != 0 || progress[3].VerifiedBytes != 5 || progress[4].State != snapshot.DeliveryAccepted {
		t.Fatal(progress)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.snapshotSending != 0 || len(source.snapshotClients) != 1 || len(source.clients) != 0 {
		t.Fatal("bulk ownership/pool mismatch")
	}
}

type snapshotDropAck struct {
	grpc.ServerStream
	stage   string
	dropped *atomic.Bool
}

func (s *snapshotDropAck) SendMsg(message any) error {
	ack, ok := message.(*loadpb.SnapshotAck)
	if ok {
		drop := s.stage == "open" && ack.State == string(snapshot.DeliveryReceiving) && ack.NextOffset == 0 ||
			s.stage == "chunk" && ack.State == string(snapshot.DeliveryReceiving) && ack.NextOffset > 0 ||
			s.stage == "final" && ack.State == string(snapshot.DeliveryAccepted)
		if drop && s.dropped.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "lost acknowledgement")
		}
	}
	return s.ServerStream.SendMsg(message)
}
func TestSnapshotRPC_ClientResumesLostOpenChunkAndFinalAck(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"open", "chunk", "final"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			dropped := new(atomic.Bool)
			f := newSnapshotRPCFixtureWith(t, snapshot.Options{}, func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				return handler(server, &snapshotDropAck{stream, stage, dropped})
			})
			source, _ := newSnapshotRPCSender(t, f, snapshot.Options{})
			send := snapshotRPCSend(t, f, []byte("bytes"), 4)
			ack, err := source.SendSnapshot(t.Context(), send)
			if err != nil || ack.State != snapshot.DeliveryAccepted || !dropped.Load() {
				t.Fatal(ack, err)
			}
			f.service.mu.Lock()
			last := f.service.lastRequest
			f.service.mu.Unlock()
			if !last.Resume || last.Manifest.ChunkBytes != 4 {
				t.Fatal(last)
			}
			if f.service.begins.Load() != 2 {
				t.Fatal("wrong retry count", f.service.begins.Load())
			}
			waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 2 && snapshotHandlersIdle(f.transport) })
		})
	}
}
func TestSnapshotRPC_ClientDoesNotRetryPermanentOrCapacityRefusal(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{snapshot.ReasonCapacity, snapshot.ReasonRestartRequired, snapshot.ReasonUnsupported} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotRPCFixture(t, snapshot.Options{})
			f.service.begin = func(context.Context, snapshot.BeginRequest) error { return snapshotWireError(reason, nil) }
			source, _ := newSnapshotRPCSender(t, f, snapshot.Options{})
			_, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
			requireSnapshotClientReason(t, err, reason)
			if f.service.begins.Load() != 1 {
				t.Fatal("permanent refusal retried")
			}
		})
	}
}
func TestSnapshotRPC_ClientNoDataBeforeAdmissionAndWindowIsBounded(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	source, observed := newSnapshotRPCSender(t, f, snapshot.Options{ChunkBytes: 4, WindowChunks: 3})
	entered, release := make(chan struct{}), make(chan struct{})
	var released, enteredOnce sync.Once
	defer released.Do(func() { close(release) })
	f.service.write = func(ctx context.Context, _ snapshot.Chunk) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Use a separate close owner; the first write marker above is single-use.
	done := make(chan error, 1)
	send := snapshotRPCSend(t, f, bytes.Repeat([]byte("a"), 64), 4)
	go func() {
		_, err := source.SendSnapshot(t.Context(), send)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no first chunk")
	}
	waitSnapshotRPC(t, func() bool { return observed.data.Load() == 3 })
	time.Sleep(20 * time.Millisecond)
	if observed.data.Load() != 3 || observed.finish.Load() != 0 {
		t.Fatal("pipeline exceeded window or finished early")
	}
	released.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not finish")
	}
}
func TestSnapshotRPC_ClientHonorsNarrowedProgressTimeout(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{ProgressTimeout: 30 * time.Millisecond, TotalTimeout: time.Second})
	f.service.write = func(ctx context.Context, _ snapshot.Chunk) error { <-ctx.Done(); return ctx.Err() }
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{ProgressTimeout: time.Second, TotalTimeout: time.Second})
	_, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
	requireSnapshotClientReason(t, err, snapshot.ReasonProgressTimeout)
}
func TestSnapshotRPC_ClientRejectsChunkOutsideOwnFrameLimit(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{ChunkBytes: 4096})
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{ChunkBytes: 4096})
	source.options.MaxMessageBytes = 1024
	_, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4096))
	requireSnapshotClientReason(t, err, snapshot.ReasonFrameLimit)
	if f.service.writes.Load() != 0 {
		t.Fatal("data before valid negotiation")
	}
}
func TestSnapshotRPC_ClientBulkFlowControlDoesNotBlockOrdinaryRPC(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	source, observed := newSnapshotRPCSender(t, f, snapshot.Options{})
	entered := make(chan struct{})
	var once sync.Once
	f.service.write = func(ctx context.Context, _ snapshot.Chunk) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	send := snapshotRPCSend(t, f, bytes.Repeat([]byte("a"), 8<<20), 1<<20)
	done := make(chan error, 1)
	go func() { _, err := source.SendSnapshot(t.Context(), send); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("bulk did not start")
	}
	time.Sleep(30 * time.Millisecond)
	if observed.data.Load() >= 8 {
		t.Fatal("bulk flow control did not block")
	}
	result, err := source.ReadReplica(t.Context(), load.ReplicaQuery{From: send.Manifest.Key.From.Node, To: send.Manifest.Key.To})
	if err != nil || !result.Found {
		t.Fatal(result, err)
	}
	source.mu.Lock()
	ordinary := source.clients[f.conn.Target()]
	bulk := source.snapshotClients[f.conn.Target()]
	distinct := ordinary != nil && bulk != nil && ordinary.conn != bulk.conn
	source.mu.Unlock()
	if !distinct {
		t.Fatal("bulk reused ordinary connection")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := source.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled transfer succeeded")
		}
	case <-ctx.Done():
		t.Fatal("bulk send did not join")
	}
	waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
}

type snapshotAdmissionGate struct {
	grpc.ServerStream
	entered    chan struct{}
	release    <-chan struct{}
	duplicates bool
}

func (s *snapshotAdmissionGate) SendMsg(message any) error {
	ack, ok := message.(*loadpb.SnapshotAck)
	if !ok || ack.NextOffset != 0 {
		return s.ServerStream.SendMsg(message)
	}
	close(s.entered)
	if !s.duplicates {
		select {
		case <-s.release:
			return s.ServerStream.SendMsg(message)
		case <-s.Context().Done():
			return s.Context().Err()
		}
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := s.ServerStream.SendMsg(message); err != nil {
			return err
		}
		select {
		case <-tick.C:
		case <-s.Context().Done():
			return s.Context().Err()
		}
	}
}
func TestSnapshotRPC_ClientWaitsForAdmissionAndIgnoresDuplicateProgress(t *testing.T) {
	t.Parallel()
	for _, duplicates := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "duplicates"}[duplicates], func(t *testing.T) {
			t.Parallel()
			entered := make(chan struct{})
			f := newSnapshotRPCFixtureWith(t, snapshot.Options{}, func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				return handler(server, &snapshotAdmissionGate{ServerStream: stream, entered: entered, duplicates: duplicates})
			})
			source, observed := newSnapshotRPCSender(t, f, snapshot.Options{ProgressTimeout: 50 * time.Millisecond, TotalTimeout: time.Second})
			send := snapshotRPCSend(t, f, []byte("bytes"), 4)
			done := make(chan error, 1)
			go func() { _, err := source.SendSnapshot(t.Context(), send); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no admission attempt")
			}
			select {
			case err := <-done:
				requireSnapshotClientReason(t, err, snapshot.ReasonProgressTimeout)
			case <-time.After(time.Second):
				t.Fatal("network progress deadline renewed")
			}
			if !duplicates && observed.data.Load() != 0 {
				t.Fatal("data sent before admission")
			}
			waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 && snapshotHandlersIdle(f.transport) })
		})
	}
}
func TestSnapshotRPC_ClientOriginalTotalDeadlineSurvivesReconnects(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixtureWith(t, snapshot.Options{}, func(_ any, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		return status.Error(codes.Unavailable, "reconnect")
	})
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{ProgressTimeout: 500 * time.Millisecond, TotalTimeout: 500 * time.Millisecond})
	started := time.Now()
	_, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
	requireSnapshotClientReason(t, err, snapshot.ReasonTotalTimeout)
	if time.Since(started) > time.Second || f.stats.ends.Load() < 2 {
		t.Fatal("total lifetime was not preserved across attempts")
	}
}
func TestSnapshotRPC_ClientAllBytesVerifiedUsesTotalForFinalization(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	f.service.finish = func(ctx context.Context, _ snapshot.Session) error {
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{ProgressTimeout: 80 * time.Millisecond, TotalTimeout: time.Second})
	ack, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
	if err != nil || ack.State != snapshot.DeliveryAccepted {
		t.Fatal(ack, err)
	}
}

func TestSnapshotRPC_ClientNotNeededHasNoPayloadOrFinish(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	f.service.notNeeded = true
	source, observed := newSnapshotRPCSender(t, f, snapshot.Options{})
	ack, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
	if err != nil || ack.State != snapshot.DeliveryNotNeeded || ack.NextOffset != 0 {
		t.Fatal(ack, err)
	}
	if observed.data.Load() != 0 || observed.finish.Load() != 0 {
		t.Fatal("payload sent for native skip")
	}
}
func TestSnapshotRPC_ClientUnsupportedPeerDoesNotFallback(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixtureWith(t, snapshot.Options{}, func(_ any, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		return status.Error(codes.Unimplemented, "unknown snapshot method")
	})
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{})
	_, err := source.SendSnapshot(t.Context(), snapshotRPCSend(t, f, []byte("bytes"), 4))
	requireSnapshotClientReason(t, err, snapshot.ReasonUnsupported)
	waitSnapshotRPC(t, func() bool { return f.stats.ends.Load() == 1 })
	if f.service.begins.Load() != 0 {
		t.Fatal("unsupported peer reached snapshot ingress")
	}
}
func TestSnapshotRPC_ClientOutgoingQuotaRetainedThroughAdmission(t *testing.T) {
	t.Parallel()
	f := newSnapshotRPCFixture(t, snapshot.Options{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.service.begin = func(ctx context.Context, _ snapshot.BeginRequest) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	source, _ := newSnapshotRPCSender(t, f, snapshot.Options{})
	send := snapshotRPCSend(t, f, []byte("bytes"), 4)
	done := make(chan error, 1)
	go func() { _, err := source.SendSnapshot(t.Context(), send); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no admission")
	}
	_, err := source.SendSnapshot(t.Context(), send)
	requireSnapshotClientReason(t, err, snapshot.ReasonCapacity)
	source.mu.Lock()
	count, connections := source.snapshotSending, len(source.snapshotClients)
	source.mu.Unlock()
	if count != 1 || connections != 1 {
		t.Fatal(count, connections)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted send did not finish")
	}
}
