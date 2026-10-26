//go:build functional

package loadsnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type snapshotSendTransportFunc func(context.Context, snapshot.Send) (snapshot.Ack, error)

func (f snapshotSendTransportFunc) SendSnapshot(ctx context.Context, s snapshot.Send) (snapshot.Ack, error) {
	return f(ctx, s)
}

type sendReport struct {
	key     snapshot.Key
	ack     snapshot.Ack
	err     error
	attempt raftengine.SnapshotAttempt
}
type sendFixture struct {
	c                                  *Coordinator
	owner                              *receiveOwner
	to                                 load.Peer
	offer                              raftengine.SnapshotOffer
	epochCancel                        context.CancelFunc
	reports                            chan sendReport
	progress                           chan metrics.SnapshotCatchUp
	resolves, validates, registrations atomic.Int32
	delivery                           atomic.Uint64
}

func newSendFixture(t *testing.T, opts snapshot.Options) *sendFixture {
	t.Helper()
	if opts.MaxSnapshotBytes == 0 {
		opts.MaxSnapshotBytes = 4 << 20
	}
	if opts.ChunkBytes == 0 {
		opts.ChunkBytes = 4
	}
	source := load.Peer{Node: topology.Identity{NodeName: "source", ClusterID: "cluster", InstanceID: "source-process", MemberID: 1}, Replica: load.ReplicaRef{GroupID: "group", ReplicaID: 1}}
	to := source
	to.Node.NodeName = "destination"
	to.Node.InstanceID = "destination-process"
	to.Node.MemberID = 2
	to.Replica.ReplicaID = 2
	ownerCtx, cancel := context.WithCancel(t.Context())
	epoch, epochCancel := context.WithCancel(ownerCtx)
	f := &sendFixture{owner: &receiveOwner{key: OwnerKey{Peer: source, RunID: "source-run"}, ctx: ownerCtx, cancel: cancel}, to: to, epochCancel: epochCancel, reports: make(chan sendReport, 32), progress: make(chan metrics.SnapshotCatchUp, 32)}
	f.offer = raftengine.SnapshotOffer{Message: raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: &raftpb.Snapshot{Data: []byte("snapshot bytes"), Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}},
		Attempt: raftengine.SnapshotAttempt{RunID: f.owner.key.RunID, Term: 3, ReplicaID: 2, Generation: 1}, PublicationGeneration: 1, Epoch: epoch}
	ports := Ports{
		ResolvePeer: func(_ context.Context, _ Owner, id uint64) (load.Peer, error) {
			f.resolves.Add(1)
			peer := f.to
			peer.Replica.ReplicaID = id
			return peer, nil
		},
		ValidateOutgoing: func(ctx context.Context, owner Owner, to load.Peer) error {
			f.validates.Add(1)
			if err := ctx.Err(); err != nil {
				return err
			}
			if owner.Key() != f.owner.key || to.Replica.GroupID != "group" {
				return fault.ErrIdentityMismatch
			}
			return nil
		},
		RegisterDelivery: func(Owner, raftengine.SnapshotAttempt, load.Peer, time.Time) (uint64, error) {
			f.registrations.Add(1)
			return f.delivery.Add(1), nil
		},
		ReportDelivery: func(_ Owner, key snapshot.Key, ack snapshot.Ack, err error) {
			f.reports <- sendReport{key: key, ack: ack, err: err}
		},
		ReportSnapshotFailure: func(_ Owner, attempt raftengine.SnapshotAttempt, err error) {
			f.reports <- sendReport{attempt: attempt, err: err}
		},
		NoteSnapshotProgress: func(_ Owner, _ raftengine.SnapshotAttempt, p metrics.SnapshotCatchUp) { f.progress <- p },
		Transport:            snapshotSendTransportFunc(func(_ context.Context, s snapshot.Send) (snapshot.Ack, error) { return validSendAck(s), nil }),
	}
	f.c = NewCoordinator(opts, ports)
	f.c.Start(t.Context())
	t.Cleanup(func() {
		cancel()
		epochCancel()
		if err := f.c.Stop(receiveContext(t)); err != nil {
			t.Error(err)
		}
	})
	return f
}
func validSendAck(s snapshot.Send) snapshot.Ack {
	return snapshot.Ack{Session: snapshot.Session{Key: s.Manifest.Key, Attachment: 1}, NextOffset: s.Manifest.TotalBytes,
		ChunkBytes: s.Manifest.ChunkBytes, State: snapshot.DeliveryAccepted, ReceiverRunID: "receiver-run", RemainingTotal: time.Second, ProgressTimeout: time.Second}
}
func prepareSend(t *testing.T, f *sendFixture) *snapshotSendJob {
	t.Helper()
	job, err := f.c.PrepareSend(f.owner, f.offer)
	if err != nil {
		t.Fatal(err)
	}
	return job.(*snapshotSendJob)
}
func awaitSendReport(t *testing.T, f *sendFixture) sendReport {
	t.Helper()
	select {
	case report := <-f.reports:
		return report
	case <-receiveContext(t).Done():
		t.Fatal("send report missing")
		return sendReport{}
	}
}
func sendCounts(c *Coordinator) (int, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outgoingCount, c.sendBytes
}
func TestSnapshotSend_PreparationPinsWithoutCallbacksAndDiscardReportsOnce(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	data := f.offer.Message.Snapshot.Data
	job := prepareSend(t, f)
	if f.resolves.Load() != 0 || f.validates.Load() != 0 || f.registrations.Load() != 0 || len(f.c.digests) != 0 {
		t.Fatal("preparation performed worker work")
	}
	if &job.native.Snapshot.Data[0] != &data[0] {
		t.Fatal("source payload cloned")
	}
	job.Discard(fault.ErrStopped)
	job.Discard(fault.ErrStopped)
	job.Run(t.Context())
	report := awaitSendReport(t, f)
	if report.attempt != f.offer.Attempt || report.err == nil || report.key.DeliveryID != 0 {
		t.Fatal(report)
	}
	select {
	case second := <-f.reports:
		t.Fatal("duplicate report", second)
	default:
	}
	if n, b := sendCounts(f.c); n != 0 || b != 0 || job.native.Snapshot != nil || job.source.handle != nil {
		t.Fatal("discard retained source", n, b)
	}
}
func TestSnapshotSend_BorrowsOldArtifactAndClosesProgressCallback(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	original := f.offer.Message.Snapshot.Data
	var retained func(snapshot.Progress)
	f.c.ports.Transport = snapshotSendTransportFunc(func(_ context.Context, s snapshot.Send) (snapshot.Ack, error) {
		if &s.Data[0] != &original[0] || s.Manifest.SHA256 != sha256.Sum256(original) {
			t.Fatal("payload was copied or changed")
		}
		var header raftpb.Message
		if err := header.Unmarshal(s.Manifest.NativeHeader); err != nil || len(header.Snapshot.Data) != 0 {
			t.Fatal("full data in header", err)
		}
		retained = s.ReportProgress
		s.ReportProgress(snapshot.Progress{ReceiverRunID: "receiver-run", VerifiedBytes: 4, State: snapshot.DeliveryReceiving, RemainingTotal: time.Second, ProgressTimeout: time.Second})
		s.ReportProgress(snapshot.Progress{ReceiverRunID: "receiver-run", VerifiedBytes: 4, State: snapshot.DeliveryReceiving, RemainingTotal: time.Second, ProgressTimeout: time.Second})
		return validSendAck(s), nil
	})
	job := prepareSend(t, f)
	replacement := *f.offer.Message.Snapshot
	replacement.Data = []byte("replacement artifact")
	f.offer.Message.Snapshot = &replacement
	job.Run(t.Context())
	report := awaitSendReport(t, f)
	if report.err != nil || report.ack.State != snapshot.DeliveryAccepted {
		t.Fatal(report)
	}
	if len(f.progress) != 2 {
		t.Fatal("duplicate or missing progress", len(f.progress))
	}
	before := len(f.progress)
	retained(snapshot.Progress{ReceiverRunID: "forged", VerifiedBytes: 999})
	if len(f.progress) != before || job.native.Snapshot != nil || job.source.handle != nil {
		t.Fatal("late callback retained or changed source")
	}
}
func TestSnapshotSend_CapacityRefusalReusesDigestAndReleasesPayload(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	f.c.ports.Transport = snapshotSendTransportFunc(func(_ context.Context, s snapshot.Send) (snapshot.Ack, error) {
		return snapshot.Ack{}, transferError(snapshot.ReasonCapacity)
	})
	for range 2 {
		job := prepareSend(t, f)
		job.Run(t.Context())
		report := awaitSendReport(t, f)
		requireReceiveReason(t, report.err, snapshot.ReasonCapacity)
		if n, b := sendCounts(f.c); n != 0 || b != 0 || job.native.Snapshot != nil {
			t.Fatal(n, b)
		}
	}
	f.c.mu.Lock()
	if len(f.c.digests) != 1 || len(f.c.hashWork) != 0 {
		t.Fatal("unchanged artifact not cached")
	}
	f.c.mu.Unlock()
}
func TestSnapshotSend_ProgressTimeoutIncludesMissingAdmissionAndDuplicates(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"no admission", "duplicates"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newSendFixture(t, snapshot.Options{TotalTimeout: time.Second, ProgressTimeout: 30 * time.Millisecond})
			f.c.ports.Transport = snapshotSendTransportFunc(func(ctx context.Context, s snapshot.Send) (snapshot.Ack, error) {
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return snapshot.Ack{}, ctx.Err()
					case <-ticker.C:
						if kind == "duplicates" {
							s.ReportProgress(snapshot.Progress{ReceiverRunID: "receiver-run", State: snapshot.DeliveryReceiving, RemainingTotal: time.Second, ProgressTimeout: 30 * time.Millisecond})
						}
					}
				}
			})
			job := prepareSend(t, f)
			job.Run(t.Context())
			report := awaitSendReport(t, f)
			requireReceiveReason(t, report.err, snapshot.ReasonProgressTimeout)
			if n, _ := sendCounts(f.c); n != 0 {
				t.Fatal("expired send retained permit")
			}
		})
	}
}
func TestSnapshotSend_AllBytesAcknowledgedUsesTotalDeadline(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{TotalTimeout: 80 * time.Millisecond, ProgressTimeout: 20 * time.Millisecond})
	f.c.ports.Transport = snapshotSendTransportFunc(func(ctx context.Context, s snapshot.Send) (snapshot.Ack, error) {
		s.ReportProgress(snapshot.Progress{ReceiverRunID: "receiver-run", VerifiedBytes: s.Manifest.TotalBytes, State: snapshot.DeliveryReceiving, RemainingTotal: 80 * time.Millisecond, ProgressTimeout: 20 * time.Millisecond})
		<-ctx.Done()
		return snapshot.Ack{}, ctx.Err()
	})
	job := prepareSend(t, f)
	job.Run(t.Context())
	report := awaitSendReport(t, f)
	requireReceiveReason(t, report.err, snapshot.ReasonTotalTimeout)
}
func TestSnapshotSend_CanceledPreparedJobsReleaseBeforeDispatch(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"epoch", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newSendFixture(t, snapshot.Options{TotalTimeout: 40 * time.Millisecond, ProgressTimeout: 20 * time.Millisecond})
			job := prepareSend(t, f)
			if kind == "epoch" {
				f.epochCancel()
			}
			report := awaitSendReport(t, f)
			if report.err == nil || report.attempt != f.offer.Attempt {
				t.Fatal(report)
			}
			if err := f.c.joinSnapshotOwnership(t.Context(), f.owner.Key()); err != nil {
				t.Fatal(err)
			}
			if n, _ := sendCounts(f.c); n != 0 || job.native.Snapshot != nil {
				t.Fatal("undispatched source pin retained")
			}
			job.Run(t.Context())
			select {
			case duplicate := <-f.reports:
				t.Fatal(duplicate)
			default:
			}
		})
	}
}
func TestSnapshotSend_TransportAndReportReturnAreJoinedBeforeRelease(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.c.ports.Transport = snapshotSendTransportFunc(func(ctx context.Context, _ snapshot.Send) (snapshot.Ack, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return snapshot.Ack{}, ctx.Err()
	})
	job := prepareSend(t, f)
	done := make(chan struct{})
	go func() { job.Run(t.Context()); close(done) }()
	receiveBarrier(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	f.owner.cancel()
	if err := f.c.StopOwner(ctx, f.owner.key); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if n, _ := sendCounts(f.c); n != 1 {
		t.Fatal("live send released source")
	}
	once.Do(func() { close(release) })
	receiveBarrier(t, done)
	if err := f.c.StopOwner(receiveContext(t), f.owner.key); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotSend_NotNeededAndMalformedAcks(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"not needed", "wrong key", "wrong run", "incomplete"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newSendFixture(t, snapshot.Options{})
			f.c.ports.Transport = snapshotSendTransportFunc(func(_ context.Context, s snapshot.Send) (snapshot.Ack, error) {
				ack := validSendAck(s)
				switch kind {
				case "not needed":
					ack.State = snapshot.DeliveryNotNeeded
					ack.NextOffset = 0
					ack.Session.Attachment = 0
					ack.ChunkBytes = 0
				case "wrong key":
					ack.Session.Key.DeliveryID++
				case "wrong run":
					s.ReportProgress(snapshot.Progress{ReceiverRunID: "first-run", State: snapshot.DeliveryReceiving, RemainingTotal: time.Second, ProgressTimeout: time.Second})
					ack.ReceiverRunID = "another-run"
				case "incomplete":
					ack.NextOffset--
				}
				return ack, nil
			})
			job := prepareSend(t, f)
			job.Run(t.Context())
			report := awaitSendReport(t, f)
			if kind == "not needed" {
				if report.err != nil || report.ack.State != snapshot.DeliveryNotNeeded {
					t.Fatal(report)
				}
			} else if report.err == nil {
				t.Fatal("malformed Ack accepted")
			}
		})
	}
}

type gatedHashContext struct {
	context.Context
	calls            atomic.Int32
	entered, release chan struct{}
}

func (c *gatedHashContext) Err() error {
	if c.calls.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}
func TestSnapshotSend_DigestSingleflightCancellationAndPublication(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{MaxOutgoing: 2})
	data := bytes.Repeat([]byte("artifact"), 128<<10)
	f.offer.Message.Snapshot.Data = data
	first := prepareSend(t, f)
	defer first.Discard(fault.ErrStopped)
	secondOffer := f.offer
	secondOffer.Message.To = 3
	secondOffer.Attempt.ReplicaID = 3
	secondOffer.Attempt.Generation++
	secondRaw, err := f.c.PrepareSend(f.owner, secondOffer)
	if err != nil {
		t.Fatal(err)
	}
	defer secondRaw.Discard(fault.ErrStopped)
	id := SourceSnapshotID{RunID: f.owner.key.RunID, Generation: 1, Index: 9, Term: 2, Bytes: uint64(len(data))}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gate := &gatedHashContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate.release) }) })
	returned := make(chan error, 2)
	go func() { _, err := f.c.snapshotDigest(gate, id, data); returned <- err }()
	receiveBarrier(t, gate.entered)
	go func() { _, err := f.c.snapshotDigest(t.Context(), id, data); returned <- err }()
	wait := receiveContext(t)
	for {
		f.c.mu.Lock()
		work := f.c.hashWork[id]
		attached := work != nil && work.waiters == 1
		f.c.mu.Unlock()
		if attached {
			break
		}
		select {
		case <-wait.Done():
			t.Fatal("singleflight waiter not attached")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	once.Do(func() { close(gate.release) })
	for range 2 {
		if err := <-returned; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	f.c.mu.Lock()
	if len(f.c.digests) != 0 || len(f.c.hashWork) != 0 {
		t.Fatal("incomplete hash cached")
	}
	f.c.mu.Unlock()
	sum, err := f.c.snapshotDigest(t.Context(), id, data)
	if err != nil || sum != sha256.Sum256(data) {
		t.Fatal(sum, err)
	}
	id.Generation++
	changed := bytes.Clone(data)
	changed[0] ^= 1
	sum, err = f.c.snapshotDigest(t.Context(), id, changed)
	if err != nil || sum != sha256.Sum256(changed) {
		t.Fatal("publication reused old digest", err)
	}
}

func TestSnapshotSend_ReportCallbackRemainsOwnedUntilReturn(t *testing.T) {
	t.Parallel()
	f := newSendFixture(t, snapshot.Options{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	original := f.c.ports.ReportDelivery
	f.c.ports.ReportDelivery = func(owner Owner, key snapshot.Key, ack snapshot.Ack, err error) {
		close(entered)
		<-release
		original(owner, key, ack, err)
	}
	job := prepareSend(t, f)
	done := make(chan struct{})
	go func() { job.Run(t.Context()); close(done) }()
	receiveBarrier(t, entered)
	if n, _ := sendCounts(f.c); n != 1 {
		t.Fatal("report callback released source early")
	}
	once.Do(func() { close(release) })
	receiveBarrier(t, done)
	if n, _ := sendCounts(f.c); n != 0 {
		t.Fatal("report return did not release")
	}
}
